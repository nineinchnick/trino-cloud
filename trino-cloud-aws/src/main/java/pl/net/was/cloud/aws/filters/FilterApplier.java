/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package pl.net.was.cloud.aws.filters;

import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.TimeZoneKey;
import pl.net.was.cloud.aws.AwsColumnHandle;
import pl.net.was.cloud.aws.AwsTableHandle;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.DateTimeEncoding.unpackZoneKey;
import static io.trino.spi.type.Timestamps.MILLISECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MILLISECOND;
import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE_TIME;
import static pl.net.was.cloud.aws.filters.FilterType.EQUAL;

public interface FilterApplier
{
    Logger log = Logger.get(FilterApplier.class);

    Map<String, FilterType> getSupportedFilters();

    default Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(
            AwsTableHandle table,
            Map<String, ColumnHandle> columns,
            Map<String, FilterType> supportedColumnFilters,
            Constraint constraint)
    {
        TupleDomain<ColumnHandle> summary = constraint.getSummary();
        // the only reason not to use isNone is so the linter doesn't complain about not checking an Optional
        if (summary.isAll() || summary.getDomains().isEmpty()) {
            return Optional.empty();
        }

        TupleDomain<ColumnHandle> currentConstraint = table.constraint();

        boolean found = false;
        for (Map.Entry<String, FilterType> entry : supportedColumnFilters.entrySet()) {
            String columnName = entry.getKey();
            FilterType supportedFilter = entry.getValue();
            ColumnHandle column = columns.get(columnName);

            TupleDomain<ColumnHandle> newConstraint = normalizeConstraint((AwsColumnHandle) column, supportedFilter, summary);
            if (newConstraint == null || newConstraint.getDomains().isEmpty()) {
                continue;
            }
            if (!validateConstraint((AwsColumnHandle) column, supportedFilter, currentConstraint, newConstraint)) {
                continue;
            }
            // merge with other pushed down constraints
            currentConstraint = currentConstraint.intersect(newConstraint);
            found = true;
            // remove from remaining constraints
            summary = summary.filter(
                    (columnHandle, _) -> !columnHandle.equals(column));
        }
        if (!found) {
            return Optional.empty();
        }

        return Optional.of(new ConstraintApplicationResult<>(
                table.withConstraint(currentConstraint),
                summary,
                constraint.getExpression(),
                true));
    }

    private TupleDomain<ColumnHandle> normalizeConstraint(AwsColumnHandle column, FilterType supportedFilter, TupleDomain<ColumnHandle> constraint)
    {
        //noinspection OptionalGetWithoutIsPresent
        Domain domain = constraint.getDomains().get().get(column);
        if (domain == null) {
            log.debug("Missing filter on %s", column.name());
            return null;
        }
        TupleDomain<ColumnHandle> newConstraint = constraint.filter(
                (columnHandle, _) -> columnHandle.equals(column));
        if (!domain.getType().isOrderable()) {
            return newConstraint;
        }
        switch (supportedFilter) {
            case GREATER_THAN_EQUAL -> {
                // normalize the constraint into a low-bound range
                Range span = domain.getValues().getRanges().getSpan();
                if (span.isLowUnbounded()) {
                    log.debug("Not pushing down filter on %s because it does not have a lower bound: %s", column.name(), domain);
                    return null;
                }
                newConstraint = TupleDomain.withColumnDomains(Map.of(
                        column, Domain.create(
                                ValueSet.ofRanges(
                                        Range.greaterThanOrEqual(
                                                domain.getType(),
                                                span.getLowBoundedValue())),
                                false)));
            }
            case EQUAL -> {
                if (!domain.getValues().isDiscreteSet() && !domain.getValues().getRanges().getOrderedRanges().stream().allMatch(Range::isSingleValue)) {
                    log.debug("Not pushing down filter on %s because it's not a discrete set: %s", column.name(), domain);
                    return null;
                }
                return newConstraint;
            }
        }
        return newConstraint;
    }

    private boolean validateConstraint(AwsColumnHandle column, FilterType supportedFilter, TupleDomain<ColumnHandle> currentConstraint, TupleDomain<ColumnHandle> newConstraint)
    {
        if (currentConstraint.getDomains().isEmpty() || !currentConstraint.getDomains().get().containsKey(column)) {
            return true;
        }
        Domain currentDomain = currentConstraint.getDomains().get().get(column);
        Domain newDomain = newConstraint.getDomains().get().get(column);
        if (currentDomain.intersect(newDomain).equals(currentDomain)) {
            // it is important to avoid processing same constraint multiple times
            // so that planner doesn't get stuck in a loop
            return false;
        }
        if (supportedFilter == EQUAL) {
            // can push down only the first predicate against this column, the engine applies the remaining one
            log.debug("Not pushing down filter on %s because a different filter was already pushed down: %s", column.name(), newDomain);
            return false;
        }
        // GREATER_THAN_EQUAL has a single low-bound range, so intersecting keeps the higher bound
        return true;
    }

    default Object getFilter(AwsColumnHandle column, TupleDomain<ColumnHandle> constraint)
    {
        return getFilter(column, constraint, null);
    }

    default Object getFilter(AwsColumnHandle column, TupleDomain<ColumnHandle> constraint, Object defaultValue)
    {
        Domain domain = null;
        if (constraint.getDomains().isPresent()) {
            domain = constraint.getDomains().get().get(column);
        }
        if (domain == null) {
            return defaultValue;
        }
        return switch (column.type().getBaseName()) {
            case StandardTypes.TIMESTAMP, StandardTypes.TIMESTAMP_WITH_TIME_ZONE -> {
                long since = (long) domain.getValues().getRanges().getSpan().getLowBoundedValue();
                yield ISO_LOCAL_DATE_TIME.format(fromTrinoTimestamp(since)) + "Z";
            }
            case StandardTypes.VARCHAR -> ((Slice) domain.getSingleValue()).toStringUtf8();
            case StandardTypes.BIGINT, StandardTypes.INTEGER -> domain.getSingleValue();
            default -> throw new IllegalStateException("Unexpected constraint for " + column.name() + "(" + column.type().getBaseName() + ")");
        };
    }

    default ZonedDateTime fromTrinoTimestamp(long timestampWithTimeZone)
    {
        TimeZoneKey zoneKey = unpackZoneKey(timestampWithTimeZone);
        long millis = unpackMillisUtc(timestampWithTimeZone);

        long epochSecond = floorDiv(millis, MILLISECONDS_PER_SECOND);
        int nanoFraction = floorMod(millis, MILLISECONDS_PER_SECOND) * NANOSECONDS_PER_MILLISECOND;
        Instant instant = Instant.ofEpochSecond(epochSecond, nanoFraction);
        return ZonedDateTime.ofInstant(instant, zoneKey.getZoneId()).withZoneSameInstant(ZoneId.of("UTC"));
    }
}
