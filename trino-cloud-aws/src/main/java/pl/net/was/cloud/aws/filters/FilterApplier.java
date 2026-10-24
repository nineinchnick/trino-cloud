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

import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.trino.spi.TrinoException;
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
import io.trino.spi.type.VarcharType;
import pl.net.was.cloud.aws.AwsColumnHandle;
import pl.net.was.cloud.aws.AwsTableHandle;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.DateTimeEncoding.unpackZoneKey;
import static io.trino.spi.type.Timestamps.MILLISECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MILLISECOND;
import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE_TIME;
import static pl.net.was.cloud.aws.AwsErrorCode.AWS_MISSING_PREDICATE;
import static pl.net.was.cloud.aws.filters.FilterType.EQUAL;

public interface FilterApplier
{
    Logger log = Logger.get(FilterApplier.class);
    // keeps EC2 requests well under the limit of filter values, while dividing large IN lists into parallel splits
    int ID_BATCH_SIZE = 100;

    Map<String, FilterType> getSupportedFilters();

    /**
     * Names of supported filters that are pushed down, but Trino still has to apply them,
     * because AWS can return rows that don't match the filter value.
     */
    default Set<String> getUnenforcedFilters()
    {
        return Set.of();
    }

    /**
     * Names of supported filters that must be pushed down, because AWS can't list rows without them.
     */
    default Set<String> getRequiredFilters()
    {
        return Set.of();
    }

    /**
     * Maximum number of values of a supported filter that a single AWS request accepts.
     * Scans filtered on more values are divided into splits, which can be read in parallel.
     */
    default Map<String, Integer> getSplitBatchSizes()
    {
        return Map.of();
    }

    /**
     * All values of varchar columns with a split batch size, for columns that choose where rows are read from, not which rows are returned.
     * Scans are divided into splits by these values even without a filter on the column, and filter values that are not known match no rows.
     * The values are only listed when splits are created, because listing them can require an AWS request.
     */
    default Map<String, Supplier<List<String>>> getKnownValues()
    {
        return Map.of();
    }

    /**
     * Values read by scans without a filter on a varchar column with a split batch size, for columns that choose where rows are read from.
     * Unlike known values, they don't restrict the filter values, because they might not include every value that can be read.
     * The values are only listed for scans without a filter on the column, because listing them can require an AWS request.
     */
    default Map<String, Supplier<List<String>>> getDefaultValues()
    {
        return Map.of();
    }

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
            if (getUnenforcedFilters().contains(columnName) || !isEnforcedBy(column, summary, newConstraint)) {
                continue;
            }
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
                false));
    }

    private static boolean isEnforcedBy(ColumnHandle column, TupleDomain<ColumnHandle> summary, TupleDomain<ColumnHandle> pushedConstraint)
    {
        //noinspection OptionalGetWithoutIsPresent
        return summary.getDomains().get().get(column).equals(pushedConstraint.getDomains().get().get(column));
    }

    private TupleDomain<ColumnHandle> normalizeConstraint(AwsColumnHandle column, FilterType supportedFilter, TupleDomain<ColumnHandle> constraint)
    {
        //noinspection OptionalGetWithoutIsPresent
        Domain domain = constraint.getDomains().get().get(column);
        if (domain == null) {
            log.debug("Missing filter on %s", column.name());
            return null;
        }
        if (domain.isNullAllowed()) {
            log.debug("Not pushing down filter on %s because it allows nulls: %s", column.name(), domain);
            return null;
        }
        return switch (supportedFilter) {
            case EQUAL -> {
                if (domain.getValues().isNone() || !domain.getValues().isDiscreteSet()) {
                    log.debug("Not pushing down filter on %s because it's not a discrete set: %s", column.name(), domain);
                    yield null;
                }
                yield TupleDomain.withColumnDomains(Map.of(column, domain));
            }
            case GREATER_THAN_EQUAL -> {
                if (!domain.getType().isOrderable() || domain.getValues().getRanges().getSpan().isLowUnbounded()) {
                    log.debug("Not pushing down filter on %s because it does not have a lower bound: %s", column.name(), domain);
                    yield null;
                }
                // normalize the constraint into a low-bound range
                yield TupleDomain.withColumnDomains(Map.of(
                        column, Domain.create(
                                ValueSet.ofRanges(
                                        Range.greaterThanOrEqual(
                                                domain.getType(),
                                                domain.getValues().getRanges().getSpan().getLowBoundedValue())),
                                false)));
            }
        };
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

    default void checkRequiredFilters(AwsTableHandle table)
    {
        Set<String> filteredColumns = table.constraint().getDomains()
                .map(domains -> domains.keySet().stream()
                        .map(column -> ((AwsColumnHandle) column).name())
                        .collect(toImmutableSet()))
                .orElse(ImmutableSet.of());
        for (String columnName : getRequiredFilters()) {
            if (!filteredColumns.contains(columnName)) {
                throw new TrinoException(AWS_MISSING_PREDICATE, "Missing required filter on %s.%s. Add a condition like %2$s = '...' or %2$s IN ('...', '...') to the WHERE clause"
                        .formatted(table.schemaTableName(), columnName));
            }
        }
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

    /**
     * Returns all values of an equality or IN-list filter, or an empty list if there's no filter on the column.
     */
    default List<String> getFilterValues(AwsColumnHandle column, TupleDomain<ColumnHandle> constraint)
    {
        checkArgument(column.type() instanceof VarcharType, "Column %s is not a varchar: %s", column.name(), column.type());
        Domain domain = constraint.getDomains()
                .map(domains -> domains.get(column))
                .orElse(null);
        if (domain == null) {
            return List.of();
        }
        return domain.getValues().getDiscreteSet().stream()
                .map(value -> ((Slice) value).toStringUtf8())
                .collect(toImmutableList());
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
