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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import pl.net.was.cloud.aws.AwsColumnHandle;
import pl.net.was.cloud.aws.AwsTableHandle;
import software.amazon.awssdk.core.SdkField;
import software.amazon.awssdk.core.SdkPojo;
import software.amazon.awssdk.core.protocol.MarshallingType;
import software.amazon.awssdk.core.traits.ListTrait;
import software.amazon.awssdk.services.ec2.model.Filter;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.IntegerType.INTEGER;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

/**
 * The column filters of a table, bound to its columns and to the members of its AWS request.
 * Pushes down predicates, divides scans into splits, and sets the pushed-down values in the requests.
 */
public final class TableFilters
{
    // keeps requests well under the limits of AWS, like 200 values of an EC2 filter, while dividing large IN lists into parallel splits
    public static final int BATCH_SIZE = 100;

    private static final Logger log = Logger.get(TableFilters.class);

    private final List<BoundFilter> filters;
    private final Optional<SdkField<?>> ec2FiltersMember;

    private TableFilters(List<BoundFilter> filters, Optional<SdkField<?>> ec2FiltersMember)
    {
        this.filters = ImmutableList.copyOf(filters);
        this.ec2FiltersMember = requireNonNull(ec2FiltersMember, "ec2FiltersMember is null");
    }

    /**
     * Request members are found in the fields of the SDK request, so a list member accepts a batch of values, and a scalar member a single value.
     */
    public static TableFilters create(SchemaTableName table, List<ColumnMetadata> columns, List<ColumnFilter> filters, List<SdkField<?>> requestFields)
    {
        Map<String, ColumnMetadata> columnsByName = columns.stream()
                .collect(toImmutableMap(ColumnMetadata::getName, identity()));
        Map<String, SdkField<?>> requestMembers = requestFields.stream()
                .collect(toImmutableMap(SdkField::memberName, identity()));
        // the list of filters is named Filters in most EC2 requests, but Filter in some, so it's found by the type of its elements
        Optional<SdkField<?>> ec2FiltersMember = requestFields.stream()
                .filter(TableFilters::isEc2FilterList)
                .findFirst();
        List<BoundFilter> boundFilters = filters.stream()
                .map(filter -> bind(table, filter, columnsByName, requestMembers, ec2FiltersMember))
                .collect(toImmutableList());
        checkArgument(boundFilters.stream().map(filter -> filter.column().name()).distinct().count() == boundFilters.size(), "%s has many filters on the same column", table);
        checkArgument(boundFilters.stream().filter(filter -> filter.filter().kind() == ColumnFilter.Kind.PARTITION).count() <= 1, "%s has many partition filters", table);
        checkArgument(boundFilters.stream().filter(filter -> !filter.filter().missingValueErrorCodes().isEmpty()).count() <= 1, "%s has many filters that skip missing values", table);
        return new TableFilters(boundFilters, ec2FiltersMember);
    }

    /**
     * Names of the columns with a filter.
     */
    public Set<String> columns()
    {
        return filters.stream()
                .map(filter -> filter.column().name())
                .collect(toImmutableSet());
    }

    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(AwsTableHandle table, Constraint constraint)
    {
        TupleDomain<ColumnHandle> summary = constraint.getSummary();
        if (summary.isNone() || table.constraint().isNone()) {
            return Optional.empty();
        }
        Map<ColumnHandle, Domain> currentDomains = table.constraint().getDomains().orElseThrow();
        TupleDomain<ColumnHandle> pushedConstraint = table.constraint();
        TupleDomain<ColumnHandle> remainingFilter = summary;
        boolean found = false;
        for (BoundFilter filter : filters) {
            Optional<Domain> domain = pushableDomain(filter, summary);
            if (domain.isEmpty()) {
                continue;
            }
            AwsColumnHandle column = filter.column();
            // pushing down the same predicate again would make the planner loop, and the engine applies a different one
            if (currentDomains.containsKey(column)) {
                log.debug("Not pushing down filter on %s because a filter was already pushed down: %s", column.name(), domain.get());
                continue;
            }
            pushedConstraint = pushedConstraint.intersect(TupleDomain.withColumnDomains(Map.of(column, domain.get())));
            found = true;
            if (filter.filter().enforced()) {
                remainingFilter = remainingFilter.filter((columnHandle, _) -> !columnHandle.equals(column));
            }
        }
        if (!found) {
            return Optional.empty();
        }
        return Optional.of(new ConstraintApplicationResult<>(
                table.withConstraint(pushedConstraint),
                remainingFilter,
                constraint.getExpression(),
                false));
    }

    /**
     * Every filter divides the scan into batches of its values, so splits read all combinations of the batches of every filter.
     * Returns the constraints of the splits, which select a part of the values of the given table constraint.
     */
    public List<TupleDomain<ColumnHandle>> splitConstraints(TupleDomain<ColumnHandle> constraint)
    {
        if (constraint.isNone()) {
            return ImmutableList.of();
        }
        Map<ColumnHandle, Domain> domains = constraint.getDomains().orElseThrow();
        Set<String> filteredColumns = domains.keySet().stream()
                .map(column -> ((AwsColumnHandle) column).name())
                .collect(toImmutableSet());
        List<List<TupleDomain<ColumnHandle>>> columnBatches = filters.stream()
                .map(filter -> columnBatches(filter, Optional.ofNullable(domains.get(filter.column())), filteredColumns))
                .collect(toImmutableList());
        return Lists.cartesianProduct(columnBatches).stream()
                .map(batches -> batches.stream().reduce(TupleDomain.all(), TupleDomain::intersect))
                .collect(toImmutableList());
    }

    /**
     * Sets the values of the constraint in the members of the request, which is a builder of an SDK request.
     */
    public void applyTo(SdkPojo request, TupleDomain<ColumnHandle> constraint)
    {
        Map<ColumnHandle, Domain> domains = constraint.getDomains().orElseThrow(() -> new IllegalArgumentException("Constraint matches no rows"));
        ImmutableList.Builder<Filter> ec2Filters = ImmutableList.builder();
        for (BoundFilter filter : filters) {
            Domain domain = domains.get(filter.column());
            if (domain == null) {
                continue;
            }
            switch (filter.filter().kind()) {
                case REQUEST_MEMBER -> setMember(filter, request, domain);
                case EC2_FILTER -> ec2Filters.add(Filter.builder()
                        .name(filter.filter().target())
                        .values(ec2FilterValues(domain))
                        .build());
                // the partition chooses the client, so it's not a part of the request
                case PARTITION -> {}
            }
        }
        List<Filter> builtFilters = ec2Filters.build();
        if (!builtFilters.isEmpty()) {
            ec2FiltersMember.orElseThrow().set(request, builtFilters);
        }
    }

    public Optional<Partition> partition(TupleDomain<ColumnHandle> constraint)
    {
        return filters.stream()
                .filter(filter -> filter.filter().kind() == ColumnFilter.Kind.PARTITION)
                .findFirst()
                .flatMap(filter -> singleValue(filter.column(), constraint)
                        .map(value -> new Partition(filter.column().name(), ((Slice) value).toStringUtf8())));
    }

    /**
     * Values of the filters that accept a single value per request, like the partition or the bucket name, which are the same for every row read by the request.
     */
    public Map<String, Object> requestValues(TupleDomain<ColumnHandle> constraint)
    {
        return filters.stream()
                .filter(filter -> filter.batchSize() == 1)
                .flatMap(filter -> singleValue(filter.column(), constraint).map(value -> Map.entry(filter.column().name(), value)).stream())
                .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    /**
     * The values of the filter that skips missing values, which have to be requested separately when one of them doesn't exist.
     */
    public Optional<MissingValues> missingValues(TupleDomain<ColumnHandle> constraint)
    {
        Map<ColumnHandle, Domain> domains = constraint.getDomains().orElse(Map.of());
        return filters.stream()
                .filter(filter -> !filter.filter().missingValueErrorCodes().isEmpty() && domains.containsKey(filter.column()))
                .findFirst()
                .map(filter -> new MissingValues(filter.column(), domains.get(filter.column()).getValues().getDiscreteSet(), filter.filter().missingValueErrorCodes()));
    }

    private static BoundFilter bind(
            SchemaTableName table,
            ColumnFilter filter,
            Map<String, ColumnMetadata> columns,
            Map<String, SdkField<?>> requestMembers,
            Optional<SdkField<?>> ec2FiltersMember)
    {
        ColumnMetadata columnMetadata = columns.get(filter.column());
        checkArgument(columnMetadata != null, "Filter column %s does not exist in %s", filter.column(), table);
        Type type = columnMetadata.getType();
        AwsColumnHandle column = new AwsColumnHandle(columnMetadata.getName(), type);
        checkArgument(
                (filter.knownValues().isEmpty() && filter.defaultValues().isEmpty()) || type instanceof VarcharType,
                "Column %s with known or default values is not a varchar: %s",
                column.name(),
                type);
        return switch (filter.kind()) {
            case REQUEST_MEMBER -> {
                SdkField<?> member = requestMembers.get(filter.target());
                checkArgument(member != null, "Request of %s does not have a %s member for the filter on %s", table, filter.target(), column.name());
                boolean list = member.marshallingType() == MarshallingType.LIST;
                SdkField<?> valueField = list ? member.getRequiredTrait(ListTrait.class).memberFieldInfo() : member;
                checkArgument(isMemberType(type, valueField.marshallingType()), "Column %s of type %s can't set the %s member of the request of %s", column.name(), type, member.memberName(), table);
                yield new BoundFilter(filter, column, Optional.of(member), list ? BATCH_SIZE : 1);
            }
            case EC2_FILTER -> {
                checkArgument(ec2FiltersMember.isPresent(), "Request of %s does not have EC2 filters for the filter on %s", table, column.name());
                checkArgument(isEc2FilterType(type), "Column %s for EC2 filter %s has an unsupported type: %s", column.name(), filter.target(), type);
                yield new BoundFilter(filter, column, Optional.empty(), BATCH_SIZE);
            }
            case PARTITION -> {
                checkArgument(type instanceof VarcharType, "Partition column %s is not a varchar: %s", column.name(), type);
                yield new BoundFilter(filter, column, Optional.empty(), 1);
            }
        };
    }

    // predicates that allow NULL values are not pushed down, because AWS never returns rows without the filtered values
    private static Optional<Domain> pushableDomain(BoundFilter filter, TupleDomain<ColumnHandle> summary)
    {
        Domain domain = summary.getDomains().orElseThrow().get(filter.column());
        if (domain == null) {
            return Optional.empty();
        }
        if (domain.isNullAllowed() || !domain.getValues().isDiscreteSet()) {
            log.debug("Not pushing down filter on %s because it's not a discrete set of values: %s", filter.column().name(), domain);
            return Optional.empty();
        }
        return Optional.of(domain);
    }

    // a column that doesn't divide the scan has a single batch without a constraint
    private static List<TupleDomain<ColumnHandle>> columnBatches(BoundFilter filter, Optional<Domain> filteredDomain, Set<String> filteredColumns)
    {
        AwsColumnHandle column = filter.column();
        Optional<Supplier<List<String>>> knownValues = filter.filter().knownValues();
        Optional<Domain> domain = filteredDomain
                .map(filtered -> knownValues
                        .map(values -> knownDomain(column, values.get()).intersect(filtered))
                        .orElse(filtered))
                .or(() -> knownValues.or(() -> defaultValues(filter.filter(), filteredColumns)).map(values -> knownDomain(column, values.get())));
        if (domain.isPresent() && domain.get().isNone()) {
            return ImmutableList.of();
        }
        if (domain.isEmpty() || domain.get().isNullAllowed() || !domain.get().getValues().isDiscreteSet()) {
            return ImmutableList.of(TupleDomain.all());
        }
        List<Object> values = domain.get().getValues().getDiscreteSet();
        // the table handle already selects these values, and a single request accepts all of them
        if (domain.equals(filteredDomain) && values.size() <= filter.batchSize()) {
            return ImmutableList.of(TupleDomain.all());
        }
        return Lists.partition(values, filter.batchSize()).stream()
                .map(batch -> TupleDomain.<ColumnHandle>withColumnDomains(Map.of(column, Domain.multipleValues(column.type(), batch))))
                .collect(toImmutableList());
    }

    private static Optional<Supplier<List<String>>> defaultValues(ColumnFilter filter, Set<String> filteredColumns)
    {
        if (filter.defaultValuesUnlessFiltered().stream().anyMatch(filteredColumns::contains)) {
            return Optional.empty();
        }
        return filter.defaultValues();
    }

    private static Domain knownDomain(AwsColumnHandle column, List<String> values)
    {
        return Domain.create(ValueSet.copyOf(column.type(), values.stream().map(Slices::utf8Slice).collect(toImmutableList())), false);
    }

    private static void setMember(BoundFilter filter, SdkPojo request, Domain domain)
    {
        SdkField<?> member = filter.member().orElseThrow();
        List<Object> values = domain.getValues().getDiscreteSet();
        if (member.marshallingType() == MarshallingType.LIST) {
            SdkField<?> valueField = member.getRequiredTrait(ListTrait.class).memberFieldInfo();
            member.set(request, values.stream()
                    .map(value -> sdkValue(valueField, value))
                    .collect(toImmutableList()));
            return;
        }
        // the split manager divides scans into a split for every value of a scalar member
        checkState(values.size() == 1, "Filter on %s has many values for the %s member: %s", filter.column().name(), member.memberName(), domain);
        member.set(request, sdkValue(member, values.getFirst()));
    }

    private static Object sdkValue(SdkField<?> field, Object value)
    {
        MarshallingType<?> type = field.marshallingType();
        if (type == MarshallingType.STRING) {
            return ((Slice) value).toStringUtf8();
        }
        if (type == MarshallingType.INTEGER) {
            return toIntExact((long) value);
        }
        return value;
    }

    private static List<String> ec2FilterValues(Domain domain)
    {
        return domain.getValues().getDiscreteSet().stream()
                .map(value -> value instanceof Slice slice ? slice.toStringUtf8() : value.toString())
                .collect(toImmutableList());
    }

    private static Optional<Object> singleValue(AwsColumnHandle column, TupleDomain<ColumnHandle> constraint)
    {
        return constraint.getDomains()
                .map(domains -> domains.get(column))
                .filter(Domain::isSingleValue)
                .map(Domain::getSingleValue);
    }

    private static boolean isMemberType(Type type, MarshallingType<?> memberType)
    {
        if (memberType == MarshallingType.STRING) {
            return type instanceof VarcharType;
        }
        if (memberType == MarshallingType.INTEGER) {
            return type.equals(INTEGER);
        }
        if (memberType == MarshallingType.LONG) {
            return type.equals(BIGINT);
        }
        if (memberType == MarshallingType.BOOLEAN) {
            return type.equals(BOOLEAN);
        }
        return false;
    }

    private static boolean isEc2FilterType(Type type)
    {
        return type instanceof VarcharType || type.equals(BOOLEAN) || type.equals(INTEGER) || type.equals(BIGINT);
    }

    private static boolean isEc2FilterList(SdkField<?> field)
    {
        return field.getOptionalTrait(ListTrait.class)
                .map(list -> list.memberFieldInfo().constructor() != null && list.memberFieldInfo().constructor().get() instanceof Filter.Builder)
                .orElse(false);
    }

    /**
     * The value of the partition column, which chooses the client that sends the requests.
     */
    public record Partition(String column, String value)
    {
        public Partition
        {
            requireNonNull(column, "column is null");
            requireNonNull(value, "value is null");
        }
    }

    public record MissingValues(AwsColumnHandle column, List<Object> values, Set<String> errorCodes)
    {
        public MissingValues
        {
            requireNonNull(column, "column is null");
            values = ImmutableList.copyOf(values);
            errorCodes = Set.copyOf(errorCodes);
        }
    }

    private record BoundFilter(ColumnFilter filter, AwsColumnHandle column, Optional<SdkField<?>> member, int batchSize)
    {
        private BoundFilter
        {
            requireNonNull(filter, "filter is null");
            requireNonNull(column, "column is null");
            requireNonNull(member, "member is null");
        }
    }
}
