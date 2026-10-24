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

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

/**
 * Declares which part of the AWS request receives the values of equality and IN predicates on a column.
 * {@link TableFilters} pushes down the predicates, divides scans into splits, and sets the values in the requests.
 */
public final class ColumnFilter
{
    /**
     * The name of the EC2 filters on the values of tags with a given key, as listed in the AWS API reference.
     */
    public static final String TAG_FILTERS = "tag:<key>";
    private static final String TAGS_COLUMN = "tags";

    private final String column;
    private final Kind kind;
    private final Optional<String> target;
    private final boolean enforced;
    private final boolean prefixes;
    private final Optional<Supplier<List<String>>> knownValues;
    private final Optional<Supplier<List<String>>> defaultValues;
    private final Set<String> defaultValuesUnlessFiltered;
    private final Set<String> missingValueErrorCodes;

    private ColumnFilter(Builder builder)
    {
        this.column = requireNonNull(builder.column, "column is null");
        this.kind = requireNonNull(builder.kind, "kind is null");
        this.target = requireNonNull(builder.target, "target is null");
        this.enforced = builder.enforced;
        this.prefixes = builder.prefixes;
        this.knownValues = requireNonNull(builder.knownValues, "knownValues is null");
        this.defaultValues = requireNonNull(builder.defaultValues, "defaultValues is null");
        this.defaultValuesUnlessFiltered = ImmutableSet.copyOf(builder.defaultValuesUnlessFiltered);
        this.missingValueErrorCodes = ImmutableSet.copyOf(builder.missingValueErrorCodes);
        checkArgument(knownValues.isEmpty() || defaultValues.isEmpty(), "Filter on %s has both known and default values", column);
        // AWS returns all values with the prefix, not only the matching ones
        checkArgument(!prefixes || !enforced, "Filter on %s pushes down prefixes, so it must be unenforced", column);
        checkArgument(!prefixes || kind == Kind.REQUEST_MEMBER || kind == Kind.EC2_FILTER, "Filter on %s can't push down prefixes", column);
    }

    /**
     * Sets the values in a member of the request, named like in the AWS API reference, like InstanceIds.
     * A list member accepts a batch of values, and a scalar member accepts a single value, so scans with more values are divided into splits.
     * AWS returns only the rows matching the values, so Trino doesn't apply the predicate again, unless the filter is {@link #unenforced()}.
     */
    public static ColumnFilter requestMember(String column, String memberName)
    {
        return new Builder(column, Kind.REQUEST_MEMBER, Optional.of(memberName), true).build();
    }

    /**
     * Adds an EC2 filter with the values to the request, like vpc-id. EC2 filter values treat * and ? as wildcards,
     * so AWS can return rows that don't match the predicate, and Trino still applies it.
     */
    public static ColumnFilter ec2Filter(String column, String filterName)
    {
        return new Builder(column, Kind.EC2_FILTER, Optional.of(filterName), false).build();
    }

    /**
     * EC2 filters on the columns with the same name in snake case, like vpc_id for vpc-id, and tag filters on the tags column for {@link #TAG_FILTERS}.
     * The names of the filters accepted by an EC2 API operation are not a part of the SDK model, so every table declares them, as listed in the AWS API reference.
     */
    public static List<ColumnFilter> ec2Filters(String... filterNames)
    {
        return Stream.of(filterNames)
                .map(filterName -> filterName.equals(TAG_FILTERS) ? tagFilters(TAGS_COLUMN) : ec2Filter(filterName.replace('-', '_'), filterName))
                .collect(toImmutableList());
    }

    /**
     * Adds an EC2 filter for every key of the map column used in equality and IN predicates, like tag:Name for tags['Name'] = 'value'.
     * EC2 filter values treat * and ? as wildcards, and the engine only removes predicates on whole columns from the filter it applies,
     * so Trino still applies the predicate.
     */
    public static ColumnFilter tagFilters(String column)
    {
        return new Builder(column, Kind.TAG_FILTERS, Optional.empty(), false).build();
    }

    /**
     * Sets the common prefix of the matching values, like the prefix of key LIKE 'prefix%', in a scalar request member.
     * Single values are also used as prefixes, so AWS can return rows that only start with them, and Trino still applies the predicate.
     */
    public static ColumnFilter keyPrefix(String column, String memberName)
    {
        return requestMember(column, memberName)
                .unenforced()
                .withPrefixes();
    }

    /**
     * Chooses the client that sends the requests, like the client of a region, so every split reads a single partition.
     * Every row read from a partition has its value, so Trino doesn't apply the predicate again.
     */
    public static ColumnFilter partition(String column)
    {
        return new Builder(column, Kind.PARTITION, Optional.empty(), true).build();
    }

    public ColumnFilter unenforced()
    {
        Builder builder = toBuilder();
        builder.enforced = false;
        return builder.build();
    }

    /**
     * Also pushes down ranges of values that start with the same prefix, like the range of LIKE 'prefix%',
     * which request members receive as the prefix, and EC2 filters as the prefix followed by the * wildcard.
     * The filter must be unenforced, because AWS returns all values with the prefix.
     */
    public ColumnFilter withPrefixes()
    {
        Builder builder = toBuilder();
        builder.prefixes = true;
        return builder.build();
    }

    /**
     * All the values that can be read, like the configured regions. Scans are divided into a split for every batch of the values,
     * also without a filter on the column, and filter values that are not known match no rows.
     * The values are only listed when splits are created, because listing them can require an AWS request.
     */
    public ColumnFilter knownValues(Supplier<List<String>> values)
    {
        Builder builder = toBuilder();
        builder.knownValues = Optional.of(values);
        return builder.build();
    }

    /**
     * Values read by scans without a filter on the column, or on any of the other given columns. Unlike known values,
     * they don't restrict the filter values, because they might not include every value that can be read.
     * The values are only listed for scans that use them, because listing them can require an AWS request.
     */
    public ColumnFilter defaultValues(Supplier<List<String>> values, String... unlessFilteredColumns)
    {
        Builder builder = toBuilder();
        builder.defaultValues = Optional.of(values);
        builder.defaultValuesUnlessFiltered = ImmutableSet.copyOf(unlessFilteredColumns);
        return builder.build();
    }

    /**
     * AWS fails the whole request when one of the values doesn't exist, with one of the given error codes,
     * so then every value is requested separately, and the values that fail with these codes return no rows.
     */
    public ColumnFilter skipMissingValues(Set<String> errorCodes)
    {
        Builder builder = toBuilder();
        builder.missingValueErrorCodes = ImmutableSet.copyOf(errorCodes);
        return builder.build();
    }

    public String column()
    {
        return column;
    }

    Kind kind()
    {
        return kind;
    }

    /**
     * The name of the request member or the EC2 filter.
     */
    String target()
    {
        return target.orElseThrow(() -> new IllegalStateException("Filter on %s doesn't have a target".formatted(column)));
    }

    boolean enforced()
    {
        return enforced;
    }

    boolean prefixes()
    {
        return prefixes;
    }

    Optional<Supplier<List<String>>> knownValues()
    {
        return knownValues;
    }

    Optional<Supplier<List<String>>> defaultValues()
    {
        return defaultValues;
    }

    Set<String> defaultValuesUnlessFiltered()
    {
        return defaultValuesUnlessFiltered;
    }

    Set<String> missingValueErrorCodes()
    {
        return missingValueErrorCodes;
    }

    private Builder toBuilder()
    {
        Builder builder = new Builder(column, kind, target, enforced);
        builder.prefixes = prefixes;
        builder.knownValues = knownValues;
        builder.defaultValues = defaultValues;
        builder.defaultValuesUnlessFiltered = defaultValuesUnlessFiltered;
        builder.missingValueErrorCodes = missingValueErrorCodes;
        return builder;
    }

    enum Kind
    {
        REQUEST_MEMBER,
        EC2_FILTER,
        TAG_FILTERS,
        PARTITION,
    }

    private static final class Builder
    {
        private final String column;
        private final Kind kind;
        private final Optional<String> target;
        private boolean enforced;
        private boolean prefixes;
        private Optional<Supplier<List<String>>> knownValues = Optional.empty();
        private Optional<Supplier<List<String>>> defaultValues = Optional.empty();
        private Set<String> defaultValuesUnlessFiltered = ImmutableSet.of();
        private Set<String> missingValueErrorCodes = ImmutableSet.of();

        private Builder(String column, Kind kind, Optional<String> target, boolean enforced)
        {
            this.column = column;
            this.kind = kind;
            this.target = target;
            this.enforced = enforced;
        }

        private ColumnFilter build()
        {
            return new ColumnFilter(this);
        }
    }
}
