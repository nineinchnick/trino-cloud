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

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * Declares which part of the AWS request receives the values of equality and IN predicates on a column.
 * {@link TableFilters} pushes down the predicates, divides scans into splits, and sets the values in the requests.
 */
public final class ColumnFilter
{
    private final String column;
    private final Kind kind;
    private final Optional<String> target;
    private final boolean enforced;
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
        this.knownValues = requireNonNull(builder.knownValues, "knownValues is null");
        this.defaultValues = requireNonNull(builder.defaultValues, "defaultValues is null");
        this.defaultValuesUnlessFiltered = ImmutableSet.copyOf(builder.defaultValuesUnlessFiltered);
        this.missingValueErrorCodes = ImmutableSet.copyOf(builder.missingValueErrorCodes);
        checkArgument(knownValues.isEmpty() || defaultValues.isEmpty(), "Filter on %s has both known and default values", column);
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
        PARTITION,
    }

    private static final class Builder
    {
        private final String column;
        private final Kind kind;
        private final Optional<String> target;
        private boolean enforced;
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
