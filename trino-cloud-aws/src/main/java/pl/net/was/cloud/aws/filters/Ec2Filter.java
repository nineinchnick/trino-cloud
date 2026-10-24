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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import io.airlift.slice.Slice;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.MapType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import pl.net.was.cloud.aws.AwsColumnHandle;
import software.amazon.awssdk.services.ec2.model.Filter;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.IntegerType.INTEGER;
import static java.util.function.Function.identity;
import static pl.net.was.cloud.aws.filters.Ec2FilterNames.TAG_FILTER;

/**
 * Pushes down equality and IN predicates on the columns of an EC2 table as the filters of its describe operation.
 * EC2 filter values treat * and ? as wildcards, so AWS can return rows that don't match the predicate, and Trino still applies it.
 */
public class Ec2Filter
        implements FilterApplier
{
    private static final String TAGS_COLUMN = "tags";

    // maps column names to filter names
    private final Map<String, String> filterNames;
    private final Optional<String> tagsColumn;

    public Ec2Filter(List<ColumnMetadata> columns, Ec2FilterNames names)
    {
        ImmutableMap.Builder<String, String> filterNames = ImmutableMap.builder();
        Optional<String> tagsColumn = Optional.empty();
        for (ColumnMetadata column : columns) {
            String defaultFilterName = column.getName().equals(TAGS_COLUMN) ? TAG_FILTER : filterName(column.getName());
            String filterName = names.columnFilterNames().getOrDefault(column.getName(), defaultFilterName);
            if (!names.filterNames().contains(filterName) && !names.columnFilterNames().containsKey(column.getName())) {
                continue;
            }
            if (filterName.equals(TAG_FILTER)) {
                checkArgument(isTagsType(column.getType()), "Column %s for filter %s is not a map with varchar keys and values: %s", column.getName(), filterName, column.getType());
                tagsColumn = Optional.of(column.getName());
                continue;
            }
            checkArgument(isFilterType(column.getType()), "Column %s for filter %s has an unsupported type: %s", column.getName(), filterName, column.getType());
            filterNames.put(column.getName(), filterName);
        }
        this.filterNames = filterNames.buildOrThrow();
        this.tagsColumn = tagsColumn;
        Set<String> matchedColumns = ImmutableSet.<String>builder()
                .addAll(this.filterNames.keySet())
                .addAll(tagsColumn.stream().iterator())
                .build();
        Set<String> unknownColumns = Sets.difference(names.columnFilterNames().keySet(), matchedColumns);
        checkArgument(unknownColumns.isEmpty(), "Filters declared for columns that don't exist: %s", unknownColumns);
        Set<String> matchedFilters = ImmutableSet.<String>builder()
                .addAll(this.filterNames.values())
                .addAll(tagsColumn.map(_ -> TAG_FILTER).stream().iterator())
                .build();
        Set<String> unmatchedFilters = Sets.difference(names.filterNames(), matchedFilters);
        checkArgument(unmatchedFilters.isEmpty(), "Filters that don't match any column: %s", unmatchedFilters);
    }

    /**
     * Column names are in snake case, and EC2 filter names are in kebab case, like vpc_id and vpc-id.
     */
    public static String filterName(String columnName)
    {
        return columnName.replace('_', '-');
    }

    @Override
    public Map<String, FilterType> getSupportedFilters()
    {
        return filterNames.keySet().stream()
                .collect(toImmutableMap(identity(), _ -> FilterType.EQUAL));
    }

    @Override
    public Set<String> getUnenforcedFilters()
    {
        return filterNames.keySet();
    }

    @Override
    public Map<String, Integer> getSplitBatchSizes()
    {
        return filterNames.keySet().stream()
                .collect(toImmutableMap(identity(), _ -> ID_BATCH_SIZE));
    }

    @Override
    public Set<String> getMapEntryFilters()
    {
        return tagsColumn.map(Set::of).orElse(Set.of());
    }

    /**
     * Returns the EC2 filters for the pushed-down predicates of the constraint.
     */
    public List<Filter> filters(TupleDomain<ColumnHandle> constraint)
    {
        Map<ColumnHandle, Domain> domains = constraint.getDomains().orElse(Map.of());
        ImmutableList.Builder<Filter> filters = ImmutableList.builder();
        domains.forEach((column, domain) -> filterName((AwsColumnHandle) column, domain).ifPresent(filterName -> filters.add(Filter.builder()
                .name(filterName)
                .values(filterValues(domain))
                .build())));
        return filters.build();
    }

    private Optional<String> filterName(AwsColumnHandle column, Domain domain)
    {
        if (column.mapKey().isEmpty()) {
            return Optional.ofNullable(filterNames.get(column.name()));
        }
        // tag filters are not divided into splits, so a filter with too many values is not used, and Trino applies the predicate
        if (!tagsColumn.equals(Optional.of(column.name())) || domain.getValues().getDiscreteSet().size() > ID_BATCH_SIZE) {
            return Optional.empty();
        }
        return Optional.of("tag:" + column.mapKey().get());
    }

    private static boolean isFilterType(Type type)
    {
        return type instanceof VarcharType || type.equals(BOOLEAN) || type.equals(INTEGER) || type.equals(BIGINT);
    }

    private static boolean isTagsType(Type type)
    {
        return type instanceof MapType mapType && mapType.getKeyType() instanceof VarcharType && mapType.getValueType() instanceof VarcharType;
    }

    private static List<String> filterValues(Domain domain)
    {
        return domain.getValues().getDiscreteSet().stream()
                .map(value -> value instanceof Slice slice ? slice.toStringUtf8() : value.toString())
                .collect(toImmutableList());
    }
}
