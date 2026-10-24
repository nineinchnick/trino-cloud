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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableSet.toImmutableSet;

/**
 * Supports the filters of all the combined filter appliers, so filters shared by many tables, like the region,
 * are declared once and added to the filters specific to a table. Every filter applier must support different columns.
 */
public class CombinedFilter
        implements FilterApplier
{
    private final List<FilterApplier> filterAppliers;

    public CombinedFilter(List<FilterApplier> filterAppliers)
    {
        this.filterAppliers = ImmutableList.copyOf(filterAppliers);
        checkArgument(!this.filterAppliers.isEmpty(), "filterAppliers is empty");
    }

    @Override
    public Map<String, FilterType> getSupportedFilters()
    {
        return combineMaps(FilterApplier::getSupportedFilters);
    }

    @Override
    public Set<String> getUnenforcedFilters()
    {
        return combineSets(FilterApplier::getUnenforcedFilters);
    }

    @Override
    public Set<String> getRequiredFilters()
    {
        return combineSets(FilterApplier::getRequiredFilters);
    }

    @Override
    public Map<String, Integer> getSplitBatchSizes()
    {
        return combineMaps(FilterApplier::getSplitBatchSizes);
    }

    @Override
    public Map<String, Supplier<List<String>>> getKnownValues()
    {
        return combineMaps(FilterApplier::getKnownValues);
    }

    private <V> Map<String, V> combineMaps(Function<FilterApplier, Map<String, V>> getter)
    {
        ImmutableMap.Builder<String, V> combined = ImmutableMap.builder();
        filterAppliers.forEach(filterApplier -> combined.putAll(getter.apply(filterApplier)));
        return combined.buildOrThrow();
    }

    private Set<String> combineSets(Function<FilterApplier, Set<String>> getter)
    {
        return filterAppliers.stream()
                .map(getter)
                .flatMap(Collection::stream)
                .collect(toImmutableSet());
    }
}
