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

import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

/**
 * Chooses the regions that regional tables read. It's enforced, because every row read from a region has that region in the region column.
 */
public class RegionFilter
        implements FilterApplier
{
    public static final String REGION_COLUMN = "region";

    private final Supplier<List<String>> regions;

    /**
     * The regions are only listed when splits are created, because with aws.regions=* they're listed using an AWS request.
     */
    public RegionFilter(Supplier<List<String>> regions)
    {
        this.regions = requireNonNull(regions, "regions is null");
    }

    @Override
    public Map<String, FilterType> getSupportedFilters()
    {
        return Map.of(REGION_COLUMN, FilterType.EQUAL);
    }

    // every region has a separate endpoint
    @Override
    public Map<String, Integer> getSplitBatchSizes()
    {
        return Map.of(REGION_COLUMN, 1);
    }

    @Override
    public Map<String, List<String>> getKnownValues()
    {
        return Map.of(REGION_COLUMN, regions.get());
    }
}
