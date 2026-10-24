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

import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.Set;

public class ImageFilter
        implements FilterApplier
{
    @Override
    public Map<String, FilterType> getSupportedFilters()
    {
        return ImmutableMap.of(
                "image_id", FilterType.EQUAL,
                "name", FilterType.EQUAL,
                "owner_id", FilterType.EQUAL);
    }

    // owners can be given as aliases, like self or amazon, but the owner_id column always holds account IDs.
    // EC2 filter values treat * and ? as wildcards, so the name filter can match more images than requested.
    // AMI names can't contain *, ? or \, so a pattern always matches a superset of the exact name.
    // AWS matches image IDs exactly, but Trino still checks them, so the row source can use either the parameter or the filter.
    @Override
    public Set<String> getUnenforcedFilters()
    {
        return Set.of("owner_id", "image_id", "name");
    }

    @Override
    public Map<String, Integer> getSplitBatchSizes()
    {
        return Map.of("image_id", ID_BATCH_SIZE);
    }
}
