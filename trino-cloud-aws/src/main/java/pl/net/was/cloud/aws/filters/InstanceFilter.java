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

public class InstanceFilter
        implements FilterApplier
{
    @Override
    public Map<String, FilterType> getSupportedFilters()
    {
        return ImmutableMap.of(
                "instance_id", FilterType.EQUAL);
    }

    // EC2 filter values treat * and ? as wildcards, so the filter can match more instances than requested
    @Override
    public Set<String> getUnenforcedFilters()
    {
        return Set.of("instance_id");
    }
}
