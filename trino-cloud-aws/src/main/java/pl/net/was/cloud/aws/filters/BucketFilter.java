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

import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

public class BucketFilter
        implements FilterApplier
{
    private final Supplier<List<String>> bucketNames;

    /**
     * The buckets are only listed for scans without a filter on the bucket name, because listing them requires an AWS request.
     */
    public BucketFilter(Supplier<List<String>> bucketNames)
    {
        this.bucketNames = requireNonNull(bucketNames, "bucketNames is null");
    }

    @Override
    public Map<String, FilterType> getSupportedFilters()
    {
        return ImmutableMap.of(
                "bucket_name", FilterType.EQUAL);
    }

    // every bucket is listed by a separate request
    @Override
    public Map<String, Integer> getSplitBatchSizes()
    {
        return Map.of("bucket_name", 1);
    }

    // the listed buckets are only the buckets owned by the account, but a filter can choose buckets of other accounts that grant access to them
    @Override
    public Map<String, Supplier<List<String>>> getDefaultValues()
    {
        return Map.of("bucket_name", bucketNames);
    }
}
