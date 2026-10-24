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

package pl.net.was.cloud.aws;

import com.google.common.collect.ImmutableMap;
import software.amazon.awssdk.core.SdkPojo;

import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * An SDK object returned by AWS, and the values of the pushed-down filters that are the same for all objects returned for the same request,
 * like the name of the bucket that objects are listed from, which derived columns can read.
 */
public record SdkRow(SdkPojo object, Map<String, Object> requestValues)
{
    public SdkRow
    {
        requireNonNull(object, "object is null");
        requestValues = ImmutableMap.copyOf(requestValues);
    }

    public SdkRow(SdkPojo object)
    {
        this(object, ImmutableMap.of());
    }

    /**
     * Returns the value in the Trino native representation of the column type, or null if the request didn't have a single value of the column.
     */
    public Object requestValue(String column)
    {
        return requestValues.get(column);
    }
}
