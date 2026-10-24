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

import software.amazon.awssdk.core.SdkPojo;

import static java.util.Objects.requireNonNull;

/**
 * An SDK object returned by AWS, and an optional context shared by all objects returned for the same request,
 * like the name of the bucket that objects are listed from, which derived columns can read.
 */
public record SdkRow(SdkPojo object, Object context)
{
    public SdkRow
    {
        requireNonNull(object, "object is null");
    }

    public SdkRow(SdkPojo object)
    {
        this(object, null);
    }
}
