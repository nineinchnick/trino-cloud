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
import com.google.common.collect.ImmutableSet;

import java.util.Map;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * Names of the filters accepted by an EC2 describe operation, from the AWS API reference, because they're not in the SDK model.
 * A filter is pushed down for the column with the same name in snake case, like vpc_id for vpc-id,
 * or for the column it's explicitly declared for, when the names differ.
 */
public record Ec2FilterNames(Set<String> filterNames, Map<String, String> columnFilterNames)
{
    /**
     * Filters on the value of a tag with a given key, pushed down for map entries of the tags column, like tags['Name'] = 'value'.
     */
    public static final String TAG_FILTER = "tag:<key>";

    public Ec2FilterNames
    {
        filterNames = ImmutableSet.copyOf(requireNonNull(filterNames, "filterNames is null"));
        columnFilterNames = ImmutableMap.copyOf(requireNonNull(columnFilterNames, "columnFilterNames is null"));
    }

    public static Ec2FilterNames ec2Filters(String... filterNames)
    {
        return new Ec2FilterNames(ImmutableSet.copyOf(filterNames), ImmutableMap.of());
    }

    public Ec2FilterNames withColumnFilter(String columnName, String filterName)
    {
        return new Ec2FilterNames(filterNames, ImmutableMap.<String, String>builder()
                .putAll(columnFilterNames)
                .put(columnName, filterName)
                .buildOrThrow());
    }
}
