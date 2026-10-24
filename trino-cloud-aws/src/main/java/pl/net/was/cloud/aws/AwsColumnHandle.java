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

import io.airlift.slice.SizeOf;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.type.MapType;
import io.trino.spi.type.Type;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.util.Objects.requireNonNull;

/**
 * A column, or the value of a single entry of a map column, which is only used in pushed-down constraints,
 * for predicates like tags['Name'] = 'value'.
 */
public record AwsColumnHandle(String name, Type type, Optional<String> mapKey)
        implements ColumnHandle
{
    private static final int INSTANCE_SIZE = instanceSize(AwsColumnHandle.class);

    public AwsColumnHandle
    {
        requireNonNull(name, "name is null");
        requireNonNull(type, "type is null");
        requireNonNull(mapKey, "mapKey is null");
    }

    public AwsColumnHandle(String name, Type type)
    {
        this(name, type, Optional.empty());
    }

    /**
     * The type of the map entry is the type of the map values.
     */
    public static AwsColumnHandle mapEntry(AwsColumnHandle mapColumn, String key)
    {
        checkArgument(mapColumn.type() instanceof MapType && mapColumn.mapKey().isEmpty(), "Column %s is not a map: %s", mapColumn.name(), mapColumn.type());
        return new AwsColumnHandle(mapColumn.name(), ((MapType) mapColumn.type()).getValueType(), Optional.of(key));
    }

    // type instances are shared, so they are not retained by the handle
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + estimatedSizeOf(name) + sizeOf(mapKey, SizeOf::estimatedSizeOf);
    }
}
