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

import io.airlift.slice.Slices;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.MapBlockBuilder;
import io.trino.spi.block.SqlMap;
import io.trino.spi.type.MapType;
import io.trino.spi.type.TypeOperators;
import software.amazon.awssdk.core.SdkField;
import software.amazon.awssdk.core.SdkPojo;
import software.amazon.awssdk.core.protocol.MarshallingType;
import software.amazon.awssdk.core.traits.ListTrait;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_SECOND;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNullElse;

final class SdkRowEncoder
{
    private static final MapType mapType = new MapType(VARCHAR, VARCHAR, new TypeOperators());

    private SdkRowEncoder() {}

    public static Iterable<List<?>> encodeRows(List<? extends SdkPojo> objects)
    {
        return objects
                .stream()
                .<List<?>>map(r -> encodeRow(r, List.of()))
                .toList();
    }

    public static Iterable<List<?>> encodeRows(List<? extends SdkPojo> objects, List<Function<SdkPojo, Object>> prependValues)
    {
        return objects
                .stream()
                .<List<?>>map(r -> encodeRow(r, prependValues))
                .toList();
    }

    public static List<?> encodeRow(SdkPojo o, List<Function<SdkPojo, Object>> prependValues)
    {
        return Stream.concat(
                        prependValues
                                .stream()
                                .map(f -> f.apply(o)),
                        o.sdkFields()
                                .stream()
                                .map(f -> encodeField(f, f.getValueOrDefault(o))))
                .toList();
    }

    // must support all types from SdkColumns.TYPE_MAP
    private static Object encodeField(SdkField<?> f, Object o)
    {
        MarshallingType<?> sdkType = f.marshallingType();
        if (sdkType == MarshallingType.STRING) {
            if (o == null) {
                return null;
            }
            return Slices.utf8Slice((String) o);
        }
        if (sdkType == MarshallingType.INSTANT) {
            if (o == null) {
                return null;
            }
            return ((Instant) o).getEpochSecond() * MICROSECONDS_PER_SECOND;
        }
        if (sdkType == MarshallingType.SHORT || sdkType == MarshallingType.INTEGER || sdkType == MarshallingType.LONG
                || sdkType == MarshallingType.FLOAT || sdkType == MarshallingType.DOUBLE
                || sdkType == MarshallingType.BOOLEAN) {
            return o;
        }
        if (sdkType == MarshallingType.SDK_POJO) {
            return encodeSdkPojo((SdkPojo) o);
        }
        if (sdkType == MarshallingType.MAP) {
            return encodeMap((Map<?, ?>) o);
        }
        if (sdkType == MarshallingType.LIST) {
            List<?> list = requireNonNullElse((List<?>) o, List.of());
            if (f.containsTrait(ListTrait.class) &&
                    f.getTrait(ListTrait.class).memberFieldInfo().marshallingType() == MarshallingType.SDK_POJO) {
                MapBlockBuilder mapBlockBuilder = mapType.createBlockBuilder(null, list.size());
                for (Object value : list) {
                    SqlMap encodedValue = encodeSdkPojo((SdkPojo) value);
                    if (encodedValue == null) {
                        mapBlockBuilder.appendNull();
                        continue;
                    }
                    mapType.writeObject(mapBlockBuilder, encodedValue);
                }
                return mapBlockBuilder.build();
            }
            BlockBuilder varcharBlockBuilder = VARCHAR.createBlockBuilder(null, list.size());
            for (Object value : list) {
                VARCHAR.writeString(varcharBlockBuilder, value.toString());
            }
            return varcharBlockBuilder.build();
        }
        if (o == null) {
            return null;
        }
        return o.toString();
    }

    private static SqlMap encodeSdkPojo(SdkPojo sdkPojo)
    {
        if (sdkPojo == null) {
            return null;
        }
        MapBlockBuilder values = mapType.createBlockBuilder(null, sdkPojo.sdkFields().size());
        values.buildEntry((keyBuilder, valueBuilder) -> {
            for (SdkField<?> field : sdkPojo.sdkFields()) {
                VARCHAR.writeString(keyBuilder, field.memberName());
                writeNullableString(valueBuilder, field.getValueOrDefault(sdkPojo));
            }
        });
        return mapType.getObject(values.build(), 0);
    }

    private static SqlMap encodeMap(Map<?, ?> map)
    {
        if (map == null) {
            return null;
        }
        MapBlockBuilder values = mapType.createBlockBuilder(null, map.size());
        values.buildEntry((keyBuilder, valueBuilder) -> {
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                VARCHAR.writeString(keyBuilder, (String) entry.getKey());
                writeNullableString(valueBuilder, entry.getValue());
            }
        });
        return mapType.getObject(values.build(), 0);
    }

    // writing NULL values instead of skipping keys keeps the subscript operator working, since it fails for missing keys
    private static void writeNullableString(BlockBuilder blockBuilder, Object value)
    {
        if (value == null) {
            blockBuilder.appendNull();
            return;
        }
        VARCHAR.writeString(blockBuilder, value.toString());
    }
}
