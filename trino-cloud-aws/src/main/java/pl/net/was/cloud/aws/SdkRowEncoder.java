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
import io.trino.spi.block.Block;
import io.trino.spi.block.SqlMap;
import io.trino.spi.block.SqlRow;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.core.SdkField;
import software.amazon.awssdk.core.SdkPojo;
import software.amazon.awssdk.core.util.SdkAutoConstructList;
import software.amazon.awssdk.core.util.SdkAutoConstructMap;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

import static io.trino.spi.block.ArrayValueBuilder.buildArrayValue;
import static io.trino.spi.block.MapValueBuilder.buildMapValue;
import static io.trino.spi.block.RowValueBuilder.buildRowValue;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static pl.net.was.cloud.aws.SdkColumns.mappedFields;

final class SdkRowEncoder
{
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
                        mappedFields(o.sdkFields())
                                .stream()
                                .map(f -> encodeValue(SdkColumns.type(f), f.getValueOrDefault(o))))
                .toList();
    }

    /**
     * Returns the value in the Trino native representation of the type, as returned by {@link SdkColumns#type(SdkField)}.
     */
    // must support all types from SdkColumns.TYPE_MAP
    public static Object encodeValue(Type type, Object value)
    {
        if (isUnset(value)) {
            return null;
        }
        if (type.equals(VARCHAR)) {
            return Slices.utf8Slice(switch (value) {
                case String string -> string;
                case BigDecimal decimal -> decimal.toPlainString();
                // objects nested deeper than SdkColumns.MAX_NESTING_DEPTH and documents
                default -> value.toString();
            });
        }
        if (type.equals(BIGINT) || type.equals(INTEGER) || type.equals(SMALLINT) || type.equals(TINYINT)) {
            return ((Number) value).longValue();
        }
        if (type.equals(REAL)) {
            return (long) Float.floatToIntBits((Float) value);
        }
        if (type.equals(DOUBLE)) {
            return ((Number) value).doubleValue();
        }
        if (type.equals(BOOLEAN)) {
            return value;
        }
        if (type.equals(TIMESTAMP_TZ_MILLIS)) {
            return packDateTimeWithZone(((Instant) value).toEpochMilli(), UTC_KEY);
        }
        if (type.equals(VARBINARY)) {
            return Slices.wrappedBuffer(((SdkBytes) value).asByteArrayUnsafe());
        }
        return switch (type) {
            case RowType rowType -> encodeObject(rowType, (SdkPojo) value);
            case ArrayType arrayType -> encodeList(arrayType, (List<?>) value);
            case MapType mapType when value instanceof List<?> list -> encodeKeyValueList(mapType, list);
            case MapType mapType -> encodeMap(mapType, (Map<?, ?>) value);
            default -> throw new IllegalArgumentException("Unsupported type %s for value %s".formatted(type, value));
        };
    }

    private static SqlRow encodeObject(RowType rowType, SdkPojo object)
    {
        List<SdkField<?>> fields = mappedFields(object.sdkFields());
        return buildRowValue(rowType, fieldBuilders -> {
            for (int i = 0; i < fields.size(); i++) {
                Type fieldType = rowType.getFields().get(i).getType();
                writeNativeValue(fieldType, fieldBuilders.get(i), encodeValue(fieldType, fields.get(i).getValueOrDefault(object)));
            }
        });
    }

    private static Block encodeList(ArrayType arrayType, List<?> list)
    {
        Type elementType = arrayType.getElementType();
        return buildArrayValue(arrayType, list.size(), elementBuilder -> {
            for (Object element : list) {
                writeNativeValue(elementType, elementBuilder, encodeValue(elementType, element));
            }
        });
    }

    private static SqlMap encodeMap(MapType mapType, Map<?, ?> map)
    {
        Type valueType = mapType.getValueType();
        return buildMapValue(mapType, map.size(), (keyBuilder, valueBuilder) -> {
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                VARCHAR.writeString(keyBuilder, (String) entry.getKey());
                writeNativeValue(valueType, valueBuilder, encodeValue(valueType, entry.getValue()));
            }
        });
    }

    // see SdkColumns.isKeyValueList
    private static SqlMap encodeKeyValueList(MapType mapType, List<?> list)
    {
        // map keys must be unique and not NULL, which AWS guarantees for tags, but it doesn't hurt to check
        Map<String, String> entries = new LinkedHashMap<>();
        for (Object element : list) {
            SdkPojo entry = (SdkPojo) element;
            List<SdkField<?>> fields = entry.sdkFields();
            String key = (String) fields.get(0).getValueOrDefault(entry);
            if (key == null) {
                continue;
            }
            entries.put(key, (String) fields.get(1).getValueOrDefault(entry));
        }
        return encodeMap(mapType, entries);
    }

    // the SDK returns auto construct collections instead of null for collections missing from the response
    private static boolean isUnset(Object value)
    {
        return value == null || value instanceof SdkAutoConstructList<?> || value instanceof SdkAutoConstructMap<?, ?>;
    }
}
