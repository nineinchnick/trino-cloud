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
import io.trino.spi.block.ArrayBlockBuilder;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.MapBlockBuilder;
import io.trino.spi.block.RowBlockBuilder;
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

    /**
     * Returns a writer of the value of the field of the SDK object, in the type returned by {@link SdkColumns#type(SdkField)}.
     */
    public static ColumnWriter fieldWriter(SdkField<?> field)
    {
        Type type = SdkColumns.type(field);
        return (row, output) -> writeValue(type, output, field.getValueOrDefault(row.object()));
    }

    /**
     * Returns a writer of a value computed from the row, which must be in the Trino native representation of the type.
     */
    public static ColumnWriter derivedWriter(Type type, Function<SdkRow, Object> value)
    {
        return (row, output) -> writeNativeValue(type, output, value.apply(row));
    }

    /**
     * Writes the value directly into the output, without creating intermediate values for nested objects, lists, and maps.
     * Must support all types from {@code SdkColumns.TYPE_MAP}.
     */
    public static void writeValue(Type type, BlockBuilder output, Object value)
    {
        if (isUnset(value)) {
            output.appendNull();
            return;
        }
        if (type.equals(VARCHAR)) {
            VARCHAR.writeString(output, switch (value) {
                case String string -> string;
                case BigDecimal decimal -> decimal.toPlainString();
                // objects nested deeper than SdkColumns.MAX_NESTING_DEPTH and documents
                default -> value.toString();
            });
            return;
        }
        if (type.equals(BIGINT) || type.equals(INTEGER) || type.equals(SMALLINT) || type.equals(TINYINT)) {
            type.writeLong(output, ((Number) value).longValue());
            return;
        }
        if (type.equals(REAL)) {
            REAL.writeFloat(output, (Float) value);
            return;
        }
        if (type.equals(DOUBLE)) {
            DOUBLE.writeDouble(output, ((Number) value).doubleValue());
            return;
        }
        if (type.equals(BOOLEAN)) {
            BOOLEAN.writeBoolean(output, (Boolean) value);
            return;
        }
        if (type.equals(TIMESTAMP_TZ_MILLIS)) {
            TIMESTAMP_TZ_MILLIS.writeLong(output, packDateTimeWithZone(((Instant) value).toEpochMilli(), UTC_KEY));
            return;
        }
        if (type.equals(VARBINARY)) {
            VARBINARY.writeSlice(output, Slices.wrappedBuffer(((SdkBytes) value).asByteArrayUnsafe()));
            return;
        }
        switch (type) {
            case RowType rowType -> ((RowBlockBuilder) output).buildEntry(fieldBuilders -> writeObject(rowType, (SdkPojo) value, fieldBuilders));
            case ArrayType arrayType -> ((ArrayBlockBuilder) output).buildEntry(elementBuilder -> writeList(arrayType.getElementType(), (List<?>) value, elementBuilder));
            case MapType mapType when value instanceof List<?> list -> writeMap(mapType, keyValueEntries(list), (MapBlockBuilder) output);
            case MapType mapType -> writeMap(mapType, (Map<?, ?>) value, (MapBlockBuilder) output);
            default -> throw new IllegalArgumentException("Unsupported type %s for value %s".formatted(type, value));
        }
    }

    /**
     * Writes the value of one column of a row into the output.
     */
    @FunctionalInterface
    public interface ColumnWriter
    {
        void write(SdkRow row, BlockBuilder output);
    }

    private static void writeObject(RowType rowType, SdkPojo object, List<BlockBuilder> fieldBuilders)
    {
        List<SdkField<?>> fields = mappedFields(object.sdkFields());
        for (int i = 0; i < fields.size(); i++) {
            writeValue(rowType.getFields().get(i).getType(), fieldBuilders.get(i), fields.get(i).getValueOrDefault(object));
        }
    }

    private static void writeList(Type elementType, List<?> list, BlockBuilder elementBuilder)
    {
        for (Object element : list) {
            writeValue(elementType, elementBuilder, element);
        }
    }

    private static void writeMap(MapType mapType, Map<?, ?> map, MapBlockBuilder output)
    {
        Type valueType = mapType.getValueType();
        output.buildEntry((keyBuilder, valueBuilder) -> {
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                VARCHAR.writeString(keyBuilder, (String) entry.getKey());
                writeValue(valueType, valueBuilder, entry.getValue());
            }
        });
    }

    // see SdkColumns.isKeyValueList
    private static Map<String, String> keyValueEntries(List<?> list)
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
        return entries;
    }

    // the SDK returns auto construct collections instead of null for collections missing from the response
    private static boolean isUnset(Object value)
    {
        return value == null || value instanceof SdkAutoConstructList<?> || value instanceof SdkAutoConstructMap<?, ?>;
    }
}
