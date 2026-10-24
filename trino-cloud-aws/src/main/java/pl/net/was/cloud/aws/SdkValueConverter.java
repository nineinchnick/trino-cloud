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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.spi.TrinoException;
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
import software.amazon.awssdk.core.protocol.MarshallingType;
import software.amazon.awssdk.core.traits.ListTrait;
import software.amazon.awssdk.core.traits.MapTrait;
import software.amazon.awssdk.utils.builder.SdkBuilder;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.spi.StandardErrorCode.INVALID_ARGUMENTS;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.Math.toIntExact;
import static java.util.function.Function.identity;
import static pl.net.was.cloud.aws.SdkColumns.columnName;

/**
 * Converts Trino values back to the values of SDK fields, the reverse of {@link SdkRowEncoder}.
 * The Trino type comes from the column, so it's the type of a field of a response model,
 * which can be different from the type of the request field with the same name.
 */
final class SdkValueConverter
{
    private static final Set<Type> INTEGRAL_TYPES = Set.of(TINYINT, SMALLINT, INTEGER, BIGINT);

    private SdkValueConverter() {}

    public static Object toSdkValue(SdkField<?> field, Type type, Block block, int position)
    {
        return toSdkValue(columnName(field), field, type, block, position);
    }

    // list elements and map values don't have a member name, so they're reported using the name of the list or map
    private static Object toSdkValue(String name, SdkField<?> field, Type type, Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }
        MarshallingType<?> sdkType = field.marshallingType();
        if (sdkType == MarshallingType.STRING && type.equals(VARCHAR)) {
            return VARCHAR.getSlice(block, position).toStringUtf8();
        }
        if (sdkType == MarshallingType.BIG_DECIMAL && type.equals(VARCHAR)) {
            return new BigDecimal(VARCHAR.getSlice(block, position).toStringUtf8());
        }
        if (INTEGRAL_TYPES.contains(type) && isIntegral(sdkType)) {
            return toIntegral(name, field, type.getLong(block, position));
        }
        if (sdkType == MarshallingType.FLOAT && type.equals(REAL)) {
            return REAL.getFloat(block, position);
        }
        if (sdkType == MarshallingType.DOUBLE && type.equals(DOUBLE)) {
            return DOUBLE.getDouble(block, position);
        }
        if (sdkType == MarshallingType.DOUBLE && type.equals(REAL)) {
            return (double) REAL.getFloat(block, position);
        }
        if (sdkType == MarshallingType.BOOLEAN && type.equals(BOOLEAN)) {
            return BOOLEAN.getBoolean(block, position);
        }
        if (sdkType == MarshallingType.INSTANT && type.equals(TIMESTAMP_TZ_MILLIS)) {
            return Instant.ofEpochMilli(unpackMillisUtc(TIMESTAMP_TZ_MILLIS.getLong(block, position)));
        }
        if (sdkType == MarshallingType.SDK_BYTES && type.equals(VARBINARY)) {
            return SdkBytes.fromByteArray(VARBINARY.getSlice(block, position).getBytes());
        }
        if (sdkType == MarshallingType.SDK_POJO && type instanceof RowType rowType) {
            return toObject(name, field, rowType, rowType.getObject(block, position));
        }
        if (sdkType == MarshallingType.LIST && type instanceof ArrayType arrayType) {
            return toList(name, field, arrayType, arrayType.getObject(block, position));
        }
        if (sdkType == MarshallingType.LIST && type instanceof MapType mapType && SdkColumns.isKeyValueList(field)) {
            return toKeyValueList(field, mapType.getObject(block, position));
        }
        if (sdkType == MarshallingType.MAP && type instanceof MapType mapType) {
            return toMap(name, field, mapType, mapType.getObject(block, position));
        }
        throw new TrinoException(NOT_SUPPORTED, "Values of type %s can't be set in %s, which expects a %s".formatted(type, name, sdkType));
    }

    private static boolean isIntegral(MarshallingType<?> sdkType)
    {
        return sdkType == MarshallingType.BYTE
                || sdkType == MarshallingType.SHORT
                || sdkType == MarshallingType.INTEGER
                || sdkType == MarshallingType.LONG;
    }

    private static Object toIntegral(String name, SdkField<?> field, long value)
    {
        MarshallingType<?> sdkType = field.marshallingType();
        if (sdkType == MarshallingType.LONG) {
            return value;
        }
        try {
            int intValue = toIntExact(value);
            if (sdkType == MarshallingType.INTEGER) {
                return intValue;
            }
            if (sdkType == MarshallingType.SHORT && intValue == (short) intValue) {
                return (short) intValue;
            }
            if (sdkType == MarshallingType.BYTE && intValue == (byte) intValue) {
                return (byte) intValue;
            }
        }
        catch (ArithmeticException _) {
            // reported below
        }
        throw new TrinoException(INVALID_ARGUMENTS, "Value %s is out of range for %s".formatted(value, name));
    }

    // only fields that are not NULL are set, so the request doesn't override defaults with NULL values
    private static Object toObject(String name, SdkField<?> field, RowType rowType, SqlRow row)
    {
        SdkPojo builder = field.constructor().get();
        Map<String, SdkField<?>> targetFields = builder.sdkFields().stream()
                .collect(toImmutableMap(SdkColumns::columnName, identity()));
        for (int i = 0; i < rowType.getFields().size(); i++) {
            RowType.Field rowField = rowType.getFields().get(i);
            Block fieldBlock = row.getRawFieldBlock(i);
            if (fieldBlock.isNull(row.getRawIndex())) {
                continue;
            }
            String fieldName = rowField.getName().orElseThrow();
            SdkField<?> targetField = targetFields.get(fieldName);
            if (targetField == null) {
                throw new TrinoException(NOT_SUPPORTED, "Field %s can't be set in %s, the supported fields are: %s".formatted(fieldName, name, String.join(", ", targetFields.keySet())));
            }
            targetField.set(builder, toSdkValue(targetField, rowField.getType(), fieldBlock, row.getRawIndex()));
        }
        return ((SdkBuilder<?, ?>) builder).build();
    }

    private static List<?> toList(String name, SdkField<?> field, ArrayType arrayType, Block elements)
    {
        SdkField<?> memberField = field.getRequiredTrait(ListTrait.class).memberFieldInfo();
        // ImmutableList doesn't allow NULL elements, and neither does the SDK when marshalling requests
        ImmutableList.Builder<Object> values = ImmutableList.builderWithExpectedSize(elements.getPositionCount());
        for (int i = 0; i < elements.getPositionCount(); i++) {
            if (elements.isNull(i)) {
                throw new TrinoException(INVALID_ARGUMENTS, "%s can't have NULL elements".formatted(name));
            }
            values.add(toSdkValue(name, memberField, arrayType.getElementType(), elements, i));
        }
        return values.build();
    }

    private static Map<String, ?> toMap(String name, SdkField<?> field, MapType mapType, SqlMap map)
    {
        SdkField<?> valueField = field.getRequiredTrait(MapTrait.class).valueFieldInfo();
        Block keys = map.getRawKeyBlock();
        Block values = map.getRawValueBlock();
        ImmutableMap.Builder<String, Object> entries = ImmutableMap.builderWithExpectedSize(map.getSize());
        for (int i = 0; i < map.getSize(); i++) {
            int position = map.getRawOffset() + i;
            if (values.isNull(position)) {
                throw new TrinoException(INVALID_ARGUMENTS, "%s can't have NULL values".formatted(name));
            }
            entries.put(VARCHAR.getSlice(keys, position).toStringUtf8(), toSdkValue(name, valueField, mapType.getValueType(), values, position));
        }
        return entries.buildOrThrow();
    }

    // see SdkColumns.isKeyValueList
    private static List<?> toKeyValueList(SdkField<?> field, SqlMap map)
    {
        SdkField<?> memberField = field.getRequiredTrait(ListTrait.class).memberFieldInfo();
        Block keys = map.getRawKeyBlock();
        Block values = map.getRawValueBlock();
        ImmutableList.Builder<Object> entries = ImmutableList.builderWithExpectedSize(map.getSize());
        for (int i = 0; i < map.getSize(); i++) {
            int position = map.getRawOffset() + i;
            SdkPojo entry = memberField.constructor().get();
            List<SdkField<?>> entryFields = entry.sdkFields();
            entryFields.get(0).set(entry, VARCHAR.getSlice(keys, position).toStringUtf8());
            if (!values.isNull(position)) {
                entryFields.get(1).set(entry, VARCHAR.getSlice(values, position).toStringUtf8());
            }
            entries.add(((SdkBuilder<?, ?>) entry).build());
        }
        return entries.build();
    }
}
