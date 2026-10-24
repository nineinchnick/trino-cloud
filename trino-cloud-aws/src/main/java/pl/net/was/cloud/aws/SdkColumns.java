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
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import software.amazon.awssdk.core.SdkField;
import software.amazon.awssdk.core.protocol.MarshallingType;
import software.amazon.awssdk.core.traits.ListTrait;
import software.amazon.awssdk.core.traits.MapTrait;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Locale.ENGLISH;
import static java.util.stream.Collectors.joining;

final class SdkColumns
{
    // nested objects deeper than this are mapped to VARCHAR, so recursive shapes don't produce infinite types
    static final int MAX_NESTING_DEPTH = 8;

    // all types must be handled in SdkRowEncoder.encodeValue()
    // SdkPojo, List<?> and Map<String, ?> are handled separately
    private static final Map<MarshallingType<?>, Type> TYPE_MAP = ImmutableMap.<MarshallingType<?>, Type>builder()
            .put(MarshallingType.BYTE, TINYINT)
            .put(MarshallingType.SHORT, SMALLINT)
            .put(MarshallingType.INTEGER, INTEGER)
            .put(MarshallingType.LONG, BIGINT)
            .put(MarshallingType.FLOAT, REAL)
            .put(MarshallingType.DOUBLE, DOUBLE)
            .put(MarshallingType.BOOLEAN, BOOLEAN)
            .put(MarshallingType.STRING, VARCHAR)
            .put(MarshallingType.INSTANT, TIMESTAMP_TZ_MILLIS)
            .put(MarshallingType.SDK_BYTES, VARBINARY)
            // BigDecimal values can have any precision and scale, so they're mapped to a plain string, and documents to JSON text
            .put(MarshallingType.BIG_DECIMAL, VARCHAR)
            .put(MarshallingType.DOCUMENT, VARCHAR)
            .buildOrThrow();

    // mixed-case words that splitting on case changes would break apart, like VCpu in VCpuInfo
    private static final Map<String, String> WORDS = ImmutableMap.of(
            "VCpu", "Vcpu",
            "MiB", "Mib",
            "MBps", "Mbps",
            "ETag", "Etag");
    // a word ends before an upper case letter that follows a lower case letter or a digit, or before the last letter of an acronym followed by a lower case letter
    private static final Pattern WORD_BOUNDARY = Pattern.compile("(?<=[a-z0-9])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])");

    private static final TypeOperators TYPE_OPERATORS = new TypeOperators();
    private static final MapType KEY_VALUE_MAP_TYPE = new MapType(VARCHAR, VARCHAR, TYPE_OPERATORS);
    // SdkField doesn't override equals, but every field is a constant of an SDK model class, so there's a bounded number of them
    private static final Map<SdkField<?>, Type> FIELD_TYPES = new ConcurrentHashMap<>();

    private SdkColumns() {}

    public static List<ColumnMetadata> columns(List<SdkField<?>> sdkFields)
    {
        return columns(List.of(), sdkFields);
    }

    public static List<ColumnMetadata> columns(List<ColumnMetadata> extraColumns, List<SdkField<?>> sdkFields)
    {
        List<ColumnMetadata> columns = ImmutableList.<ColumnMetadata>builder()
                .addAll(extraColumns)
                .addAll(mappedFields(sdkFields).stream()
                        .map(field -> new ColumnMetadata(columnName(field), type(field)))
                        .toList())
                .build();
        checkUniqueNames(columns.stream().map(ColumnMetadata::getName).toList());
        return columns;
    }

    public static String columnName(SdkField<?> field)
    {
        String name = field.memberName();
        for (Map.Entry<String, String> word : WORDS.entrySet()) {
            name = name.replace(word.getKey(), word.getValue());
        }
        return WORD_BOUNDARY.splitAsStream(name)
                .map(part -> part.toLowerCase(ENGLISH))
                .collect(joining("_"));
    }

    /**
     * Fields that are mapped to columns or row fields, in the same order as the columns or row fields.
     */
    public static List<SdkField<?>> mappedFields(List<SdkField<?>> sdkFields)
    {
        return sdkFields.stream()
                .filter(field -> field.marshallingType() != MarshallingType.NULL)
                .collect(toImmutableList());
    }

    public static Type type(SdkField<?> field)
    {
        return FIELD_TYPES.computeIfAbsent(field, key -> type(key, 0));
    }

    /**
     * Lists of objects with only a Key and a Value, like tags, are mapped to maps, so they can be accessed like {@code tags['Name']}.
     */
    public static boolean isKeyValueList(SdkField<?> field)
    {
        return listMember(field)
                .filter(member -> member.marshallingType() == MarshallingType.SDK_POJO)
                .map(member -> member.constructor().get().sdkFields())
                .filter(fields -> fields.size() == 2
                        && fields.stream().allMatch(memberField -> memberField.marshallingType() == MarshallingType.STRING)
                        && fields.get(0).memberName().equals("Key")
                        && fields.get(1).memberName().equals("Value"))
                .isPresent();
    }

    private static Type type(SdkField<?> field, int depth)
    {
        MarshallingType<?> sdkType = field.marshallingType();
        if (sdkType == MarshallingType.SDK_POJO) {
            if (depth >= MAX_NESTING_DEPTH) {
                return VARCHAR;
            }
            return rowType(mappedFields(field.constructor().get().sdkFields()), depth + 1);
        }
        if (sdkType == MarshallingType.LIST) {
            if (isKeyValueList(field)) {
                return KEY_VALUE_MAP_TYPE;
            }
            return new ArrayType(listMember(field)
                    .map(member -> type(member, depth))
                    .orElse(VARCHAR));
        }
        if (sdkType == MarshallingType.MAP) {
            Type valueType = field.getOptionalTrait(MapTrait.class)
                    .map(trait -> type(trait.valueFieldInfo(), depth))
                    .orElse(VARCHAR);
            return new MapType(VARCHAR, valueType, TYPE_OPERATORS);
        }
        Type type = TYPE_MAP.get(sdkType);
        checkArgument(type != null, "Unsupported type %s of field %s", sdkType, field.memberName());
        return type;
    }

    private static RowType rowType(List<SdkField<?>> fields, int depth)
    {
        List<RowType.Field> rowFields = fields.stream()
                .map(field -> RowType.field(columnName(field), type(field, depth)))
                .collect(toImmutableList());
        checkUniqueNames(rowFields.stream().map(field -> field.getName().orElseThrow()).toList());
        return RowType.from(rowFields);
    }

    private static Optional<SdkField<?>> listMember(SdkField<?> field)
    {
        if (field.marshallingType() != MarshallingType.LIST) {
            return Optional.empty();
        }
        return field.getOptionalTrait(ListTrait.class)
                .map(trait -> (SdkField<?>) trait.memberFieldInfo());
    }

    private static void checkUniqueNames(List<String> names)
    {
        Set<String> uniqueNames = new HashSet<>();
        for (String name : names) {
            checkArgument(uniqueNames.add(name), "More than one field is mapped to %s in %s, add the word that is split incorrectly to SdkColumns.WORDS", name, names);
        }
    }
}
