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

import com.google.common.base.CaseFormat;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import software.amazon.awssdk.core.SdkField;
import software.amazon.awssdk.core.protocol.MarshallingType;
import software.amazon.awssdk.core.traits.ListTrait;

import java.util.List;
import java.util.Map;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimestampType.TIMESTAMP_SECONDS;
import static io.trino.spi.type.VarcharType.VARCHAR;

final class SdkColumns
{
    // all types must be handled in SdkRowEncoder.encodeField()
    // missing: Void, BigDecimal, SdkBytes, Document
    // SdkPojo, List<?> and Map<String, ?> handled separately
    private static final Map<MarshallingType<?>, Type> TYPE_MAP = ImmutableMap.<MarshallingType<?>, Type>builder()
            .put(MarshallingType.SHORT, SMALLINT)
            .put(MarshallingType.INTEGER, INTEGER)
            .put(MarshallingType.LONG, BIGINT)
            .put(MarshallingType.FLOAT, REAL)
            .put(MarshallingType.DOUBLE, DOUBLE)
            .put(MarshallingType.BOOLEAN, BOOLEAN)
            .put(MarshallingType.STRING, VARCHAR)
            .put(MarshallingType.INSTANT, TIMESTAMP_SECONDS)
            .put(MarshallingType.LIST, new ArrayType(VARCHAR))
            .buildOrThrow();

    private SdkColumns() {}

    public static List<ColumnMetadata> columns(List<SdkField<?>> sdkFields)
    {
        return columns(List.of(), sdkFields);
    }

    public static List<ColumnMetadata> columns(List<ColumnMetadata> extraColumns, List<SdkField<?>> sdkFields)
    {
        return ImmutableList.<ColumnMetadata>builder()
                .addAll(extraColumns)
                .addAll(sdkFields.stream()
                        .map(SdkColumns::column)
                        .toList())
                .build();
    }

    private static ColumnMetadata column(SdkField<?> field)
    {
        MarshallingType<?> sdkType = field.marshallingType();
        Type trinoType = TYPE_MAP.getOrDefault(sdkType, VARCHAR);
        if (sdkType == MarshallingType.SDK_POJO || sdkType == MarshallingType.MAP) {
            trinoType = new MapType(VARCHAR, VARCHAR, new TypeOperators());
        }

        if (sdkType == MarshallingType.LIST &&
                field.containsTrait(ListTrait.class) &&
                field.getTrait(ListTrait.class).memberFieldInfo().marshallingType() == MarshallingType.SDK_POJO) {
            trinoType = new ArrayType(new MapType(VARCHAR, VARCHAR, new TypeOperators()));
        }

        String name = CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, field.memberName());
        return new ColumnMetadata(name, trinoType);
    }
}
