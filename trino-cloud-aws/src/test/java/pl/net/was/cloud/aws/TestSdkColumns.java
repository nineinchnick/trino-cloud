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

import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.ec2.model.Instance;
import software.amazon.awssdk.services.ec2.model.InstanceTypeInfo;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.util.List;

import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestSdkColumns
{
    private static final TypeOperators TYPE_OPERATORS = new TypeOperators();

    @Test
    void testScalarTypes()
    {
        List<ColumnMetadata> columns = SdkColumns.columns(Instance.builder());

        assertThat(columnType(columns, "instance_id")).isEqualTo(VARCHAR);
        assertThat(columnType(columns, "ami_launch_index")).isEqualTo(INTEGER);
        assertThat(columnType(columns, "launch_time")).isEqualTo(TIMESTAMP_TZ_MILLIS);
    }

    @Test
    void testNestedObjectsAreRows()
    {
        List<ColumnMetadata> columns = SdkColumns.columns(Instance.builder());

        assertThat(columnType(columns, "state")).isEqualTo(RowType.from(List.of(
                RowType.field("code", INTEGER),
                RowType.field("name", VARCHAR))));
        assertThat(columnType(columns, "security_groups")).isEqualTo(new ArrayType(RowType.from(List.of(
                RowType.field("group_id", VARCHAR),
                RowType.field("group_name", VARCHAR)))));
        RowType placement = (RowType) columnType(columns, "placement");
        assertThat(placement.getFields())
                .extracting(field -> field.getName().orElseThrow())
                .contains("availability_zone", "host_id", "partition_number");
    }

    @Test
    void testTagsAreMaps()
    {
        assertThat(columnType(SdkColumns.columns(Instance.builder()), "tags"))
                .isEqualTo(new MapType(VARCHAR, VARCHAR, TYPE_OPERATORS));
    }

    @Test
    void testTypedMapValues()
    {
        assertThat(columnType(SdkColumns.columns(GetObjectResponse.builder()), "metadata"))
                .isEqualTo(new MapType(VARCHAR, VARCHAR, TYPE_OPERATORS));
    }

    @Test
    void testColumnNames()
    {
        List<ColumnMetadata> instanceTypeColumns = SdkColumns.columns(InstanceTypeInfo.builder());
        assertThat(rowFieldNames(instanceTypeColumns, "vcpu_info")).contains("default_vcpus");
        assertThat(rowFieldNames(instanceTypeColumns, "memory_info")).contains("size_in_mib");
        assertThat(rowFieldNames(instanceTypeColumns, "instance_storage_info")).contains("total_size_in_gb");

        List<ColumnMetadata> objectColumns = SdkColumns.columns(S3Object.builder());
        assertThat(columnType(objectColumns, "etag")).isEqualTo(VARCHAR);
        assertThat(columnType(objectColumns, "owner")).isEqualTo(RowType.from(List.of(
                RowType.field("display_name", VARCHAR),
                RowType.field("id", VARCHAR))));
    }

    @Test
    void testDuplicateColumnNames()
    {
        assertThatThrownBy(() -> SdkTableColumns.builder(Instance.builder()).prepend(new ColumnMetadata("instance_id", VARCHAR), _ -> null).build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageStartingWith("More than one field is mapped to instance_id in [instance_id, ");
    }

    @Test
    void testNestingDepthLimit()
    {
        Type type = SdkColumns.type(RecursiveShape.CHILD);
        for (int depth = 0; depth < SdkColumns.MAX_NESTING_DEPTH; depth++) {
            assertThat(type).isInstanceOf(RowType.class);
            type = ((RowType) type).getFields().get(1).getType();
        }
        assertThat(type).isEqualTo(VARCHAR);
    }

    private static Type columnType(List<ColumnMetadata> columns, String name)
    {
        return columns.stream()
                .filter(column -> column.getName().equals(name))
                .map(ColumnMetadata::getType)
                .findFirst()
                .orElseThrow(() -> new AssertionError("No column named " + name));
    }

    private static List<String> rowFieldNames(List<ColumnMetadata> columns, String columnName)
    {
        return ((RowType) columnType(columns, columnName)).getFields().stream()
                .map(field -> field.getName().orElseThrow())
                .toList();
    }
}
