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

import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.SdkField;
import software.amazon.awssdk.core.SdkPojo;
import software.amazon.awssdk.services.ec2.model.GroupIdentifier;
import software.amazon.awssdk.services.ec2.model.Instance;
import software.amazon.awssdk.services.ec2.model.Placement;
import software.amazon.awssdk.services.ec2.model.ReservedInstances;
import software.amazon.awssdk.services.ec2.model.RunInstancesRequest;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.ec2.model.TagSpecification;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static io.trino.spi.StandardErrorCode.INVALID_ARGUMENTS;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.block.RowValueBuilder.buildRowValue;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestSdkValueConverter
{
    @Test
    void testRowToObject()
    {
        Placement placement = Placement.builder()
                .availabilityZone("us-east-1a")
                .partitionNumber(3)
                .build();
        Block block = encode(field(Instance.builder(), "Placement"), placement);

        Object converted = SdkValueConverter.toSdkValue(field(RunInstancesRequest.builder(), "Placement"), type(Instance.builder(), "Placement"), block, 0);

        assertThat(converted).isEqualTo(placement);
    }

    @Test
    void testRowFieldsWithoutRequestFields()
    {
        RowType rowType = RowType.from(List.of(RowType.field("availability_zone", VARCHAR), RowType.field("unknown", VARCHAR)));
        assertThat(SdkValueConverter.toSdkValue(field(RunInstancesRequest.builder(), "Placement"), rowType, writeNativeValue(rowType, null), 0)).isNull();

        Block row = writeNativeValue(rowType, buildRowValue(rowType, fields -> {
            VARCHAR.writeString(fields.get(0), "us-east-1a");
            VARCHAR.writeString(fields.get(1), "value");
        }));
        assertThatThrownBy(() -> SdkValueConverter.toSdkValue(field(RunInstancesRequest.builder(), "Placement"), rowType, row, 0))
                .isInstanceOfSatisfying(TrinoException.class, e -> assertThat(e.getErrorCode()).isEqualTo(NOT_SUPPORTED.toErrorCode()))
                .hasMessageStartingWith("Field unknown can't be set in placement, the supported fields are: ");
    }

    @Test
    void testTagsMapToList()
    {
        List<Tag> tags = List.of(Tag.builder().key("Name").value("web").build(), Tag.builder().key("Empty").build());
        Block block = encode(field(Instance.builder(), "Tags"), tags);

        Object converted = SdkValueConverter.toSdkValue(field(TagSpecification.builder(), "Tags"), type(Instance.builder(), "Tags"), block, 0);

        assertThat(converted).isEqualTo(tags);
    }

    @Test
    void testArrayToList()
    {
        ArrayType arrayType = new ArrayType(VARCHAR);
        Block block = writeNativeValue(arrayType, SdkRowEncoder.encodeValue(arrayType, List.of("sg-1", "sg-2")));

        assertThat(SdkValueConverter.toSdkValue(field(RunInstancesRequest.builder(), "SecurityGroupIds"), arrayType, block, 0))
                .isEqualTo(List.of("sg-1", "sg-2"));
    }

    @Test
    void testMapToMap()
    {
        Block block = encode(field(GetObjectResponse.builder(), "Metadata"), Map.of("owner", "trino"));

        assertThat(SdkValueConverter.toSdkValue(field(PutObjectRequest.builder(), "Metadata"), type(GetObjectResponse.builder(), "Metadata"), block, 0))
                .isEqualTo(Map.of("owner", "trino"));
    }

    @Test
    void testIntegerWidths()
    {
        SdkField<?> maxCount = field(RunInstancesRequest.builder(), "MaxCount");

        assertThat(SdkValueConverter.toSdkValue(maxCount, BIGINT, bigint(5), 0)).isEqualTo(5);
        assertThatThrownBy(() -> SdkValueConverter.toSdkValue(maxCount, BIGINT, bigint(Integer.MAX_VALUE + 1L), 0))
                .isInstanceOfSatisfying(TrinoException.class, e -> assertThat(e.getErrorCode()).isEqualTo(INVALID_ARGUMENTS.toErrorCode()))
                .hasMessage("Value 2147483648 is out of range for max_count");
    }

    @Test
    void testScalarRoundTrips()
    {
        assertRoundTrip(Instance.builder(), "LaunchTime", Instant.ofEpochMilli(1_700_000_000_123L));
        assertRoundTrip(Instance.builder(), "EbsOptimized", true);
        assertRoundTrip(Instance.builder(), "AmiLaunchIndex", 7);
        assertRoundTrip(ReservedInstances.builder(), "FixedPrice", 1.5f);
    }

    @Test
    void testIncompatibleTypes()
    {
        assertThatThrownBy(() -> SdkValueConverter.toSdkValue(
                field(RunInstancesRequest.builder(), "SecurityGroups"),
                type(Instance.builder(), "SecurityGroups"),
                encode(field(Instance.builder(), "SecurityGroups"), List.of(GroupIdentifier.builder().groupId("sg-1").build())),
                0))
                .isInstanceOfSatisfying(TrinoException.class, e -> assertThat(e.getErrorCode()).isEqualTo(NOT_SUPPORTED.toErrorCode()))
                .hasMessage("Values of type row(\"group_id\" varchar, \"group_name\" varchar) can't be set in security_groups, which expects a String");
    }

    private static void assertRoundTrip(Object builder, String memberName, Object value)
    {
        SdkField<?> field = field(builder, memberName);
        assertThat(SdkValueConverter.toSdkValue(field, SdkColumns.type(field), encode(field, value), 0)).isEqualTo(value);
    }

    private static Block encode(SdkField<?> field, Object value)
    {
        Type type = SdkColumns.type(field);
        return writeNativeValue(type, SdkRowEncoder.encodeValue(type, value));
    }

    private static Block bigint(long value)
    {
        return writeNativeValue(BIGINT, value);
    }

    private static Type type(Object builder, String memberName)
    {
        return SdkColumns.type(field(builder, memberName));
    }

    private static SdkField<?> field(Object builder, String memberName)
    {
        return ((SdkPojo) builder).sdkFields().stream()
                .filter(field -> field.memberName().equals(memberName))
                .findFirst()
                .orElseThrow();
    }
}
