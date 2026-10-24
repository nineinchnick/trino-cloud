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

import com.google.common.collect.Iterables;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.core.SdkField;
import software.amazon.awssdk.core.SdkPojo;
import software.amazon.awssdk.services.ec2.model.Instance;
import software.amazon.awssdk.services.ec2.model.NatGateway;
import software.amazon.awssdk.services.ec2.model.ReservedInstances;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TypeUtils.readNativeValue;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

final class TestSdkRowEncoder
{
    @Test
    void testUnsetStringIsNull()
    {
        Instance instance = Instance.builder()
                .instanceId("i-0123456789abcdef0")
                .build();

        assertThat(encodedValue(instance, "InstanceId")).isEqualTo(utf8Slice("i-0123456789abcdef0"));
        assertThat(encodedValue(instance, "PublicIpAddress")).isNull();
        assertThat(encodedValue(instance, "Platform")).isNull();
    }

    @Test
    void testTimestampWithMillisInUtc()
    {
        Instant createTime = Instant.ofEpochMilli(1_700_000_000_123L);
        NatGateway natGateway = NatGateway.builder()
                .natGatewayId("nat-0123456789abcdef0")
                .createTime(createTime)
                .build();

        assertThat(encodedValue(natGateway, "CreateTime")).isEqualTo(packDateTimeWithZone(createTime.toEpochMilli(), UTC_KEY));
        assertThat(unpackMillisUtc((long) encodedValue(natGateway, "CreateTime"))).isEqualTo(1_700_000_000_123L);
        assertThat(encodedValue(natGateway, "DeleteTime")).isNull();
    }

    @Test
    void testNestedObjectIsRow()
    {
        Instance instance = Instance.builder()
                .placement(placement -> placement.availabilityZone("us-east-1a").partitionNumber(3))
                .state(state -> state.code(16).name("running"))
                .build();

        assertThat(encodedValue(instance, "IamInstanceProfile")).isNull();
        assertThat(decodedRow(instance, "Placement"))
                .containsEntry("availability_zone", "us-east-1a")
                .containsEntry("partition_number", 3)
                .containsEntry("host_id", null);
        assertThat(decodedValue(instance, "State"))
                .isEqualTo(Map.of("code", 16, "name", "running"));
    }

    @Test
    void testUnsetListIsNull()
    {
        Instance instance = Instance.builder()
                .securityGroups(group -> group.groupId("sg-0123456789abcdef0"))
                .build();

        assertThat(decodedValue(instance, "SecurityGroups")).isEqualTo(List.of(mapWithNulls("group_id", "sg-0123456789abcdef0", "group_name", null)));
        assertThat(encodedValue(instance, "Tags")).isNull();
        assertThat(encodedValue(instance, "ProductCodes")).isNull();
    }

    @Test
    void testTagsAreMap()
    {
        Instance instance = Instance.builder()
                .tags(tag -> tag.key("Name").value("web"), tag -> tag.key("Empty"), tag -> tag.value("no key"))
                .build();

        assertThat(decodedValue(instance, "Tags")).isEqualTo(mapWithNulls("Name", "web", "Empty", null));
    }

    @Test
    void testUnsetNestedListIsNull()
    {
        Instance instance = Instance.builder()
                .networkInterfaces(networkInterface -> networkInterface.networkInterfaceId("eni-0123456789abcdef0"))
                .build();

        List<?> networkInterfaces = (List<?>) decodedValue(instance, "NetworkInterfaces");
        assertThat(networkInterfaces).hasSize(1);
        assertThat(asMap(networkInterfaces.getFirst()))
                .containsEntry("network_interface_id", "eni-0123456789abcdef0")
                .containsEntry("groups", null);
    }

    @Test
    void testUnsetMapIsNull()
    {
        assertThat(encodedValue(GetObjectResponse.builder().build(), "Metadata")).isNull();
        assertThat(asMap(decodedValue(GetObjectResponse.builder().metadata(Map.of("owner", "trino")).build(), "Metadata")))
                .containsExactly(entry("owner", "trino"));
    }

    @Test
    void testFloatIsEncodedAsIntBits()
    {
        ReservedInstances reservedInstances = ReservedInstances.builder()
                .fixedPrice(1.5f)
                .build();

        assertThat(encodedValue(reservedInstances, "FixedPrice")).isEqualTo((long) Float.floatToIntBits(1.5f));
        assertThat(encodedValue(reservedInstances, "UsagePrice")).isNull();
    }

    @Test
    void testIntegerIsEncodedAsLong()
    {
        Instance instance = Instance.builder()
                .amiLaunchIndex(2)
                .build();

        assertThat(encodedValue(instance, "AmiLaunchIndex")).isEqualTo(2L);
    }

    @Test
    void testBytesAndDecimals()
    {
        assertThat(encodeValue(VARBINARY, SdkBytes.fromUtf8String("bytes"))).isEqualTo(wrappedBuffer("bytes".getBytes(UTF_8)));
        assertThat(encodeValue(VARCHAR, new BigDecimal("1E+3"))).isEqualTo(utf8Slice("1000"));
    }

    @Test
    void testObjectsNestedTooDeepAreStrings()
    {
        RecursiveShape shape = RecursiveShape.chain(SdkColumns.MAX_NESTING_DEPTH + 2);
        Object decoded = decode(SdkColumns.type(RecursiveShape.CHILD), encodeValue(SdkColumns.type(RecursiveShape.CHILD), shape));

        for (int depth = 1; depth < SdkColumns.MAX_NESTING_DEPTH; depth++) {
            assertThat(decoded).isInstanceOf(Map.class);
            decoded = ((Map<?, ?>) decoded).get("child");
        }
        assertThat(((Map<?, ?>) decoded).get("name")).isEqualTo("level " + (SdkColumns.MAX_NESTING_DEPTH - 1));
        assertThat(((Map<?, ?>) decoded).get("child")).isEqualTo(shape.descendant(SdkColumns.MAX_NESTING_DEPTH).toString());
    }

    private static Object encodedValue(SdkPojo object, String memberName)
    {
        SdkField<?> field = SdkColumns.mappedFields(object).get(fieldIndex(object, memberName));
        Type type = SdkColumns.type(field);
        BlockBuilder output = type.createBlockBuilder(null, 1);
        SdkRowEncoder.fieldWriter(field).write(new SdkRow(object), output);
        return readNativeValue(type, output.build(), 0);
    }

    private static Object encodeValue(Type type, Object value)
    {
        BlockBuilder output = type.createBlockBuilder(null, 1);
        SdkRowEncoder.writeValue(type, output, value);
        return readNativeValue(type, output.build(), 0);
    }

    private static Object decodedValue(SdkPojo object, String memberName)
    {
        SdkField<?> field = SdkColumns.mappedFields(object).get(fieldIndex(object, memberName));
        return decode(SdkColumns.type(field), encodedValue(object, memberName));
    }

    private static Map<String, Object> decodedRow(SdkPojo object, String memberName)
    {
        return asMap(decodedValue(object, memberName));
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> asMap(Object value)
    {
        return (Map<String, Object>) value;
    }

    private static int fieldIndex(SdkPojo object, String memberName)
    {
        int index = Iterables.indexOf(SdkColumns.mappedFields(object), field -> field.memberName().equals(memberName));
        checkArgument(index >= 0, "No field named %s", memberName);
        return index;
    }

    // rows are decoded to maps of field names to values, to make assertions readable
    private static Object decode(Type type, Object encodedValue)
    {
        Object value = type.getObjectValue(writeNativeValue(type, encodedValue), 0);
        return withNamedFields(type, value);
    }

    private static Object withNamedFields(Type type, Object value)
    {
        if (value == null) {
            return null;
        }
        return switch (type) {
            case RowType rowType -> {
                List<?> fields = (List<?>) value;
                Map<String, Object> namedFields = new HashMap<>();
                for (int i = 0; i < fields.size(); i++) {
                    RowType.Field field = rowType.getFields().get(i);
                    namedFields.put(field.getName().orElseThrow(), withNamedFields(field.getType(), fields.get(i)));
                }
                yield namedFields;
            }
            case ArrayType arrayType -> ((List<?>) value).stream()
                    .map(element -> withNamedFields(arrayType.getElementType(), element))
                    .toList();
            default -> value;
        };
    }

    private static Map<String, Object> mapWithNulls(String key1, Object value1, String key2, Object value2)
    {
        Map<String, Object> map = new HashMap<>();
        map.put(key1, value1);
        map.put(key2, value2);
        return map;
    }
}
