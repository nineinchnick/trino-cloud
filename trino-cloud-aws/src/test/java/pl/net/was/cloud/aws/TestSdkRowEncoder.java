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
import io.trino.spi.block.Block;
import io.trino.spi.block.MapBlockBuilder;
import io.trino.spi.type.MapType;
import io.trino.spi.type.TypeOperators;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.SdkPojo;
import software.amazon.awssdk.services.ec2.model.Instance;
import software.amazon.awssdk.services.ec2.model.NatGateway;
import software.amazon.awssdk.services.ec2.model.ReservedInstances;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_SECOND;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

final class TestSdkRowEncoder
{
    private static final MapType MAP_TYPE = new MapType(VARCHAR, VARCHAR, new TypeOperators());

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
    void testUnsetTimestampIsNull()
    {
        NatGateway natGateway = NatGateway.builder()
                .natGatewayId("nat-0123456789abcdef0")
                .createTime(Instant.ofEpochSecond(1_700_000_000))
                .build();

        assertThat(encodedValue(natGateway, "CreateTime")).isEqualTo(1_700_000_000L * MICROSECONDS_PER_SECOND);
        assertThat(encodedValue(natGateway, "DeleteTime")).isNull();
    }

    @Test
    void testUnsetNestedObjectIsNull()
    {
        Instance instance = Instance.builder()
                .placement(placement -> placement.availabilityZone("us-east-1a"))
                .build();

        assertThat(encodedValue(instance, "IamInstanceProfile")).isNull();
        assertThat(decodeMap(encodedValue(instance, "Placement")))
                .containsEntry("AvailabilityZone", "us-east-1a")
                .containsEntry("HostId", null);
    }

    @Test
    void testUnsetListIsNull()
    {
        Instance instance = Instance.builder()
                .securityGroups(group -> group.groupId("sg-0123456789abcdef0"))
                .build();

        Block securityGroups = (Block) encodedValue(instance, "SecurityGroups");
        assertThat(securityGroups.getPositionCount()).isEqualTo(1);
        assertThat(encodedValue(instance, "Tags")).isNull();
        assertThat(encodedValue(instance, "ProductCodes")).isNull();
    }

    @Test
    void testUnsetNestedListIsNull()
    {
        Instance instance = Instance.builder()
                .networkInterfaces(networkInterface -> networkInterface.networkInterfaceId("eni-0123456789abcdef0"))
                .build();

        Block networkInterfaces = (Block) encodedValue(instance, "NetworkInterfaces");
        assertThat(networkInterfaces.getPositionCount()).isEqualTo(1);
        assertThat(decodeMap(MAP_TYPE.getObject(networkInterfaces, 0)))
                .containsEntry("NetworkInterfaceId", "eni-0123456789abcdef0")
                .containsEntry("Groups", null);
    }

    @Test
    void testUnsetMapIsNull()
    {
        assertThat(encodedValue(GetObjectResponse.builder().build(), "Metadata")).isNull();
        assertThat(decodeMap(encodedValue(GetObjectResponse.builder().metadata(Map.of("owner", "trino")).build(), "Metadata")))
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

    private static Object encodedValue(SdkPojo object, String memberName)
    {
        int index = Iterables.indexOf(object.sdkFields(), field -> field.memberName().equals(memberName));
        checkArgument(index >= 0, "No field named %s", memberName);
        return SdkRowEncoder.encodeRow(object, List.of()).get(index);
    }

    private static Map<Object, Object> decodeMap(Object encodedValue)
    {
        MapBlockBuilder blockBuilder = MAP_TYPE.createBlockBuilder(null, 1);
        MAP_TYPE.writeObject(blockBuilder, encodedValue);
        return (Map<Object, Object>) MAP_TYPE.getObjectValue(blockBuilder.build(), 0);
    }
}
