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
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.SdkPojo;
import software.amazon.awssdk.services.ec2.model.Instance;
import software.amazon.awssdk.services.ec2.model.NatGateway;

import java.time.Instant;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_SECOND;
import static org.assertj.core.api.Assertions.assertThat;

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
    void testUnsetTimestampIsNull()
    {
        NatGateway natGateway = NatGateway.builder()
                .natGatewayId("nat-0123456789abcdef0")
                .createTime(Instant.ofEpochSecond(1_700_000_000))
                .build();

        assertThat(encodedValue(natGateway, "CreateTime")).isEqualTo(1_700_000_000L * MICROSECONDS_PER_SECOND);
        assertThat(encodedValue(natGateway, "DeleteTime")).isNull();
    }

    private static Object encodedValue(SdkPojo object, String memberName)
    {
        int index = Iterables.indexOf(object.sdkFields(), field -> field.memberName().equals(memberName));
        checkArgument(index >= 0, "No field named %s", memberName);
        return SdkRowEncoder.encodeRow(object, List.of()).get(index);
    }
}
