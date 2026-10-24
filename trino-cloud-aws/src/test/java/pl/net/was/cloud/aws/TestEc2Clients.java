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

import io.opentelemetry.api.OpenTelemetry;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;

import static org.assertj.core.api.Assertions.assertThat;

// clients are only created, so they never send any requests
final class TestEc2Clients
{
    @Test
    void testClientPerRegion()
    {
        try (AwsClientConfigurer clientConfigurer = new AwsClientConfigurer(config(), OpenTelemetry.noop());
                Ec2Clients clients = new Ec2Clients(clientConfigurer)) {
            Ec2Client defaultClient = clients.defaultClient();
            Ec2Client regionalClient = clients.client("eu-west-1");

            assertThat(clients.defaultRegion()).isEqualTo("us-east-1");
            assertThat(defaultClient.serviceClientConfiguration().region()).isEqualTo(Region.US_EAST_1);
            assertThat(regionalClient.serviceClientConfiguration().region()).isEqualTo(Region.EU_WEST_1);
            assertThat(clients.client("us-east-1")).isSameAs(defaultClient);
            assertThat(clients.client("eu-west-1")).isSameAs(regionalClient);
        }
    }

    private static AwsConfig config()
    {
        return new AwsConfig()
                .setRegion("us-east-1")
                .setAccessKey("access-key")
                .setSecretKey("secret-key");
    }
}
