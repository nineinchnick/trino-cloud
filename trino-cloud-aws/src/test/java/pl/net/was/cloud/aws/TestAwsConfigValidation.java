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

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;

import java.net.URI;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestAwsConfigValidation
{
    @Test
    void testConfiguredRegion()
    {
        AwsClientConfigurer configurer = new AwsClientConfigurer(new AwsConfig().setRegion("eu-central-1"));

        try (Ec2Client client = configurer.configure(Ec2Client.builder()).build()) {
            assertThat(client.serviceClientConfiguration().region()).isEqualTo(Region.EU_CENTRAL_1);
        }
    }

    @Test
    void testEndpointOverride()
    {
        AwsClientConfigurer configurer = new AwsClientConfigurer(new AwsConfig()
                .setRegion("us-east-1")
                .setEndpoint(URI.create("http://localhost:4566")));

        try (Ec2Client client = configurer.configure(Ec2Client.builder()).build()) {
            assertThat(client.serviceClientConfiguration().endpointOverride()).contains(URI.create("http://localhost:4566"));
        }
    }

    @Test
    void testUnknownRegion()
    {
        assertThatThrownBy(() -> new AwsClientConfigurer(new AwsConfig().setRegion("eu-centrall-1")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageStartingWith("Unknown AWS region 'eu-centrall-1' in aws.region, expected one of: ")
                .hasMessageContaining("eu-central-1");
    }
}
