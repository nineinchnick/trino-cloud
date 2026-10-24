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
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotBlank;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;

import java.net.URI;
import java.util.List;

import static io.airlift.testing.ValidationAssertions.assertFailsValidation;
import static io.airlift.testing.ValidationAssertions.assertValidates;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestAwsConfigValidation
{
    @Test
    void testValidConfigurations()
    {
        assertValidates(new AwsConfig());
        assertValidates(new AwsConfig()
                .setAccessKey("access")
                .setSecretKey("secret"));
        assertValidates(new AwsConfig()
                .setProfile("production")
                .setIamRole("arn:aws:iam::123456789012:role/trino")
                .setExternalId("external"));
    }

    @Test
    void testAccessKeyWithoutSecretKey()
    {
        assertFailsValidation(
                new AwsConfig().setAccessKey("access"),
                "staticCredentialsComplete",
                "aws.access-key and aws.secret-key must be set together",
                AssertTrue.class);
    }

    @Test
    void testSecretKeyWithoutAccessKey()
    {
        assertFailsValidation(
                new AwsConfig().setSecretKey("secret"),
                "staticCredentialsComplete",
                "aws.access-key and aws.secret-key must be set together",
                AssertTrue.class);
    }

    @Test
    void testProfileWithStaticCredentials()
    {
        assertFailsValidation(
                new AwsConfig()
                        .setProfile("production")
                        .setAccessKey("access")
                        .setSecretKey("secret"),
                "profileExclusiveWithStaticCredentials",
                "aws.profile cannot be set together with aws.access-key and aws.secret-key",
                AssertTrue.class);
    }

    @Test
    void testExternalIdWithoutIamRole()
    {
        assertFailsValidation(
                new AwsConfig().setExternalId("external"),
                "externalIdUsedWithIamRole",
                "aws.external-id can only be set together with aws.iam-role",
                AssertTrue.class);
    }

    @Test
    void testAllRegionsWithRegionNames()
    {
        assertValidates(new AwsConfig().setRegions(List.of("*")));
        assertFailsValidation(
                new AwsConfig().setRegions(List.of("*", "eu-west-1")),
                "allRegionsExclusive",
                "aws.regions must be either * or a list of region names",
                AssertTrue.class);
    }

    @Test
    void testBlankRoleSessionName()
    {
        assertFailsValidation(
                new AwsConfig().setRoleSessionName(" "),
                "roleSessionName",
                "must not be blank",
                NotBlank.class);
    }

    @Test
    void testConfiguredRegion()
    {
        AwsClientConfigurer configurer = new AwsClientConfigurer(new AwsConfig().setRegion("eu-central-1"), OpenTelemetry.noop());

        try (Ec2Client client = configurer.configure(Ec2Client.builder()).build()) {
            assertThat(client.serviceClientConfiguration().region()).isEqualTo(Region.EU_CENTRAL_1);
        }
    }

    @Test
    void testUnknownRegion()
    {
        assertThatThrownBy(() -> new AwsClientConfigurer(new AwsConfig().setRegion("eu-centrall-1"), OpenTelemetry.noop()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageStartingWith("Unknown AWS region 'eu-centrall-1', set aws.region to one of: ")
                .hasMessageContaining("eu-central-1");
    }

    @Test
    void testEndpointOverride()
    {
        AwsClientConfigurer configurer = new AwsClientConfigurer(new AwsConfig()
                .setRegion("us-east-1")
                .setEndpoint(URI.create("http://localhost:4566")), OpenTelemetry.noop());

        try (Ec2Client client = configurer.configure(Ec2Client.builder()).build()) {
            assertThat(client.serviceClientConfiguration().endpointOverride()).contains(URI.create("http://localhost:4566"));
        }
    }

    @Test
    void testStaticCredentials()
    {
        AwsClientConfigurer configurer = new AwsClientConfigurer(new AwsConfig()
                .setRegion("us-east-1")
                .setAccessKey("access")
                .setSecretKey("secret"), OpenTelemetry.noop());

        try (Ec2Client client = configurer.configure(Ec2Client.builder()).build()) {
            StaticCredentialsProvider credentialsProvider = (StaticCredentialsProvider) client.serviceClientConfiguration().credentialsProvider();
            AwsCredentials credentials = credentialsProvider.resolveCredentials();
            assertThat(credentials.accessKeyId()).isEqualTo("access");
            assertThat(credentials.secretAccessKey()).isEqualTo("secret");
            assertThat(credentialsProvider.toString()).doesNotContain("secret");
        }
    }

    @Test
    void testIamRole()
    {
        AwsClientConfigurer configurer = new AwsClientConfigurer(new AwsConfig()
                .setRegion("us-east-1")
                .setAccessKey("access")
                .setSecretKey("secret")
                .setIamRole("arn:aws:iam::123456789012:role/trino"), OpenTelemetry.noop());

        try (Ec2Client client = configurer.configure(Ec2Client.builder()).build()) {
            assertThat(client.serviceClientConfiguration().credentialsProvider()).isInstanceOf(StsAssumeRoleCredentialsProvider.class);
        }
    }
}
