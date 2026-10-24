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
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Test;
import pl.net.was.cloud.aws.AwsConfig.RetryMode;

import java.net.URI;
import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertDeprecatedEquivalence;
import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

final class TestAwsConfig
{
    @Test
    void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(AwsConfig.class)
                .setRegion(null)
                .setRegions(ImmutableList.of())
                .setEndpoint(null)
                .setAccessKey(null)
                .setSecretKey(null)
                .setProfile(null)
                .setIamRole(null)
                .setExternalId(null)
                .setRoleSessionName("trino-cloud")
                .setMaxConnections(50)
                .setRetryMode(RetryMode.ADAPTIVE)
                .setMaxErrorRetries(10)
                .setReadOnly(true)
                .setMaxInsertedRows(10));
    }

    @Test
    void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("aws.region", "eu-central-1")
                .put("aws.regions", "eu-central-1, eu-west-1")
                .put("aws.endpoint", "http://localhost:4566")
                .put("aws.access-key", "access")
                .put("aws.secret-key", "secret")
                .put("aws.iam-role", "arn:aws:iam::123456789012:role/trino")
                .put("aws.external-id", "external")
                .put("aws.role-session-name", "session")
                .put("aws.max-connections", "10")
                .put("aws.retry-mode", "STANDARD")
                .put("aws.max-error-retries", "3")
                .put("aws.read-only", "false")
                .put("aws.max-inserted-rows", "100")
                .buildOrThrow();

        AwsConfig expected = new AwsConfig()
                .setRegion("eu-central-1")
                .setRegions(ImmutableList.of("eu-central-1", "eu-west-1"))
                .setEndpoint(URI.create("http://localhost:4566"))
                .setAccessKey("access")
                .setSecretKey("secret")
                .setIamRole("arn:aws:iam::123456789012:role/trino")
                .setExternalId("external")
                .setRoleSessionName("session")
                .setMaxConnections(10)
                .setRetryMode(RetryMode.STANDARD)
                .setMaxErrorRetries(3)
                .setReadOnly(false)
                .setMaxInsertedRows(100);

        // aws.profile is mutually exclusive with static credentials, so it's mapped in a separate test
        assertFullMapping(properties, expected, ImmutableSet.of("aws.profile"));
    }

    @Test
    void testProfilePropertyMapping()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("aws.region", "eu-central-1")
                .put("aws.regions", "eu-central-1, eu-west-1")
                .put("aws.endpoint", "http://localhost:4566")
                .put("aws.profile", "production")
                .put("aws.iam-role", "arn:aws:iam::123456789012:role/trino")
                .put("aws.external-id", "external")
                .put("aws.role-session-name", "session")
                .put("aws.max-connections", "10")
                .put("aws.retry-mode", "STANDARD")
                .put("aws.max-error-retries", "3")
                .put("aws.read-only", "false")
                .put("aws.max-inserted-rows", "100")
                .buildOrThrow();

        AwsConfig expected = new AwsConfig()
                .setRegion("eu-central-1")
                .setRegions(ImmutableList.of("eu-central-1", "eu-west-1"))
                .setEndpoint(URI.create("http://localhost:4566"))
                .setProfile("production")
                .setIamRole("arn:aws:iam::123456789012:role/trino")
                .setExternalId("external")
                .setRoleSessionName("session")
                .setMaxConnections(10)
                .setRetryMode(RetryMode.STANDARD)
                .setMaxErrorRetries(3)
                .setReadOnly(false)
                .setMaxInsertedRows(100);

        assertFullMapping(properties, expected, ImmutableSet.of("aws.access-key", "aws.secret-key"));
    }

    @Test
    void testLegacyPropertyMappings()
    {
        assertDeprecatedEquivalence(
                AwsConfig.class,
                ImmutableMap.of("aws.region", "eu-central-1"),
                ImmutableMap.of("region", "eu-central-1"));
    }
}
