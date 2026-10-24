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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Test;

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
                .setEndpoint(null)
                .setAccessKey(null)
                .setSecretKey(null)
                .setProfile(null)
                .setIamRole(null)
                .setExternalId(null)
                .setRoleSessionName("trino-cloud"));
    }

    @Test
    void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("aws.region", "eu-central-1")
                .put("aws.endpoint", "http://localhost:4566")
                .put("aws.access-key", "access")
                .put("aws.secret-key", "secret")
                .put("aws.iam-role", "arn:aws:iam::123456789012:role/trino")
                .put("aws.external-id", "external")
                .put("aws.role-session-name", "session")
                .buildOrThrow();

        AwsConfig expected = new AwsConfig()
                .setRegion("eu-central-1")
                .setEndpoint(URI.create("http://localhost:4566"))
                .setAccessKey("access")
                .setSecretKey("secret")
                .setIamRole("arn:aws:iam::123456789012:role/trino")
                .setExternalId("external")
                .setRoleSessionName("session");

        // aws.profile is mutually exclusive with static credentials, so it's mapped in a separate test
        assertFullMapping(properties, expected, ImmutableSet.of("aws.profile"));
    }

    @Test
    void testProfilePropertyMapping()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("aws.region", "eu-central-1")
                .put("aws.endpoint", "http://localhost:4566")
                .put("aws.profile", "production")
                .put("aws.iam-role", "arn:aws:iam::123456789012:role/trino")
                .put("aws.external-id", "external")
                .put("aws.role-session-name", "session")
                .buildOrThrow();

        AwsConfig expected = new AwsConfig()
                .setRegion("eu-central-1")
                .setEndpoint(URI.create("http://localhost:4566"))
                .setProfile("production")
                .setIamRole("arn:aws:iam::123456789012:role/trino")
                .setExternalId("external")
                .setRoleSessionName("session");

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
