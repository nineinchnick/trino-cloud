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
import io.trino.spi.connector.ConnectorFactory;
import io.trino.testing.TestingConnectorContext;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static com.google.common.collect.Iterables.getOnlyElement;
import static org.assertj.core.api.Assertions.assertThat;

final class TestAwsPlugin
{
    private static final Map<String, String> CONFIG = ImmutableMap.of(
            "aws.region", "us-east-1",
            "aws.access-key", "access",
            "aws.secret-key", "secret");

    @Test
    void testCreateConnector()
    {
        ConnectorFactory factory = getOnlyElement(new AwsPlugin().getConnectorFactories());
        assertThat(factory.getName()).isEqualTo("aws");
        factory.create("test", CONFIG, new TestingConnectorContext())
                .shutdown();
    }

    @Test
    void testCreateConnectorWithIamRole()
    {
        ConnectorFactory factory = getOnlyElement(new AwsPlugin().getConnectorFactories());
        factory.create(
                        "test",
                        ImmutableMap.<String, String>builder()
                                .putAll(CONFIG)
                                .put("aws.iam-role", "arn:aws:iam::123456789012:role/trino")
                                .buildOrThrow(),
                        new TestingConnectorContext())
                .shutdown();
    }
}
