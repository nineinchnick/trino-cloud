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
import software.amazon.awssdk.services.s3.S3Client;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.regex.Pattern;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static org.assertj.core.api.Assertions.assertThat;

final class TestAwsDocumentation
{
    // the module directory is the working directory of tests
    private static final Path DOCUMENTATION = Path.of("README.md");
    // the first column of the tables in the Tables section, like | `ec2.instances` |
    private static final Pattern TABLE_ROW = Pattern.compile("^\\| `(\\w+\\.\\w+)` +\\|", Pattern.MULTILINE);

    @Test
    void testDocumentedTablesMatchConnectorTables()
            throws IOException
    {
        assertThat(documentedTables()).isEqualTo(connectorTables());
    }

    private static Set<String> documentedTables()
            throws IOException
    {
        return TABLE_ROW.matcher(tablesSection(Files.readString(DOCUMENTATION)))
                .results()
                .map(match -> match.group(1))
                .collect(toImmutableSet());
    }

    private static String tablesSection(String documentation)
    {
        int start = documentation.indexOf("\n# Tables\n");
        assertThat(start).as("Tables section").isNotNegative();
        int end = documentation.indexOf("\n# ", start + 1);
        assertThat(end).as("section after Tables").isPositive();
        return documentation.substring(start, end);
    }

    // the clients are only used to build the table definitions, so they never send any requests
    private static Set<String> connectorTables()
    {
        AwsConfig config = new AwsConfig()
                .setRegion("us-east-1")
                .setAccessKey("access-key")
                .setSecretKey("secret-key");
        try (AwsClientConfigurer clientConfigurer = new AwsClientConfigurer(config, OpenTelemetry.noop());
                Ec2Clients ec2Clients = new Ec2Clients(clientConfigurer);
                S3Client s3 = clientConfigurer.configure(S3Client.builder()).build()) {
            return new AwsTables(ec2Clients, s3).all().stream()
                    .map(table -> table.name().toString())
                    .collect(toImmutableSet());
        }
    }
}
