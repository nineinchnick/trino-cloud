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
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import org.junit.jupiter.api.Test;
import pl.net.was.cloud.aws.AwsTables.RegionalRowId;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;

import java.util.List;
import java.util.Map;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestAwsTables
{
    @Test
    void testBucketLocationConstraint()
    {
        CreateBucketRequest request = CreateBucketRequest.builder().bucket("my-bucket").build();

        assertThat(AwsTables.withLocationConstraint(request, "us-east-1")).isEqualTo(request);
        assertThat(AwsTables.withLocationConstraint(request, "eu-central-1").createBucketConfiguration().locationConstraintAsString()).isEqualTo("eu-central-1");

        CreateBucketRequest withRegion = request.toBuilder()
                .createBucketConfiguration(configuration -> configuration.locationConstraint("eu-west-1"))
                .build();
        assertThat(AwsTables.withLocationConstraint(withRegion, "eu-central-1")).isEqualTo(withRegion);
    }

    // the clients are only used to build the table definitions, so they never send any requests
    @Test
    void testRegionColumnOfRegionalTables()
    {
        AwsConfig config = new AwsConfig()
                .setRegion("us-east-1")
                .setAccessKey("access-key")
                .setSecretKey("secret-key");
        try (AwsClientConfigurer clientConfigurer = new AwsClientConfigurer(config, OpenTelemetry.noop());
                Ec2Clients ec2Clients = new Ec2Clients(clientConfigurer, config);
                S3Client s3 = clientConfigurer.configure(S3Client.builder()).build()) {
            for (AwsTable table : new AwsTables(ec2Clients, s3).all()) {
                List<String> columnNames = table.columns().stream()
                        .map(ColumnMetadata::getName)
                        .collect(toImmutableList());
                boolean regional = table.name().getSchemaName().equals("ec2") && !table.name().getTableName().equals("regions");
                if (!regional) {
                    assertThat(columnNames).as(table.name().toString()).doesNotContain("region");
                    continue;
                }
                assertThat(columnNames).as(table.name().toString()).contains("region");
                AwsColumnHandle region = table.columnHandle("region");
                Domain defaultRegion = Domain.singleValue(VARCHAR, utf8Slice("us-east-1"));
                assertThat(table.filters().splitConstraints(TupleDomain.all()))
                        .as(table.name().toString())
                        .extracting(split -> split.getDomains().orElseThrow().get(region))
                        .containsExactly(defaultRegion);
                TupleDomain<ColumnHandle> regionFilter = TupleDomain.withColumnDomains(Map.of(region, defaultRegion));
                assertThat(table.filters().applyFilter(new AwsTableHandle(table.name()), new Constraint(regionFilter)))
                        .as(table.name().toString())
                        .hasValueSatisfying(result -> assertThat(result.getRemainingFilter().isAll()).isTrue());
            }
        }
    }

    @Test
    void testInputOnlyColumns()
    {
        AwsConfig config = new AwsConfig()
                .setRegion("us-east-1")
                .setAccessKey("access-key")
                .setSecretKey("secret-key");
        try (AwsClientConfigurer clientConfigurer = new AwsClientConfigurer(config, OpenTelemetry.noop());
                Ec2Clients ec2Clients = new Ec2Clients(clientConfigurer, config);
                S3Client s3 = clientConfigurer.configure(S3Client.builder()).build()) {
            AwsTable images = new AwsTables(ec2Clients, s3).require(new SchemaTableName("ec2", "images"));
            assertThat(images.column("instance_id").getComment()).contains("Only used by INSERT, always NULL when reading");
            assertThat(images.column("image_id").getComment()).hasValue("Pushed down: = and IN");
        }
    }

    @Test
    void testRegionalRowId()
    {
        RegionalRowId rowId = new RegionalRowId("eu-west-1", "i-123");

        assertThat(rowId.toString()).isEqualTo("eu-west-1/i-123");
        assertThat(RegionalRowId.parse(rowId.toString())).isEqualTo(rowId);
        assertThatThrownBy(() -> RegionalRowId.parse("i-123"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Row ID without a region: i-123");
    }
}
