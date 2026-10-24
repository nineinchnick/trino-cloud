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

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.testing.TestingConnectorSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import software.amazon.awssdk.services.s3.S3Client;

import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static io.trino.spi.connector.DynamicFilterSnapshot.EMPTY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static pl.net.was.cloud.aws.filters.FilterApplier.ID_BATCH_SIZE;

@TestInstance(PER_CLASS)
final class TestAwsSplitManager
{
    private static final AwsConfig CONFIG = new AwsConfig()
            .setRegion("us-east-1")
            .setAccessKey("access-key")
            .setSecretKey("secret-key");

    // the clients are only used to build the table definitions, so they never send any requests
    private final AwsClientConfigurer clientConfigurer = new AwsClientConfigurer(CONFIG, OpenTelemetry.noop());
    private final Ec2Clients ec2Clients = new Ec2Clients(clientConfigurer);
    private final S3Client s3 = clientConfigurer.configure(S3Client.builder()).build();
    private final AwsSplitManager splitManager = new AwsSplitManager(new AwsTables(ec2Clients, s3));

    @AfterAll
    void tearDown()
    {
        ec2Clients.close();
        s3.close();
        clientConfigurer.close();
    }

    @Test
    void testWholeTableWithoutFilters()
    {
        assertThat(splits(new AwsTableHandle(new SchemaTableName("ec2", "instances"))))
                .containsExactly(AwsConnectorSplit.WHOLE_TABLE);
    }

    @Test
    void testWholeTableWhenValuesFitInOneRequest()
    {
        assertThat(splits(filtered("ec2", "instances", "instance_id", ids(ID_BATCH_SIZE))))
                .containsExactly(AwsConnectorSplit.WHOLE_TABLE);
    }

    @Test
    void testSplitPerBucket()
    {
        assertThat(splits(filtered("s3", "objects", "bucket_name", List.of("a", "b", "c"))))
                .extracting(split -> values(split, "bucket_name"))
                .containsExactly(List.of("a"), List.of("b"), List.of("c"));
    }

    @Test
    void testSplitPerBatchOfIds()
    {
        List<List<String>> batches = splits(filtered("ec2", "instances", "instance_id", ids(2 * ID_BATCH_SIZE + 1))).stream()
                .map(split -> values(split, "instance_id"))
                .toList();

        assertThat(batches).hasSize(3);
        assertThat(batches).extracting(List::size).containsExactly(ID_BATCH_SIZE, ID_BATCH_SIZE, 1);
        assertThat(batches.stream().flatMap(List::stream).toList()).isEqualTo(ids(2 * ID_BATCH_SIZE + 1));
    }

    @Test
    void testNoSplittingOfNullableDomains()
    {
        AwsColumnHandle bucketName = new AwsColumnHandle("bucket_name", VARCHAR);
        AwsTableHandle table = new AwsTableHandle(new SchemaTableName("s3", "objects")).withConstraint(TupleDomain.withColumnDomains(Map.of(
                bucketName, Domain.multipleValues(VARCHAR, List.of(Slices.utf8Slice("a"), Slices.utf8Slice("b"))).union(Domain.onlyNull(VARCHAR)))));

        assertThat(splits(table)).containsExactly(AwsConnectorSplit.WHOLE_TABLE);
    }

    private List<AwsConnectorSplit> splits(AwsTableHandle table)
    {
        try (ConnectorSplitSource splitSource = splitManager.getSplits(null, TestingConnectorSession.SESSION, table, DynamicFilter.EMPTY.getColumnsCovered(), Constraint.alwaysTrue())) {
            List<ConnectorSplit> splits = splitSource.getNextBatch(1000, EMPTY).join();
            assertThat(splitSource.isFinished()).isTrue();
            return splits.stream()
                    .map(AwsConnectorSplit.class::cast)
                    .toList();
        }
    }

    private static AwsTableHandle filtered(String schema, String table, String column, List<String> values)
    {
        return new AwsTableHandle(new SchemaTableName(schema, table)).withConstraint(TupleDomain.withColumnDomains(Map.of(
                new AwsColumnHandle(column, VARCHAR), Domain.multipleValues(VARCHAR, values.stream().map(Slices::utf8Slice).toList()))));
    }

    private static List<String> values(AwsConnectorSplit split, String column)
    {
        Map<ColumnHandle, Domain> domains = split.constraint().getDomains().orElseThrow();
        return domains.get(new AwsColumnHandle(column, VARCHAR)).getValues().getDiscreteSet().stream()
                .map(value -> ((Slice) value).toStringUtf8())
                .toList();
    }

    private static List<String> ids(int count)
    {
        return IntStream.range(0, count)
                .mapToObj("i-%017x"::formatted)
                .toList();
    }
}
