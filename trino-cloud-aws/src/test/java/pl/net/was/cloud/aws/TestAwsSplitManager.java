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
import pl.net.was.cloud.aws.filters.FilterApplier;
import pl.net.was.cloud.aws.filters.FilterType;
import software.amazon.awssdk.services.ec2.model.Vpc;
import software.amazon.awssdk.services.s3.S3Client;

import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.spi.connector.DynamicFilterSnapshot.EMPTY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.function.Function.identity;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static pl.net.was.cloud.aws.filters.FilterApplier.ID_BATCH_SIZE;

@TestInstance(PER_CLASS)
final class TestAwsSplitManager
{
    private static final List<String> REGIONS = List.of("eu-west-1", "us-east-1");
    private static final AwsConfig CONFIG = new AwsConfig()
            .setRegion("us-east-1")
            .setRegions(REGIONS)
            .setAccessKey("access-key")
            .setSecretKey("secret-key");

    // the clients are only used to build the table definitions, so they never send any requests
    private final AwsClientConfigurer clientConfigurer = new AwsClientConfigurer(CONFIG, OpenTelemetry.noop());
    private final Ec2Clients ec2Clients = new Ec2Clients(clientConfigurer, CONFIG);
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
    void testWholeTableOfGlobalTables()
    {
        assertThat(splits(new AwsTableHandle(new SchemaTableName("ec2", "regions"))))
                .containsExactly(AwsConnectorSplit.WHOLE_TABLE);
        assertThat(splits(new AwsTableHandle(new SchemaTableName("s3", "buckets"))))
                .containsExactly(AwsConnectorSplit.WHOLE_TABLE);
    }

    @Test
    void testSplitPerRegionWithoutFilters()
    {
        for (String table : List.of("instances", "images", "snapshots", "vpcs")) {
            assertThat(splits(new AwsTableHandle(new SchemaTableName("ec2", table))))
                    .as(table)
                    .extracting(TestAwsSplitManager::filteredColumns)
                    .containsExactly(Map.of("region", List.of("eu-west-1")), Map.of("region", List.of("us-east-1")));
        }
    }

    @Test
    void testSplitPerRegionWhenValuesFitInOneRequest()
    {
        assertThat(splits(filtered("ec2", "instances", "instance_id", ids(ID_BATCH_SIZE))))
                .extracting(TestAwsSplitManager::filteredColumns)
                .containsExactly(Map.of("region", List.of("eu-west-1")), Map.of("region", List.of("us-east-1")));
    }

    @Test
    void testRegionFilter()
    {
        assertThat(splits(filtered("ec2", "vpcs", "region", List.of("us-east-1", "ap-south-1"))))
                .extracting(TestAwsSplitManager::filteredColumns)
                .containsExactly(Map.of("region", List.of("us-east-1")));
        assertThat(splits(filtered("ec2", "vpcs", "region", List.of("ap-south-1"))))
                .isEmpty();
        // the table handle already selects the only region
        assertThat(splits(filtered("ec2", "vpcs", "region", List.of("eu-west-1"))))
                .containsExactly(AwsConnectorSplit.WHOLE_TABLE);
    }

    @Test
    void testSplitPerRegionAndBatchOfIds()
    {
        List<String> ids = ids(2 * ID_BATCH_SIZE + 1);
        List<Map<String, List<String>>> expected = REGIONS.stream()
                .flatMap(region -> Stream.of(ids.subList(0, ID_BATCH_SIZE), ids.subList(ID_BATCH_SIZE, 2 * ID_BATCH_SIZE), ids.subList(2 * ID_BATCH_SIZE, ids.size()))
                        .map(batch -> Map.of("region", List.of(region), "instance_id", batch)))
                .toList();

        assertThat(splits(filtered("ec2", "instances", "instance_id", ids)))
                .extracting(TestAwsSplitManager::filteredColumns)
                .containsExactlyElementsOf(expected);
    }

    @Test
    void testSplitPerBucket()
    {
        assertThat(splits(filtered("s3", "objects", "bucket_name", List.of("a", "b", "c"))))
                .extracting(split -> values(split, "bucket_name"))
                .containsExactly(List.of("a"), List.of("b"), List.of("c"));
    }

    @Test
    void testNoSplittingOfNullableDomains()
    {
        AwsColumnHandle bucketName = new AwsColumnHandle("bucket_name", VARCHAR);
        AwsTableHandle table = new AwsTableHandle(new SchemaTableName("s3", "objects")).withConstraint(TupleDomain.withColumnDomains(Map.of(
                bucketName, Domain.multipleValues(VARCHAR, List.of(Slices.utf8Slice("a"), Slices.utf8Slice("b"))).union(Domain.onlyNull(VARCHAR)))));

        assertThat(splits(table)).containsExactly(AwsConnectorSplit.WHOLE_TABLE);
    }

    @Test
    void testSplitPerKnownValueWithoutFilter()
    {
        assertThat(knownValueSplits(Map.of()))
                .containsExactlyInAnyOrder(
                        Map.of("owner_id", List.of("owner-a")),
                        Map.of("owner_id", List.of("owner-b")));
    }

    @Test
    void testFilterOnKnownValues()
    {
        assertThat(knownValueSplits(Map.of("owner_id", List.of("owner-b", "owner-c"))))
                .containsExactly(Map.of("owner_id", List.of("owner-b")));
        assertThat(knownValueSplits(Map.of("owner_id", List.of("owner-c"))))
                .isEmpty();
        assertThat(knownValueSplits(Map.of("owner_id", List.of("owner-a"))))
                .containsExactly(Map.of());
    }

    @Test
    void testSplitPerCombinationOfBatches()
    {
        assertThat(knownValueSplits(Map.of("vpc_id", List.of("vpc-1", "vpc-2", "vpc-3"))))
                .containsExactlyInAnyOrder(
                        Map.of("owner_id", List.of("owner-a"), "vpc_id", List.of("vpc-1", "vpc-2")),
                        Map.of("owner_id", List.of("owner-a"), "vpc_id", List.of("vpc-3")),
                        Map.of("owner_id", List.of("owner-b"), "vpc_id", List.of("vpc-1", "vpc-2")),
                        Map.of("owner_id", List.of("owner-b"), "vpc_id", List.of("vpc-3")));
    }

    // owners are known values, like the configured regions of regional tables, and every split column is a varchar
    private static List<Map<String, List<String>>> knownValueSplits(Map<String, List<String>> filters)
    {
        FilterApplier filterApplier = new FilterApplier()
        {
            @Override
            public Map<String, FilterType> getSupportedFilters()
            {
                return Map.of("vpc_id", FilterType.EQUAL, "owner_id", FilterType.EQUAL);
            }

            @Override
            public Map<String, Integer> getSplitBatchSizes()
            {
                return Map.of("vpc_id", 2, "owner_id", 1);
            }

            @Override
            public Map<String, List<String>> getKnownValues()
            {
                return Map.of("owner_id", List.of("owner-a", "owner-b"));
            }
        };
        AwsTable table = AwsTable.builder(new SchemaTableName("test", "vpcs"), SdkTableColumns.of(Vpc.builder().sdkFields()))
                .rowSource((_, _) -> List.of())
                .filterApplier(filterApplier)
                .build();
        ImmutableMap.Builder<ColumnHandle, Domain> domains = ImmutableMap.builder();
        filters.forEach((column, values) -> domains.put(table.columnHandle(column), Domain.multipleValues(VARCHAR, values.stream().map(Slices::utf8Slice).toList())));
        return AwsSplitManager.splits(table, filterApplier, TupleDomain.withColumnDomains(domains.buildOrThrow())).stream()
                .map(TestAwsSplitManager::filteredColumns)
                .toList();
    }

    private static Map<String, List<String>> filteredColumns(AwsConnectorSplit split)
    {
        return split.constraint().getDomains().orElseThrow().keySet().stream()
                .map(column -> ((AwsColumnHandle) column).name())
                .collect(toImmutableMap(identity(), column -> values(split, column)));
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
