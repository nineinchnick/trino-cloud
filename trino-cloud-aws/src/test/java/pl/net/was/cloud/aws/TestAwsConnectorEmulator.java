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
import io.trino.sql.planner.plan.FilterNode;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import io.trino.testing.containers.Floci;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.IntStream;

import static com.google.common.collect.MoreCollectors.onlyElement;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.stream.Collectors.joining;
import static org.assertj.core.api.Assertions.assertThat;
import static pl.net.was.cloud.aws.EmulatorTestData.IMAGE_ID;
import static pl.net.was.cloud.aws.EmulatorTestData.IMAGE_OWNER_ID;
import static pl.net.was.cloud.aws.EmulatorTestData.INSTANCE_TYPE;
import static pl.net.was.cloud.aws.EmulatorTestData.MANY_OBJECTS_BUCKET;
import static pl.net.was.cloud.aws.EmulatorTestData.MANY_OBJECTS_DELETED_COUNT;
import static pl.net.was.cloud.aws.EmulatorTestData.MANY_OBJECTS_LIVE_COUNT;
import static pl.net.was.cloud.aws.EmulatorTestData.NAME;
import static pl.net.was.cloud.aws.EmulatorTestData.OBJECTS_BUCKET;
import static pl.net.was.cloud.aws.EmulatorTestData.VERSIONED_BUCKET;
import static pl.net.was.cloud.aws.filters.FilterApplier.ID_BATCH_SIZE;

final class TestAwsConnectorEmulator
        extends AbstractTestQueryFramework
{
    private static final String MISSING_INSTANCE_ID = "i-0123456789abcdef0";
    private static final String IMAGE_NAME = "amzn2-ami-hvm-2.0.20230404.0-x86_64-gp2";
    // Floci doesn't support DescribePlacementGroups, DescribePublicIpv4Pools and DescribeVpnConnections,
    // so placement_groups, public_ipv4_pools and vpn_connections are only covered by TestAwsQueriesRealAccount
    private static final List<String> EC2_TABLES = List.of(
            "availability_zones",
            "images",
            "instance_types",
            "instances",
            "key_pairs",
            "launch_templates",
            "nat_gateways",
            "network_interfaces",
            "prefix_lists",
            "regions",
            "route_tables",
            "security_groups",
            "snapshots",
            "subnets",
            "tags",
            "volumes",
            "vpc_endpoints",
            "vpc_peering_connections",
            "vpcs",
            "vpn_gateways");

    private EmulatorTestData testData;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Floci emulator = closeAfterClass(EmulatorAwsQueryRunner.createEmulator());
        emulator.start();
        testData = EmulatorTestData.create(emulator);
        return EmulatorAwsQueryRunner.createQueryRunner(emulator);
    }

    @Test
    void testShowSchemas()
    {
        assertQuery("SHOW SCHEMAS FROM aws", "VALUES 'ec2', 'information_schema', 's3'");
    }

    @Test
    void testShowTables()
    {
        assertQuery(
                "SHOW TABLES FROM aws.ec2",
                "VALUES 'availability_zones', 'images', 'instance_types', 'instances', 'key_pairs', 'launch_templates', 'nat_gateways', 'network_interfaces', 'placement_groups', 'prefix_lists', 'public_ipv4_pools', 'regions', 'route_tables', 'security_groups', 'snapshots', 'subnets', 'tags', 'volumes', 'vpc_endpoints', 'vpc_peering_connections', 'vpcs', 'vpn_connections', 'vpn_gateways'");
        assertQuery(
                "SHOW TABLES FROM aws.s3",
                "VALUES 'buckets', 'deleted_objects', 'objects', 'object_versions'");
    }

    @Test
    void testSelectAllColumns()
    {
        for (String table : EC2_TABLES) {
            assertThat(query("SELECT * FROM aws.ec2." + table))
                    .as(table)
                    .succeeds();
        }
        assertThat(query("SELECT * FROM aws.s3.buckets")).succeeds();
        assertThat(query("SELECT * FROM aws.s3.objects")).succeeds();
        assertThat(query("SELECT * FROM aws.s3.object_versions")).succeeds();
        assertThat(query("SELECT * FROM aws.s3.deleted_objects")).succeeds();
    }

    @Test
    void testAvailabilityZones()
    {
        assertQuery(
                "SELECT zone_name FROM aws.ec2.availability_zones",
                "VALUES 'us-east-1a', 'us-east-1b', 'us-east-1c'");
    }

    @Test
    void testRegions()
    {
        assertQuery(
                "SELECT count(*) > 0 FROM aws.ec2.regions WHERE region_name = 'us-east-1'",
                "VALUES true");
    }

    @Test
    void testInstanceTypes()
    {
        assertQuery(
                "SELECT instance_type FROM aws.ec2.instance_types WHERE instance_type = '%s'".formatted(INSTANCE_TYPE),
                "VALUES '%s'".formatted(INSTANCE_TYPE));
    }

    @Test
    void testInstances()
    {
        assertQuery(
                "SELECT instance_id, image_id, instance_type, vpc_id FROM aws.ec2.instances WHERE subnet_id = '%s'".formatted(testData.subnetId()),
                testData.instanceIds().stream()
                        .map(instanceId -> "('%s', '%s', '%s', '%s')".formatted(instanceId, IMAGE_ID, INSTANCE_TYPE, testData.vpcId()))
                        .collect(joining(", ", "VALUES ", "")));
    }

    @Test
    void testInstanceIdPushdown()
    {
        String instanceId = testData.instanceIds().getFirst();
        assertThat(query("SELECT instance_id, subnet_id FROM aws.ec2.instances WHERE instance_id = '%s'".formatted(instanceId)))
                .isNotFullyPushedDown(FilterNode.class)
                .matches("VALUES (VARCHAR '%s', VARCHAR '%s')".formatted(instanceId, testData.subnetId()));
    }

    @Test
    void testMissingInstanceId()
    {
        assertQueryReturnsEmptyResult("SELECT instance_id FROM aws.ec2.instances WHERE instance_id = 'i-0123456789abcdef0'");
        assertQueryReturnsEmptyResult("SELECT instance_id FROM aws.ec2.instances WHERE instance_id = 'not-an-instance-id'");
    }

    @Test
    void testManyInstanceIds()
    {
        List<String> instanceIds = ImmutableList.<String>builder()
                .addAll(testData.instanceIds())
                .addAll(IntStream.range(0, 2 * ID_BATCH_SIZE)
                        .mapToObj("i-%017x"::formatted)
                        .toList())
                .build();
        String sql = "SELECT instance_id FROM aws.ec2.instances WHERE instance_id IN (%s)".formatted(sqlList(instanceIds));
        assertThat(query(sql))
                .matches(testData.instanceIds().stream()
                        .map("VARCHAR '%s'"::formatted)
                        .collect(joining(", ", "VALUES ", "")));
        assertSplitCount(sql, 3);
    }

    @Test
    void testInstanceIdInList()
    {
        String firstId = testData.instanceIds().get(0);
        String secondId = testData.instanceIds().get(1);
        assertThat(query("SELECT instance_id FROM aws.ec2.instances WHERE instance_id IN ('%s', '%s')".formatted(firstId, secondId)))
                .isNotFullyPushedDown(FilterNode.class)
                .matches("VALUES VARCHAR '%s', VARCHAR '%s'".formatted(firstId, secondId));
        // missing IDs must not hide the existing ones
        assertThat(query("SELECT instance_id FROM aws.ec2.instances WHERE instance_id IN ('%s', '%s', 'not-an-instance-id')".formatted(firstId, MISSING_INSTANCE_ID)))
                .isNotFullyPushedDown(FilterNode.class)
                .matches("VALUES VARCHAR '%s'".formatted(firstId));
    }

    @Test
    void testInstanceIdWildcards()
    {
        // EC2 treats * and ? in filter values as wildcards, but SQL equality must not
        assertQueryReturnsEmptyResult("SELECT instance_id FROM aws.ec2.instances WHERE instance_id = 'i-*'");
        assertQueryReturnsEmptyResult("SELECT instance_id FROM aws.ec2.instances WHERE instance_id IN ('i-0123456789abcdef?', 'i-*')");
    }

    @Test
    void testInstanceIdIsNull()
    {
        String instanceId = testData.instanceIds().getFirst();
        assertThat(query("SELECT instance_id FROM aws.ec2.instances WHERE instance_id IS NULL"))
                .isNotFullyPushedDown(FilterNode.class)
                .returnsEmptyResult();
        assertThat(query("SELECT instance_id FROM aws.ec2.instances WHERE instance_id IS NULL OR instance_id = '%s'".formatted(instanceId)))
                .isNotFullyPushedDown(FilterNode.class)
                .matches("VALUES VARCHAR '%s'".formatted(instanceId));
    }

    @Test
    void testInstanceTags()
    {
        assertQuery(
                "SELECT count(*) FROM aws.ec2.instances WHERE tags['Name'] = '%s'".formatted(NAME),
                "VALUES 2");
    }

    @Test
    void testNestedObjectsAsRows()
    {
        assertQuery(
                "SELECT state.name, state.code, placement.availability_zone, security_groups[1].group_id FROM aws.ec2.instances WHERE instance_id = '%s'".formatted(testData.instanceIds().getFirst()),
                "VALUES ('running', 16, 'us-east-1a', '%s')".formatted(testData.securityGroupId()));
        assertThat(query("SELECT typeof(state), typeof(tags) FROM aws.ec2.instances WHERE instance_id = '%s'".formatted(testData.instanceIds().getFirst())))
                .matches("VALUES (VARCHAR 'row(\"code\" integer, \"name\" varchar)', VARCHAR 'map(varchar, varchar)')");
    }

    @Test
    void testStateName()
    {
        assertQuery(
                "SELECT state_name, state.name FROM aws.ec2.instances WHERE instance_id = '%s'".formatted(testData.instanceIds().getFirst()),
                "VALUES ('running', 'running')");
        List<String> columns = computeActual("SHOW COLUMNS FROM aws.ec2.instances").getMaterializedRows().stream()
                .map(row -> (String) row.getField(0))
                .toList();
        assertThat(columns.get(columns.indexOf("state") + 1)).isEqualTo("state_name");
    }

    @Test
    void testTimestampColumns()
    {
        assertThat(query("SELECT typeof(launch_time), launch_time BETWEEN now() - INTERVAL '1' DAY AND now() FROM aws.ec2.instances WHERE instance_id = '%s'".formatted(testData.instanceIds().getFirst())))
                .matches("VALUES (VARCHAR 'timestamp(3) with time zone', true)");
    }

    @Test
    void testNullStringColumns()
    {
        assertQuery(
                "SELECT platform IS NULL FROM aws.ec2.instances WHERE instance_id = '%s'".formatted(testData.instanceIds().getFirst()),
                "VALUES true");
    }

    @Test
    void testNullTimestampColumns()
    {
        assertQuery(
                "SELECT usage_operation_update_time IS NULL, launch_time IS NULL FROM aws.ec2.instances WHERE instance_id = '%s'".formatted(testData.instanceIds().getFirst()),
                "VALUES (true, false)");
    }

    @Test
    void testNullNestedObjectColumns()
    {
        assertQuery(
                "SELECT iam_instance_profile IS NULL, placement.host_id IS NULL, placement.availability_zone FROM aws.ec2.instances WHERE instance_id = '%s'".formatted(testData.instanceIds().getFirst()),
                "VALUES (true, true, 'us-east-1a')");
    }

    @Test
    void testNullListColumns()
    {
        assertQuery(
                "SELECT product_codes IS NULL, licenses IS NULL, cardinality(tags) FROM aws.ec2.instances WHERE instance_id = '%s'".formatted(testData.instanceIds().getFirst()),
                "VALUES (true, true, 1)");
    }

    @Test
    void testImagesDefaultToOwnImages()
    {
        assertThat(query("SELECT image_id, owner_id FROM aws.ec2.images"))
                .matches("VALUES (VARCHAR '%s', VARCHAR '%s')".formatted(testData.ownImageId(), testData.accountId()));
        assertThat(query("SELECT image_id FROM aws.ec2.images LIMIT 1"))
                .matches("VALUES VARCHAR '%s'".formatted(testData.ownImageId()));
        assertThat(query("SELECT image_id FROM aws.ec2.images WHERE name LIKE 'trino%'"))
                .matches("VALUES VARCHAR '%s'".formatted(testData.ownImageId()));
    }

    // isFullyPushedDown() and isNotFullyPushedDown() can't be used for images, because they also run the query with pushdown disabled,
    // and without a pushed down owner the table only returns your own images
    @Test
    void testImagesByOwner()
    {
        assertThat(query("SELECT image_id FROM aws.ec2.images WHERE owner_id = '%s'".formatted(IMAGE_OWNER_ID)))
                .matches("VALUES VARCHAR 'ami-0abcdef1234567890', VARCHAR 'ami-0abcdef1234567891', VARCHAR 'ami-amazonlinux2023-arm64'");
        assertThat(query("SELECT image_id FROM aws.ec2.images WHERE owner_id IN ('%s', '%s')".formatted(IMAGE_OWNER_ID, testData.accountId())))
                .matches("VALUES VARCHAR 'ami-0abcdef1234567890', VARCHAR 'ami-0abcdef1234567891', VARCHAR 'ami-amazonlinux2023-arm64', VARCHAR '%s'".formatted(testData.ownImageId()));
    }

    @Test
    void testImagesByOwnerAlias()
    {
        // AWS returns your own images for the self alias, but their owner_id is the account ID, so Trino filters them out
        assertQueryReturnsEmptyResult("SELECT image_id FROM aws.ec2.images WHERE owner_id = 'self'");
    }

    @Test
    void testImageById()
    {
        assertThat(query("SELECT name, owner_id FROM aws.ec2.images WHERE image_id = '%s'".formatted(IMAGE_ID)))
                .matches("VALUES (VARCHAR '%s', VARCHAR '%s')".formatted(IMAGE_NAME, IMAGE_OWNER_ID));
    }

    @Test
    void testImagesByName()
    {
        assertThat(query("SELECT image_id FROM aws.ec2.images WHERE owner_id = '%s' AND name IN ('%s', 'missing')".formatted(IMAGE_OWNER_ID, IMAGE_NAME)))
                .matches("VALUES VARCHAR '%s'".formatted(IMAGE_ID));
        // EC2 treats * and ? in filter values as wildcards, but SQL equality must not
        assertQueryReturnsEmptyResult("SELECT image_id FROM aws.ec2.images WHERE owner_id = '%s' AND name = 'amzn2*'".formatted(IMAGE_OWNER_ID));
        assertQueryReturnsEmptyResult("SELECT image_id FROM aws.ec2.images WHERE owner_id = '%s' AND name = 'amzn2-ami-hvm-2.0.20230404.0-x86_64-gp?'".formatted(IMAGE_OWNER_ID));
    }

    @Test
    void testManyImageIds()
    {
        List<String> imageIds = ImmutableList.<String>builder()
                .add(IMAGE_ID, testData.ownImageId())
                .addAll(IntStream.range(0, ID_BATCH_SIZE)
                        .mapToObj("not-an-image-id-%d"::formatted)
                        .toList())
                .build();
        String sql = "SELECT image_id FROM aws.ec2.images WHERE image_id IN (%s)".formatted(sqlList(imageIds));
        assertThat(query(sql))
                .matches("VALUES VARCHAR '%s', VARCHAR '%s'".formatted(IMAGE_ID, testData.ownImageId()));
        assertSplitCount(sql, 2);
    }

    @Test
    void testImageIdInList()
    {
        // Floci returns a made-up image for any well-formed image ID, so only a malformed ID can be missing
        assertThat(query("SELECT image_id FROM aws.ec2.images WHERE image_id IN ('%s', '%s', 'not-an-image-id')".formatted(IMAGE_ID, testData.ownImageId())))
                .matches("VALUES VARCHAR '%s', VARCHAR '%s'".formatted(IMAGE_ID, testData.ownImageId()));
    }

    @Test
    void testImageIdIsNull()
    {
        assertQueryReturnsEmptyResult("SELECT image_id FROM aws.ec2.images WHERE image_id IS NULL");
        assertThat(query("SELECT image_id FROM aws.ec2.images WHERE image_id IS NULL OR image_id = '%s'".formatted(testData.ownImageId())))
                .matches("VALUES VARCHAR '%s'".formatted(testData.ownImageId()));
    }

    // Floci doesn't support CreateSnapshot, so these only check that the pushed down filters are accepted
    @Test
    void testSnapshotFilters()
    {
        assertQueryReturnsEmptyResult("SELECT snapshot_id FROM aws.ec2.snapshots");
        assertQueryReturnsEmptyResult("SELECT snapshot_id FROM aws.ec2.snapshots WHERE owner_id IN ('%s', '%s')".formatted(IMAGE_OWNER_ID, testData.accountId()));
        assertQueryReturnsEmptyResult("SELECT snapshot_id FROM aws.ec2.snapshots WHERE volume_id IN ('vol-0123456789abcdef0', 'vol-0123456789abcdef1')");
    }

    @Test
    void testMissingSnapshotId()
    {
        assertQueryReturnsEmptyResult("SELECT snapshot_id FROM aws.ec2.snapshots WHERE snapshot_id = 'snap-0123456789abcdef0'");
        assertQueryReturnsEmptyResult("SELECT snapshot_id FROM aws.ec2.snapshots WHERE snapshot_id IN ('snap-0123456789abcdef0', 'not-a-snapshot-id')");
    }

    @Test
    void testMissingImageId()
    {
        // Floci returns a made-up image for any well-formed image ID, so only a malformed ID can be tested
        assertQueryReturnsEmptyResult("SELECT image_id FROM aws.ec2.images WHERE image_id = 'not-an-image-id'");
    }

    @Test
    void testVpcs()
    {
        assertQuery(
                "SELECT cidr_block FROM aws.ec2.vpcs WHERE vpc_id = '%s'".formatted(testData.vpcId()),
                "VALUES '10.0.0.0/16'");
    }

    @Test
    void testSubnets()
    {
        assertQuery(
                "SELECT vpc_id, cidr_block, availability_zone FROM aws.ec2.subnets WHERE subnet_id = '%s'".formatted(testData.subnetId()),
                "VALUES ('%s', '10.0.1.0/24', 'us-east-1a')".formatted(testData.vpcId()));
    }

    @Test
    void testSecurityGroups()
    {
        assertQuery(
                "SELECT group_name, vpc_id FROM aws.ec2.security_groups WHERE group_id = '%s'".formatted(testData.securityGroupId()),
                "VALUES ('%s', '%s')".formatted(NAME, testData.vpcId()));
    }

    @Test
    void testTags()
    {
        assertQuery(
                "SELECT key, resource_id, resource_type, value FROM aws.ec2.tags WHERE resource_id = '%s'".formatted(testData.vpcId()),
                "VALUES ('Name', '%s', 'vpc', '%s')".formatted(testData.vpcId(), NAME));
    }

    @Test
    void testKeyPairs()
    {
        assertQuery("SELECT key_name FROM aws.ec2.key_pairs", "VALUES '%s'".formatted(NAME));
    }

    @Test
    void testLaunchTemplates()
    {
        assertQuery("SELECT launch_template_name FROM aws.ec2.launch_templates", "VALUES '%s'".formatted(NAME));
    }

    @Test
    void testVpcEndpoints()
    {
        assertQuery(
                "SELECT vpc_id, service_name FROM aws.ec2.vpc_endpoints",
                "VALUES ('%s', 'com.amazonaws.us-east-1.s3')".formatted(testData.vpcId()));
    }

    @Test
    void testBuckets()
    {
        assertQuery(
                "SELECT name FROM aws.s3.buckets",
                "VALUES '%s', '%s', '%s'".formatted(OBJECTS_BUCKET, VERSIONED_BUCKET, MANY_OBJECTS_BUCKET));
    }

    // Floci ignores MaxResults and never returns a NextToken for EC2 describe calls, so only S3 can test reading more than one page
    @Test
    void testObjectsSpanningManyPages()
    {
        assertQuery(
                "SELECT count(*), count(DISTINCT key) FROM aws.s3.objects WHERE bucket_name = '%s'".formatted(MANY_OBJECTS_BUCKET),
                "VALUES (%1$s, %1$s)".formatted(MANY_OBJECTS_LIVE_COUNT));
        assertQuery(
                "SELECT count(*), count(DISTINCT key) FROM aws.s3.object_versions WHERE bucket_name = '%s'".formatted(MANY_OBJECTS_BUCKET),
                "VALUES (%s, %s)".formatted(MANY_OBJECTS_LIVE_COUNT + MANY_OBJECTS_DELETED_COUNT, MANY_OBJECTS_LIVE_COUNT + MANY_OBJECTS_DELETED_COUNT));
        assertQuery(
                "SELECT count(*), count(DISTINCT key) FROM aws.s3.deleted_objects WHERE bucket_name = '%s'".formatted(MANY_OBJECTS_BUCKET),
                "VALUES (%1$s, %1$s)".formatted(MANY_OBJECTS_DELETED_COUNT));
        assertQuery(
                "SELECT count(*) FROM (SELECT key FROM aws.s3.objects WHERE bucket_name = '%s' LIMIT 10)".formatted(MANY_OBJECTS_BUCKET),
                "VALUES 10");
    }

    @Test
    void testSingleColumnOfObjectsSpanningManyPages()
    {
        MaterializedResult keys = computeActual("SELECT key FROM aws.s3.objects WHERE bucket_name = '%s'".formatted(MANY_OBJECTS_BUCKET));

        assertThat(keys.getTypes()).containsExactly(VARCHAR);
        assertThat(keys.getRowCount()).isEqualTo(MANY_OBJECTS_LIVE_COUNT);
        assertThat(keys.getOnlyColumnAsSet()).hasSize(MANY_OBJECTS_LIVE_COUNT);
    }

    @Test
    void testObjects()
    {
        assertThat(query("SELECT bucket_name, key, size FROM aws.s3.objects WHERE bucket_name = '%s'".formatted(OBJECTS_BUCKET)))
                .matches("VALUES (VARCHAR '%1$s', VARCHAR 'a.txt', BIGINT '5'), (VARCHAR '%1$s', VARCHAR 'dir/b.txt', BIGINT '11')".formatted(OBJECTS_BUCKET));
    }

    @Test
    void testObjectVersions()
    {
        assertThat(query("SELECT key, size, is_latest FROM aws.s3.object_versions WHERE bucket_name = '%s'".formatted(VERSIONED_BUCKET)))
                .matches("VALUES (VARCHAR 'changed.txt', BIGINT '2', false), (VARCHAR 'changed.txt', BIGINT '9', true), (VARCHAR 'deleted.txt', BIGINT '4', false)");
    }

    @Test
    void testDeletedObjects()
    {
        assertThat(query("SELECT key, is_latest FROM aws.s3.deleted_objects WHERE bucket_name = '%s'".formatted(VERSIONED_BUCKET)))
                .matches("VALUES (VARCHAR 'deleted.txt', true)");
    }

    @Test
    void testWritesRejectedWhenReadOnly()
    {
        String instanceId = testData.instanceIds().getFirst();
        String expectedMessage = "This catalog is read-only, set aws.read-only=false to enable INSERT, UPDATE and DELETE";
        assertQueryFails("INSERT INTO aws.ec2.instances (image_id, instance_type) VALUES ('%s', '%s')".formatted(IMAGE_ID, INSTANCE_TYPE), expectedMessage);
        assertQueryFails("UPDATE aws.ec2.instances SET instance_type = 't3.small' WHERE instance_id = '%s'".formatted(instanceId), expectedMessage);
        assertQueryFails("DELETE FROM aws.ec2.instances WHERE instance_id = '%s'".formatted(instanceId), expectedMessage);
        assertQueryFails("DELETE FROM aws.ec2.images WHERE image_id = '%s'".formatted(IMAGE_ID), expectedMessage);
        assertQuery("SELECT count(*) FROM aws.ec2.instances WHERE subnet_id = '%s'".formatted(testData.subnetId()), "VALUES 2");
    }

    @Test
    void testObjectsInManyBuckets()
    {
        String sql = "SELECT bucket_name, key FROM aws.s3.objects WHERE bucket_name IN ('%s', '%s')".formatted(OBJECTS_BUCKET, VERSIONED_BUCKET);
        assertThat(query(sql))
                .matches("VALUES (VARCHAR '%1$s', VARCHAR 'a.txt'), (VARCHAR '%1$s', VARCHAR 'dir/b.txt'), (VARCHAR '%2$s', VARCHAR 'changed.txt')".formatted(OBJECTS_BUCKET, VERSIONED_BUCKET));
        assertSplitCount(sql, 2);
    }

    @Test
    void testObjectsInAllBuckets()
    {
        String sql = "SELECT bucket_name, count(*) FROM aws.s3.objects GROUP BY bucket_name";
        assertQuery(sql, "VALUES ('%s', 2), ('%s', 1), ('%s', %s)".formatted(OBJECTS_BUCKET, VERSIONED_BUCKET, MANY_OBJECTS_BUCKET, MANY_OBJECTS_LIVE_COUNT));
        assertSplitCount(sql, 3);
        assertQuery(
                "SELECT bucket_name, count(*) FROM aws.s3.object_versions GROUP BY bucket_name",
                "VALUES ('%s', 2), ('%s', 3), ('%s', %s)".formatted(OBJECTS_BUCKET, VERSIONED_BUCKET, MANY_OBJECTS_BUCKET, MANY_OBJECTS_LIVE_COUNT + MANY_OBJECTS_DELETED_COUNT));
        assertQuery(
                "SELECT bucket_name, count(*) FROM aws.s3.deleted_objects GROUP BY bucket_name",
                "VALUES ('%s', 1), ('%s', %s)".formatted(VERSIONED_BUCKET, MANY_OBJECTS_BUCKET, MANY_OBJECTS_DELETED_COUNT));
    }

    // predicates on the bucket name that allow NULL are not pushed down, so all buckets are listed, and Trino applies the predicates
    @Test
    void testObjectsWithBucketNameFiltersNotPushedDown()
    {
        assertQuery("SELECT count(*) FROM aws.s3.objects WHERE size > 0", "VALUES %s".formatted(3 + MANY_OBJECTS_LIVE_COUNT));
        assertQueryReturnsEmptyResult("SELECT * FROM aws.s3.objects WHERE bucket_name IS NULL");
        assertQuery(
                "SELECT key FROM aws.s3.objects WHERE bucket_name = '%s' OR bucket_name IS NULL".formatted(OBJECTS_BUCKET),
                "VALUES 'a.txt', 'dir/b.txt'");
    }

    @Test
    void testColumnComments()
    {
        assertThat(columnComment("ec2", "images", "instance_id")).isEqualTo("Only used by INSERT, always NULL when reading");
        assertThat(columnComment("ec2", "images", "image_id")).isEqualTo("");
        assertQuery(
                "SELECT comment FROM aws.information_schema.columns WHERE table_schema = 'ec2' AND table_name = 'images' AND column_name = 'instance_id'",
                "VALUES 'Only used by INSERT, always NULL when reading'");
    }

    @Test
    void testObjectsMetadataWithoutBucketName()
    {
        assertThat(query("DESCRIBE aws.s3.objects")).succeeds();
        assertThat(query("SHOW COLUMNS FROM aws.s3.objects")).succeeds();
        assertThat(query("EXPLAIN SELECT * FROM aws.s3.objects")).succeeds();
        assertQueryReturnsEmptyResult("SELECT * FROM aws.s3.objects LIMIT 0");
        assertQueryReturnsEmptyResult("SELECT * FROM aws.s3.objects WHERE false");
    }

    // DESCRIBE has the column name in the first field, and the comment in the last one
    private Object columnComment(String schema, String table, String column)
    {
        return computeActual("DESCRIBE aws.%s.%s".formatted(schema, table)).getMaterializedRows().stream()
                .filter(row -> row.getField(0).equals(column))
                .map(row -> row.getField(3))
                .collect(onlyElement());
    }

    private static String sqlList(List<String> values)
    {
        return values.stream()
                .map("'%s'"::formatted)
                .collect(joining(", "));
    }

    private void assertSplitCount(String sql, int expectedSplits)
    {
        assertQueryStats(getSession(), sql, stats -> assertThat(TableScanSplits.count(stats)).isEqualTo(expectedSplits), _ -> {});
    }
}
