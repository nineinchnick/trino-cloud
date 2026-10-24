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

import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.containers.Floci;
import org.junit.jupiter.api.Test;

import java.util.List;

import static java.util.stream.Collectors.joining;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static pl.net.was.cloud.aws.EmulatorTestData.IMAGE_ID;
import static pl.net.was.cloud.aws.EmulatorTestData.IMAGE_OWNER_ID;
import static pl.net.was.cloud.aws.EmulatorTestData.INSTANCE_TYPE;
import static pl.net.was.cloud.aws.EmulatorTestData.NAME;
import static pl.net.was.cloud.aws.EmulatorTestData.OBJECTS_BUCKET;
import static pl.net.was.cloud.aws.EmulatorTestData.VERSIONED_BUCKET;

final class TestAwsConnectorEmulator
        extends AbstractTestQueryFramework
{
    // Floci doesn't support DescribePlacementGroups, DescribePublicIpv4Pools and DescribeVpnConnections,
    // so placement_groups, public_ipv4_pools and vpn_connections are only covered by TestAwsQueriesRealAccount
    private static final List<String> EC2_TABLES_WITHOUT_REQUIRED_FILTERS = List.of(
            "availability_zones",
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
        for (String table : EC2_TABLES_WITHOUT_REQUIRED_FILTERS) {
            assertThat(query("SELECT * FROM aws.ec2." + table))
                    .as(table)
                    .succeeds();
        }
        assertThat(query("SELECT * FROM aws.ec2.images WHERE owner_id = '%s'".formatted(IMAGE_OWNER_ID))).succeeds();
        assertThat(query("SELECT * FROM aws.s3.buckets")).succeeds();
        assertThat(query("SELECT * FROM aws.s3.objects WHERE bucket_name = '%s'".formatted(OBJECTS_BUCKET))).succeeds();
        assertThat(query("SELECT * FROM aws.s3.object_versions WHERE bucket_name = '%s'".formatted(VERSIONED_BUCKET))).succeeds();
        assertThat(query("SELECT * FROM aws.s3.deleted_objects WHERE bucket_name = '%s'".formatted(VERSIONED_BUCKET))).succeeds();
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
                .isFullyPushedDown()
                .matches("VALUES (VARCHAR '%s', VARCHAR '%s')".formatted(instanceId, testData.subnetId()));
    }

    @Test
    void testMissingInstanceId()
    {
        assertQueryReturnsEmptyResult("SELECT instance_id FROM aws.ec2.instances WHERE instance_id = 'i-0123456789abcdef0'");
        assertQueryReturnsEmptyResult("SELECT instance_id FROM aws.ec2.instances WHERE instance_id = 'not-an-instance-id'");
    }

    @Test
    void testInstanceIdInList()
    {
        // TODO #288 IN lists should be pushed down and return both instances
        assertThatThrownBy(() -> computeActual("SELECT instance_id FROM aws.ec2.instances WHERE instance_id IN ('%s', '%s')".formatted(testData.instanceIds().get(0), testData.instanceIds().get(1))))
                .hasMessage("Domain is not a single value");
    }

    @Test
    void testInstanceTags()
    {
        assertQuery(
                "SELECT count(*) FROM aws.ec2.instances CROSS JOIN UNNEST(tags) AS t(tag) WHERE tag['Key'] = 'Name' AND tag['Value'] = '%s'".formatted(NAME),
                "VALUES 2");
    }

    @Test
    void testNullStringColumns()
    {
        // TODO #287 missing values should be NULL instead of empty strings
        assertQuery(
                "SELECT platform FROM aws.ec2.instances WHERE instance_id = '%s'".formatted(testData.instanceIds().getFirst()),
                "VALUES ''");
    }

    @Test
    void testImagesRequireFilterOrLimit()
    {
        assertQueryFails("SELECT * FROM aws.ec2.images", "Missing a limit or constraint for image_id, name, or owner_id");
        // TODO #16 the limit is never pushed down, so it doesn't help
        assertQueryFails("SELECT image_id FROM aws.ec2.images LIMIT 1", "Missing a limit or constraint for image_id, name, or owner_id");
    }

    // isFullyPushedDown() can't be used for tables with required filters, because it also runs the query with pushdown disabled,
    // but these queries only succeed when the filter is pushed down
    @Test
    void testImagesByOwner()
    {
        assertThat(query("SELECT image_id FROM aws.ec2.images WHERE owner_id = '%s'".formatted(IMAGE_OWNER_ID)))
                .matches("VALUES VARCHAR 'ami-0abcdef1234567890', VARCHAR 'ami-0abcdef1234567891', VARCHAR 'ami-amazonlinux2023-arm64'");
    }

    @Test
    void testImageById()
    {
        assertThat(query("SELECT name, owner_id FROM aws.ec2.images WHERE image_id = '%s'".formatted(IMAGE_ID)))
                .matches("VALUES (VARCHAR 'amzn2-ami-hvm-2.0.20230404.0-x86_64-gp2', VARCHAR '%s')".formatted(IMAGE_OWNER_ID));
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
        // TODO #287 the columns come from Tag instead of TagDescription, so value is the resource ID,
        // and resource_id and resource_type are missing
        assertQuery(
                "SELECT key, value FROM aws.ec2.tags WHERE value = '%s'".formatted(testData.vpcId()),
                "VALUES ('Name', '%s')".formatted(testData.vpcId()));
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
                "VALUES '%s', '%s'".formatted(OBJECTS_BUCKET, VERSIONED_BUCKET));
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
    void testObjectsRequireBucketName()
    {
        assertQueryFails("SELECT * FROM aws.s3.objects", "Missing required constraint for s3.objects.bucket_name");
        assertQueryFails("SELECT * FROM aws.s3.object_versions", "Missing required constraint for s3.object_versions.bucket_name");
        assertQueryFails("SELECT * FROM aws.s3.deleted_objects", "Missing required constraint for s3.deleted_objects.bucket_name");
    }
}
