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
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.containers.Floci;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.Ec2ClientBuilder;
import software.amazon.awssdk.services.ec2.model.Instance;
import software.amazon.awssdk.services.ec2.model.InstanceStateName;
import software.amazon.awssdk.services.ec2.model.InstanceType;
import software.amazon.awssdk.services.ec2.model.ResourceType;
import software.amazon.awssdk.services.ec2.model.RunInstancesRequest;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.ec2.model.TagSpecification;

import static io.trino.testing.containers.Floci.FLOCI_REGION;
import static org.assertj.core.api.Assertions.assertThat;
import static pl.net.was.cloud.aws.EmulatorTestData.IMAGE_ID;
import static pl.net.was.cloud.aws.EmulatorTestData.INSTANCE_TYPE;

final class TestAwsConnectorEmulatorRegions
        extends AbstractTestQueryFramework
{
    private static final String OTHER_REGION = "eu-west-1";
    // instances are removed in a region without other instances, so they don't change the results of other tests
    private static final String DELETE_REGION = "eu-central-1";
    private static final String TAGGED = "element_at(tags, 'Name') = 'trino-test-regions'";

    private Floci emulator;
    private String defaultRegionInstanceId;
    private String otherRegionInstanceId;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        emulator = closeAfterClass(EmulatorAwsQueryRunner.createEmulator());
        emulator.start();
        try (Ec2Client defaultRegionEc2 = ec2Client(FLOCI_REGION);
                Ec2Client otherRegionEc2 = ec2Client(OTHER_REGION)) {
            defaultRegionInstanceId = runInstance(defaultRegionEc2, true);
            otherRegionInstanceId = runInstance(otherRegionEc2, true);
        }
        QueryRunner queryRunner = EmulatorAwsQueryRunner.createQueryRunner(emulator, ImmutableMap.of(
                "aws.regions", String.join(",", FLOCI_REGION, OTHER_REGION, DELETE_REGION),
                "aws.read-only", "false"));
        queryRunner.createCatalog("aws_default_region", "aws", EmulatorAwsQueryRunner.connectionProperties(emulator));
        queryRunner.createCatalog("aws_all_regions", "aws", ImmutableMap.<String, String>builder()
                .putAll(EmulatorAwsQueryRunner.connectionProperties(emulator))
                .put("aws.regions", "*")
                .buildOrThrow());
        return queryRunner;
    }

    @Test
    void testInstancesInAllRegions()
    {
        String sql = "SELECT region, instance_id FROM aws.ec2.instances WHERE " + TAGGED;
        assertThat(query(sql)).matches(bothInstances());
        assertSplitCount(sql, 3);
    }

    @Test
    void testInstanceIdsInAllRegions()
    {
        String sql = "SELECT region, instance_id FROM aws.ec2.instances WHERE instance_id IN ('%s', '%s')".formatted(defaultRegionInstanceId, otherRegionInstanceId);
        assertThat(query(sql)).matches(bothInstances());
        assertSplitCount(sql, 3);
    }

    @Test
    void testRegionFilter()
    {
        String sql = "SELECT region, instance_id FROM aws.ec2.instances WHERE region = '%s'".formatted(OTHER_REGION);
        assertThat(query(sql))
                .matches("VALUES (VARCHAR '%s', VARCHAR '%s')".formatted(OTHER_REGION, otherRegionInstanceId))
                .isFullyPushedDown();
        assertSplitCount(sql, 1);

        assertThat(query("SELECT region, instance_id FROM aws.ec2.instances WHERE region IN ('%s', '%s')".formatted(FLOCI_REGION, OTHER_REGION)))
                .matches(bothInstances())
                .isFullyPushedDown();
    }

    @Test
    void testRegionNotConfigured()
    {
        assertThat(query("SELECT region, instance_id FROM aws.ec2.instances WHERE region = 'ap-south-1'")).returnsEmptyResult();
    }

    @Test
    void testDefaultRegionOnly()
    {
        assertThat(query("SELECT region, instance_id FROM aws_default_region.ec2.instances WHERE " + TAGGED))
                .matches("VALUES (VARCHAR '%s', VARCHAR '%s')".formatted(FLOCI_REGION, defaultRegionInstanceId));
    }

    @Test
    void testAllRegions()
    {
        assertThat(query("SELECT region, instance_id FROM aws_all_regions.ec2.instances WHERE " + TAGGED))
                .matches(bothInstances());
        // every region enabled for the account is read in a separate split
        assertThat((long) computeScalar("SELECT count(DISTINCT region) FROM aws_all_regions.ec2.availability_zones"))
                .isEqualTo((long) computeScalar("SELECT count(*) FROM aws_all_regions.ec2.regions"));
    }

    @Test
    void testDeleteInOtherRegion()
    {
        String instanceId;
        try (Ec2Client deleteRegionEc2 = ec2Client(DELETE_REGION)) {
            instanceId = runInstance(deleteRegionEc2, false);
            assertUpdate("DELETE FROM aws.ec2.instances WHERE instance_id = '%s'".formatted(instanceId), 1);
            Instance instance = deleteRegionEc2.describeInstances(request -> request.instanceIds(instanceId))
                    .reservations().getFirst()
                    .instances().getFirst();
            assertThat(instance.state().name()).isIn(InstanceStateName.SHUTTING_DOWN, InstanceStateName.TERMINATED);
        }
    }

    @Test
    void testInsertInOtherRegion()
    {
        assertQueryFails(
                "INSERT INTO aws.ec2.instances (region, image_id, instance_type) VALUES ('%s', '%s', '%s')".formatted(OTHER_REGION, IMAGE_ID, INSTANCE_TYPE),
                "Resources can only be created in the region set in aws.region, %s, not in %s".formatted(FLOCI_REGION, OTHER_REGION));
    }

    private String bothInstances()
    {
        return "VALUES (VARCHAR '%s', VARCHAR '%s'), (VARCHAR '%s', VARCHAR '%s')".formatted(
                FLOCI_REGION,
                defaultRegionInstanceId,
                OTHER_REGION,
                otherRegionInstanceId);
    }

    private Ec2Client ec2Client(String region)
    {
        Ec2ClientBuilder builder = Ec2Client.builder();
        emulator.updateClient(builder);
        return builder.region(Region.of(region)).build();
    }

    private static String runInstance(Ec2Client ec2, boolean tagged)
    {
        RunInstancesRequest.Builder request = RunInstancesRequest.builder()
                .imageId(IMAGE_ID)
                .instanceType(InstanceType.fromValue(INSTANCE_TYPE))
                .minCount(1)
                .maxCount(1);
        if (tagged) {
            request.tagSpecifications(TagSpecification.builder()
                    .resourceType(ResourceType.INSTANCE)
                    .tags(Tag.builder().key("Name").value("trino-test-regions").build())
                    .build());
        }
        return ec2.runInstances(request.build())
                .instances()
                .getFirst()
                .instanceId();
    }

    private void assertSplitCount(String sql, int expectedSplits)
    {
        assertQueryStats(getSession(), sql, stats -> assertThat(TableScanSplits.count(stats)).isEqualTo(expectedSplits), _ -> {});
    }
}
