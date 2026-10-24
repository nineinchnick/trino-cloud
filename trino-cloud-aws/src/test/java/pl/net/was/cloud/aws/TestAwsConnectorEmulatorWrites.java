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
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.Ec2ClientBuilder;
import software.amazon.awssdk.services.ec2.model.InstanceType;

import java.util.concurrent.atomic.AtomicInteger;

import static io.trino.testing.containers.Floci.FLOCI_REGION;
import static org.assertj.core.api.Assertions.assertThat;
import static pl.net.was.cloud.aws.EmulatorTestData.IMAGE_ID;
import static pl.net.was.cloud.aws.EmulatorTestData.IMAGE_OWNER_ID;
import static pl.net.was.cloud.aws.EmulatorTestData.INSTANCE_TYPE;

final class TestAwsConnectorEmulatorWrites
        extends AbstractTestQueryFramework
{
    private static final int MAX_INSERTED_ROWS = 2;

    // each test creates instances in its own subnet, so tests don't see instances created by other tests
    private final AtomicInteger nextSubnet = new AtomicInteger(2);
    private EmulatorTestData testData;
    private Ec2Client ec2;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Floci emulator = closeAfterClass(EmulatorAwsQueryRunner.createEmulator());
        emulator.start();
        testData = EmulatorTestData.create(emulator);
        Ec2ClientBuilder ec2Builder = Ec2Client.builder();
        emulator.updateClient(ec2Builder);
        ec2 = closeAfterClass(ec2Builder.build());
        return EmulatorAwsQueryRunner.createQueryRunner(emulator, ImmutableMap.of(
                "aws.read-only", "false",
                "aws.max-inserted-rows", String.valueOf(MAX_INSERTED_ROWS)));
    }

    @Test
    void testInsertInstance()
    {
        String subnetId = createSubnet();
        assertUpdate("INSERT INTO aws.ec2.instances (image_id, instance_type, subnet_id) VALUES ('%s', '%s', '%s')".formatted(IMAGE_ID, INSTANCE_TYPE, subnetId), 1);
        assertQuery(
                "SELECT image_id, instance_type, vpc_id FROM aws.ec2.instances WHERE subnet_id = '%s'".formatted(subnetId),
                "VALUES ('%s', '%s', '%s')".formatted(IMAGE_ID, INSTANCE_TYPE, testData.vpcId()));
    }

    @Test
    void testInsertInstanceWithTags()
    {
        String subnetId = createSubnet();
        assertUpdate(
                "INSERT INTO aws.ec2.instances (image_id, instance_type, subnet_id, ebs_optimized, tags) VALUES ('%s', '%s', '%s', true, MAP(ARRAY['Name', 'Team'], ARRAY['inserted', 'data']))".formatted(IMAGE_ID, INSTANCE_TYPE, subnetId),
                1);
        String instanceId = (String) computeScalar("SELECT instance_id FROM aws.ec2.instances WHERE subnet_id = '%s'".formatted(subnetId));
        // Floci ignores ebs_optimized, so it only checks that a boolean column can be inserted
        assertQuery(
                "SELECT tags['Name'], tags['Team'] FROM aws.ec2.instances WHERE instance_id = '%s'".formatted(instanceId),
                "VALUES ('inserted', 'data')");
        assertQuery(
                "SELECT key, resource_type FROM aws.ec2.tags WHERE resource_id = '%s'".formatted(instanceId),
                "VALUES ('Name', 'instance'), ('Team', 'instance')");
    }

    @Test
    void testInsertBucket()
    {
        assertUpdate("INSERT INTO aws.s3.buckets (name) VALUES ('trino-inserted-bucket')", 1);
        assertQuery("SELECT count(*) FROM aws.s3.buckets WHERE name = 'trino-inserted-bucket'", "VALUES 1");

        assertQueryFails(
                "INSERT INTO aws.s3.buckets (name, creation_date) VALUES ('trino-other-bucket', now())",
                "Inserts to s3.buckets with creation_date are not supported");
        assertQuery("SELECT count(*) FROM aws.s3.buckets WHERE name = 'trino-other-bucket'", "VALUES 0");
    }

    @Test
    void testMaxInsertedRows()
    {
        String subnetId = createSubnet();
        String row = "('%s', '%s', '%s')".formatted(IMAGE_ID, INSTANCE_TYPE, subnetId);
        assertQueryFails(
                "INSERT INTO aws.ec2.instances (image_id, instance_type, subnet_id) VALUES %s, %s, %s".formatted(row, row, row),
                "Inserting more than %s rows into ec2.instances is not allowed, increase aws.max-inserted-rows to insert more".formatted(MAX_INSERTED_ROWS));
        assertQuery("SELECT count(*) FROM aws.ec2.instances WHERE subnet_id = '%s'".formatted(subnetId), "VALUES 0");

        assertUpdate("INSERT INTO aws.ec2.instances (image_id, instance_type, subnet_id) VALUES %s, %s".formatted(row, row), MAX_INSERTED_ROWS);
        assertQuery("SELECT count(*) FROM aws.ec2.instances WHERE subnet_id = '%s'".formatted(subnetId), "VALUES " + MAX_INSERTED_ROWS);
    }

    @Test
    void testDeleteInstance()
    {
        String subnetId = createSubnet();
        String instanceId = runInstance(subnetId);
        String otherInstanceId = runInstance(subnetId);

        assertUpdate("DELETE FROM aws.ec2.instances WHERE instance_id = '%s'".formatted(instanceId), 1);

        assertThat(instanceState(instanceId)).isIn("shutting-down", "terminated");
        assertThat(instanceState(otherInstanceId)).isEqualTo("running");
    }

    @Test
    void testUpdateState()
    {
        String subnetId = createSubnet();
        String instanceId = runInstance(subnetId);
        String otherInstanceId = runInstance(subnetId);

        assertUpdate("UPDATE aws.ec2.instances SET state = ROW(NULL, 'stopped') WHERE instance_id = '%s'".formatted(instanceId), 1);
        assertThat(instanceState(instanceId)).isEqualTo("stopped");
        assertThat(instanceState(otherInstanceId)).isEqualTo("running");

        assertUpdate("UPDATE aws.ec2.instances SET state = ROW(16, 'running') WHERE instance_id = '%s'".formatted(instanceId), 1);
        assertThat(instanceState(instanceId)).isEqualTo("running");

        assertQueryFails(
                "UPDATE aws.ec2.instances SET state = ROW(NULL, 'rebooting') WHERE instance_id = '%s'".formatted(instanceId),
                "Updates of ec2.instances.state column to rebooting are not supported");
        assertQueryFails(
                "UPDATE aws.ec2.instances SET state = ROW(80, NULL) WHERE instance_id = '%s'".formatted(instanceId),
                "Updates of ec2.instances.state column require a row with a name, like ROW\\(NULL, 'stopped'\\)");
        assertThat(instanceState(instanceId)).isEqualTo("running");
        assertThat(instanceState(otherInstanceId)).isEqualTo("running");
    }

    @Test
    void testUpdateInstanceType()
    {
        String subnetId = createSubnet();
        String instanceId = runInstance(subnetId);
        ec2.stopInstances(request -> request.instanceIds(instanceId));
        assertThat(instanceState(instanceId)).isEqualTo("stopped");

        assertUpdate("UPDATE aws.ec2.instances SET instance_type = 't3.small' WHERE instance_id = '%s'".formatted(instanceId), 1);

        assertQuery(
                "SELECT instance_type, state.name FROM aws.ec2.instances WHERE instance_id = '%s'".formatted(instanceId),
                "VALUES ('t3.small', 'stopped')");
    }

    @Test
    void testUpdateUnsupportedColumns()
    {
        String instanceId = testData.instanceIds().getFirst();
        assertQueryFails(
                "UPDATE aws.ec2.instances SET subnet_id = 'subnet-123' WHERE instance_id = '%s'".formatted(instanceId),
                "Updating subnet_id in ec2.instances is not supported, only these columns can be updated: instance_type, state");
        assertQueryFails(
                "UPDATE aws.ec2.instances SET instance_type = 't3.small', key_name = 'other' WHERE instance_id = '%s'".formatted(instanceId),
                "Updating key_name in ec2.instances is not supported, only these columns can be updated: instance_type, state");
        assertQueryFails(
                "UPDATE aws.ec2.images SET name = 'other' WHERE image_id = '%s'".formatted(IMAGE_ID),
                "Updates are not supported for ec2.images");
        assertQueryFails(
                "UPDATE aws.ec2.instances SET instance_type = NULL WHERE instance_id = '%s'".formatted(instanceId),
                "Updating instance_type in ec2.instances to NULL is not supported");

        assertQuery(
                "SELECT instance_type, subnet_id FROM aws.ec2.instances WHERE instance_id = '%s'".formatted(instanceId),
                "VALUES ('%s', '%s')".formatted(INSTANCE_TYPE, testData.subnetId()));
    }

    @Test
    void testUpdateAndDeleteRequirePrimaryKeyFilter()
    {
        String subnetFilter = "subnet_id = '%s'".formatted(testData.subnetId());
        String instancesMessage = "Deletes and updates on ec2.instances require a filter on instance_id, like WHERE instance_id = '...'";
        assertQueryFails("DELETE FROM aws.ec2.instances", instancesMessage);
        assertQueryFails("DELETE FROM aws.ec2.instances WHERE " + subnetFilter, instancesMessage);
        assertQueryFails("UPDATE aws.ec2.instances SET instance_type = 't3.small'", instancesMessage);
        assertQueryFails("UPDATE aws.ec2.instances SET instance_type = 't3.small' WHERE " + subnetFilter, instancesMessage);
        assertQueryFails(
                "DELETE FROM aws.ec2.images WHERE owner_id = '%s'".formatted(IMAGE_OWNER_ID),
                "Deletes and updates on ec2.images require a filter on image_id, like WHERE image_id = '...'");

        assertQuery("SELECT count(*) FROM aws.ec2.instances WHERE %s AND state.name = 'running'".formatted(subnetFilter), "VALUES 2");
        assertQuery("SELECT count(*) FROM aws.ec2.images WHERE image_id = '%s'".formatted(IMAGE_ID), "VALUES 1");
    }

    private String runInstance(String subnetId)
    {
        return ec2.runInstances(request -> request
                        .imageId(IMAGE_ID)
                        .instanceType(InstanceType.fromValue(INSTANCE_TYPE))
                        .minCount(1)
                        .maxCount(1)
                        .subnetId(subnetId))
                .instances()
                .getFirst()
                .instanceId();
    }

    private String instanceState(String instanceId)
    {
        return (String) computeScalar("SELECT state.name FROM aws.ec2.instances WHERE instance_id = '%s'".formatted(instanceId));
    }

    private String createSubnet()
    {
        return ec2.createSubnet(request -> request
                        .vpcId(testData.vpcId())
                        .cidrBlock("10.0.%s.0/24".formatted(nextSubnet.getAndIncrement()))
                        .availabilityZone(FLOCI_REGION + "a"))
                .subnet()
                .subnetId();
    }
}
