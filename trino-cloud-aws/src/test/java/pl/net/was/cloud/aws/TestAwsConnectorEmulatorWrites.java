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

import java.util.concurrent.atomic.AtomicInteger;

import static io.trino.testing.containers.Floci.FLOCI_REGION;
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

        assertQuery("SELECT count(*) FROM aws.ec2.instances WHERE %s AND state['Name'] = 'running'".formatted(subnetFilter), "VALUES 2");
        assertQuery("SELECT count(*) FROM aws.ec2.images WHERE image_id = '%s'".formatted(IMAGE_ID), "VALUES 1");
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
