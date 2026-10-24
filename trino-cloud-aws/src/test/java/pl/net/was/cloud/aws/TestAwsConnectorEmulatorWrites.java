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

import static pl.net.was.cloud.aws.EmulatorTestData.IMAGE_ID;
import static pl.net.was.cloud.aws.EmulatorTestData.IMAGE_OWNER_ID;

final class TestAwsConnectorEmulatorWrites
        extends AbstractTestQueryFramework
{
    private EmulatorTestData testData;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Floci emulator = closeAfterClass(EmulatorAwsQueryRunner.createEmulator());
        emulator.start();
        testData = EmulatorTestData.create(emulator);
        return EmulatorAwsQueryRunner.createQueryRunner(emulator, ImmutableMap.of("aws.read-only", "false"));
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
}
