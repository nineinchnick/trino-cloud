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
import org.testng.annotations.Test;

public class TestAwsQueries
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return AwsQueryRunner.createQueryRunner();
    }

    @Test
    public void showTables()
    {
        assertQuery("SHOW SCHEMAS FROM aws", "VALUES 'ec2', 'information_schema'");
        assertQuery("SHOW TABLES FROM aws.ec2",
                "VALUES 'availability_zones', 'images', 'instance_types', 'instances', 'key_pairs', 'launch_templates', 'nat_gateways', 'network_interfaces', 'placement_groups', 'prefix_lists', 'public_ipv4_pools', 'regions', 'route_tables', 'security_groups', 'snapshots', 'subnets', 'tags', 'volumes', 'vpc_endpoints', 'vpc_peering_connections', 'vpcs', 'vpn_connections', 'vpn_gateways'");
    }

    @Test
    public void selectFromTable()
    {
        assertQuery("SELECT instance_type FROM aws.ec2.instances WHERE instance_id = 'i-03b6c688b1d220d2e'",
                "VALUES ('t2.micro')");
    }
}
