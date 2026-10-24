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
import io.trino.testing.MaterializedRow;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.exception.SdkClientException;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

// Queries a real AWS account, so it only runs when explicitly enabled with TRINO_CLOUD_E2E=true.
// It must not depend on any specific resources existing in the account.
@EnabledIfEnvironmentVariable(named = "TRINO_CLOUD_E2E", matches = "true")
final class TestAwsQueriesRealAccount
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        assumeThat(hasAwsCredentials()).as("AWS credentials are available").isTrue();
        return AwsQueryRunner.createQueryRunner();
    }

    @Test
    void testShowTables()
    {
        assertQuery("SHOW SCHEMAS FROM aws", "VALUES 'ec2', 'information_schema', 's3'");
        assertQuery(
                "SHOW TABLES FROM aws.ec2",
                "VALUES 'availability_zones', 'images', 'instance_types', 'instances', 'key_pairs', 'launch_templates', 'nat_gateways', 'network_interfaces', 'placement_groups', 'prefix_lists', 'public_ipv4_pools', 'regions', 'route_tables', 'security_groups', 'snapshots', 'subnets', 'tags', 'volumes', 'vpc_endpoints', 'vpc_peering_connections', 'vpcs', 'vpn_connections', 'vpn_gateways'");
        assertQuery(
                "SHOW TABLES FROM aws.s3",
                "VALUES 'buckets', 'deleted_objects', 'objects', 'object_versions'");
    }

    @Test
    void testSelectInstanceById()
    {
        List<MaterializedRow> instances = computeActual("SELECT instance_id, instance_type FROM aws.ec2.instances LIMIT 1").getMaterializedRows();
        assumeThat(instances).as("instances in the account").isNotEmpty();
        String instanceId = (String) instances.getFirst().getField(0);
        String instanceType = (String) instances.getFirst().getField(1);

        assertQuery(
                "SELECT instance_type FROM aws.ec2.instances WHERE instance_id = '%s'".formatted(instanceId),
                "VALUES '%s'".formatted(instanceType));
    }

    @Test
    void testSelectBucketByName()
    {
        List<MaterializedRow> buckets = computeActual("SELECT name FROM aws.s3.buckets LIMIT 1").getMaterializedRows();
        assumeThat(buckets).as("buckets in the account").isNotEmpty();
        String bucketName = (String) buckets.getFirst().getField(0);

        assertQuery(
                "SELECT name FROM aws.s3.buckets WHERE name = '%s'".formatted(bucketName),
                "VALUES '%s'".formatted(bucketName));
        assertThat(query("SELECT key FROM aws.s3.objects WHERE bucket_name = '%s' LIMIT 10".formatted(bucketName)))
                .succeeds();
    }

    // Floci doesn't support the API calls behind these tables, so only this test covers them
    @Test
    void testTablesNotSupportedByEmulator()
    {
        assertThat(query("SELECT * FROM aws.ec2.placement_groups")).succeeds();
        assertThat(query("SELECT * FROM aws.ec2.public_ipv4_pools")).succeeds();
        assertThat(query("SELECT * FROM aws.ec2.vpn_connections")).succeeds();
    }

    private static boolean hasAwsCredentials()
    {
        try (DefaultCredentialsProvider credentialsProvider = DefaultCredentialsProvider.builder().build()) {
            credentialsProvider.resolveCredentials();
            return true;
        }
        catch (SdkClientException _) {
            return false;
        }
    }
}
