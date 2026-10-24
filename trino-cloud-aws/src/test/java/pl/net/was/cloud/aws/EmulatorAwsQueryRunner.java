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
import io.airlift.log.Level;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.trino.Session;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.testing.containers.Floci;

import java.util.Map;

import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.containers.Floci.FLOCI_ACCESS_KEY;
import static io.trino.testing.containers.Floci.FLOCI_REGION;
import static io.trino.testing.containers.Floci.FLOCI_SECRET_KEY;
import static java.util.Objects.requireNonNullElse;

final class EmulatorAwsQueryRunner
{
    // Floci 2.2.0, the default in Trino's Floci container, tries to start a Docker container for every EC2 instance,
    // and without access to the Docker socket RunInstances hangs. Later versions emulate instances without Docker.
    private static final String FLOCI_IMAGE = "floci/floci:2.3.0";

    private EmulatorAwsQueryRunner() {}

    static Floci createEmulator()
    {
        Floci emulator = new Floci();
        emulator.setDockerImageName(FLOCI_IMAGE);
        return emulator;
    }

    static QueryRunner createQueryRunner(Floci emulator)
            throws Exception
    {
        return createQueryRunner(emulator, ImmutableMap.of());
    }

    private static QueryRunner createQueryRunner(Floci emulator, Map<String, String> coordinatorProperties)
            throws Exception
    {
        Session defaultSession = testSessionBuilder()
                .setCatalog("aws")
                .setSchema("ec2")
                .build();

        QueryRunner queryRunner = DistributedQueryRunner.builder(defaultSession)
                .setCoordinatorProperties(coordinatorProperties)
                .setWorkerCount(0)
                .build();
        try {
            queryRunner.installPlugin(new AwsPlugin());
            queryRunner.createCatalog("aws", "aws", ImmutableMap.of(
                    "aws.endpoint", emulator.endpoint().toString(),
                    "aws.region", FLOCI_REGION,
                    "aws.access-key", FLOCI_ACCESS_KEY,
                    "aws.secret-key", FLOCI_SECRET_KEY));
            return queryRunner;
        }
        catch (Throwable e) {
            queryRunner.close();
            throw e;
        }
    }

    static void main()
            throws Exception
    {
        Logging logger = Logging.initialize();
        logger.setLevel("pl.net.was", Level.DEBUG);
        logger.setLevel("io.trino", Level.INFO);

        Floci emulator = createEmulator();
        emulator.start();
        EmulatorTestData.create(emulator);
        QueryRunner queryRunner = createQueryRunner(emulator, ImmutableMap.of(
                "http-server.http.port", requireNonNullElse(System.getenv("TRINO_PORT"), "8082")));

        Logger log = Logger.get(EmulatorAwsQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", ((DistributedQueryRunner) queryRunner).getCoordinator().getBaseUrl());
    }
}
