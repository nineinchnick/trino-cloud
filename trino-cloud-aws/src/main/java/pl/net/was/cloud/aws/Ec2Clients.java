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

import com.google.inject.Inject;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Objects.requireNonNull;

/**
 * EC2 is a regional service, so every region needs its own client. Clients are created when a region is first used,
 * and kept for the lifetime of the catalog, because every client has its own connection pool.
 */
public class Ec2Clients
        implements AutoCloseable
{
    private final AwsClientConfigurer clientConfigurer;
    private final String defaultRegion;
    private final Map<String, Ec2Client> clients = new ConcurrentHashMap<>();

    @Inject
    public Ec2Clients(AwsClientConfigurer clientConfigurer)
    {
        this.clientConfigurer = requireNonNull(clientConfigurer, "clientConfigurer is null");
        this.defaultRegion = clientConfigurer.region().id();
    }

    /**
     * The region set in aws.region.
     */
    public String defaultRegion()
    {
        return defaultRegion;
    }

    public Ec2Client defaultClient()
    {
        return client(defaultRegion);
    }

    public Ec2Client client(String region)
    {
        return clients.computeIfAbsent(region, this::createClient);
    }

    @Override
    public void close()
    {
        clients.values().forEach(Ec2Client::close);
    }

    private Ec2Client createClient(String region)
    {
        return clientConfigurer.configure(Ec2Client.builder())
                .region(Region.of(region))
                .build();
    }
}
