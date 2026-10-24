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

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;
import static pl.net.was.cloud.aws.AwsClientConfigurer.knownRegion;

/**
 * EC2 is a regional service, so every region needs its own client. Clients are created when a region is first used,
 * and kept for the lifetime of the catalog, because every client has its own connection pool.
 */
public class Ec2Clients
        implements AutoCloseable
{
    private final AwsClientConfigurer clientConfigurer;
    private final String defaultRegion;
    private final Supplier<List<String>> regions;
    private final Map<String, Ec2Client> clients = new ConcurrentHashMap<>();

    @Inject
    public Ec2Clients(AwsClientConfigurer clientConfigurer, AwsConfig config)
    {
        this.clientConfigurer = requireNonNull(clientConfigurer, "clientConfigurer is null");
        this.defaultRegion = clientConfigurer.region().id();
        this.regions = configuredRegions(config.getRegions());
    }

    /**
     * The region set in aws.region.
     */
    public String defaultRegion()
    {
        return defaultRegion;
    }

    /**
     * The regions set in aws.regions, or the default region when it's not set. With *, the regions enabled for the account
     * are listed when they're first needed, and not listed again for the lifetime of the catalog.
     */
    public List<String> regions()
    {
        return regions.get();
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

    private Supplier<List<String>> configuredRegions(List<String> regionNames)
    {
        if (regionNames.isEmpty()) {
            return Suppliers.ofInstance(List.of(defaultRegion));
        }
        if (regionNames.equals(List.of(AwsConfig.ALL_REGIONS))) {
            // a failed request isn't memoized, so it's retried by the next query
            return Suppliers.memoize(this::enabledRegions);
        }
        List<String> regionIds = ImmutableSet.copyOf(regionNames).stream()
                .map(regionName -> knownRegion(regionName, "aws.regions").id())
                .collect(toImmutableList());
        return Suppliers.ofInstance(regionIds);
    }

    // without AllRegions, DescribeRegions only returns the regions enabled for the account
    private List<String> enabledRegions()
    {
        return defaultClient().describeRegions().regions().stream()
                .map(software.amazon.awssdk.services.ec2.model.Region::regionName)
                .sorted()
                .collect(toImmutableList());
    }

    private Ec2Client createClient(String region)
    {
        return clientConfigurer.configure(Ec2Client.builder())
                .region(Region.of(region))
                .build();
    }
}
