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
import software.amazon.awssdk.awscore.client.builder.AwsClientBuilder;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain;

import java.net.URI;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.stream.Collectors.joining;

public class AwsClientConfigurer
{
    private final Region region;
    private final Optional<URI> endpoint;

    @Inject
    public AwsClientConfigurer(AwsConfig config)
    {
        this.region = resolveRegion(config);
        this.endpoint = config.getEndpoint();
    }

    public <B extends AwsClientBuilder<B, ?>> B configure(B builder)
    {
        builder.region(region);
        endpoint.ifPresent(builder::endpointOverride);
        return builder;
    }

    private static Region resolveRegion(AwsConfig config)
    {
        String regionName = config.getRegion().orElseGet(AwsClientConfigurer::defaultRegionName);
        Region region = Region.of(regionName);
        checkArgument(Region.regions().contains(region),
                "Unknown AWS region '%s' in aws.region, expected one of: %s",
                regionName,
                Region.regions().stream()
                        .map(Region::id)
                        .sorted()
                        .collect(joining(", ")));
        return region;
    }

    private static String defaultRegionName()
    {
        try {
            return DefaultAwsRegionProviderChain.builder()
                    .build()
                    .getRegion()
                    .id();
        }
        catch (SdkClientException e) {
            throw new IllegalStateException("aws.region is not set and the AWS SDK default region provider chain could not resolve a region", e);
        }
    }
}
