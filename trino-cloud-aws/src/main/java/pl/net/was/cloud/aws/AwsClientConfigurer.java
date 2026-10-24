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
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.awscore.client.builder.AwsClientBuilder;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;

import java.net.URI;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.stream.Collectors.joining;

public class AwsClientConfigurer
{
    private final Region region;
    private final Optional<URI> endpoint;
    private final AwsCredentialsProvider credentialsProvider;

    @Inject
    public AwsClientConfigurer(AwsConfig config)
    {
        this.region = resolveRegion(config);
        this.endpoint = config.getEndpoint();
        this.credentialsProvider = createCredentialsProvider(config, region, endpoint);
    }

    public <B extends AwsClientBuilder<B, ?>> B configure(B builder)
    {
        return configureRegionAndEndpoint(builder, region, endpoint)
                .credentialsProvider(credentialsProvider);
    }

    private static Region resolveRegion(AwsConfig config)
    {
        String regionName = config.getRegion().orElseGet(() -> defaultRegionName(config.getProfile()));
        Region region = Region.of(regionName);
        checkArgument(Region.regions().contains(region),
                "Unknown AWS region '%s', set aws.region to one of: %s",
                regionName,
                Region.regions().stream()
                        .map(Region::id)
                        .sorted()
                        .collect(joining(", ")));
        return region;
    }

    private static String defaultRegionName(Optional<String> profile)
    {
        try {
            return DefaultAwsRegionProviderChain.builder()
                    .profileName(profile.orElse(null))
                    .build()
                    .getRegion()
                    .id();
        }
        catch (SdkClientException e) {
            throw new IllegalStateException("aws.region is not set and the AWS SDK default region provider chain could not resolve a region", e);
        }
    }

    private static AwsCredentialsProvider createCredentialsProvider(AwsConfig config, Region region, Optional<URI> endpoint)
    {
        AwsCredentialsProvider sourceCredentialsProvider = createSourceCredentialsProvider(config);
        if (config.getIamRole().isEmpty()) {
            return sourceCredentialsProvider;
        }
        StsClient stsClient = configureRegionAndEndpoint(StsClient.builder(), region, endpoint)
                .credentialsProvider(sourceCredentialsProvider)
                .build();
        return StsAssumeRoleCredentialsProvider.builder()
                .refreshRequest(request -> request
                        .roleArn(config.getIamRole().get())
                        .roleSessionName(config.getRoleSessionName())
                        .externalId(config.getExternalId().orElse(null)))
                .stsClient(stsClient)
                .build();
    }

    private static AwsCredentialsProvider createSourceCredentialsProvider(AwsConfig config)
    {
        if (config.getAccessKey().isPresent() && config.getSecretKey().isPresent()) {
            return StaticCredentialsProvider.create(AwsBasicCredentials.create(config.getAccessKey().get(), config.getSecretKey().get()));
        }
        if (config.getProfile().isPresent()) {
            return ProfileCredentialsProvider.builder()
                    .profileName(config.getProfile().get())
                    .build();
        }
        return DefaultCredentialsProvider.builder().build();
    }

    private static <B extends AwsClientBuilder<B, ?>> B configureRegionAndEndpoint(B builder, Region region, Optional<URI> endpoint)
    {
        builder.region(region);
        endpoint.ifPresent(builder::endpointOverride);
        return builder;
    }
}
