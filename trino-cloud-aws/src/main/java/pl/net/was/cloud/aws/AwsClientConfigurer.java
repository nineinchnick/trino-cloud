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
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.instrumentation.awssdk.v2_2.AwsSdkTelemetry;
import pl.net.was.cloud.aws.AwsConfig.RetryMode;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.awscore.client.builder.AwsClientBuilder;
import software.amazon.awssdk.awscore.client.builder.AwsSyncClientBuilder;
import software.amazon.awssdk.awscore.retry.AwsRetryStrategy;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.http.apache5.Apache5HttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain;
import software.amazon.awssdk.retries.api.RetryStrategy;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.utils.SdkAutoCloseable;

import java.net.URI;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.stream.Collectors.joining;

public class AwsClientConfigurer
        implements AutoCloseable
{
    private final Region region;
    private final Optional<URI> endpoint;
    private final int maxConnections;
    private final RetryStrategy retryStrategy;
    private final ExecutionInterceptor tracingInterceptor;
    private final Optional<StsClient> stsClient;
    private final AwsCredentialsProvider credentialsProvider;

    @Inject
    public AwsClientConfigurer(AwsConfig config, OpenTelemetry openTelemetry)
    {
        this.region = resolveRegion(config);
        this.endpoint = config.getEndpoint();
        this.maxConnections = config.getMaxConnections();
        this.retryStrategy = retryStrategy(config.getRetryMode()).toBuilder()
                .maxAttempts(config.getMaxErrorRetries() + 1)
                .build();
        this.tracingInterceptor = AwsSdkTelemetry.builder(openTelemetry)
                .setCaptureExperimentalSpanAttributes(true)
                .setRecordIndividualHttpError(true)
                .build()
                .createExecutionInterceptor();
        AwsCredentialsProvider sourceCredentialsProvider = createSourceCredentialsProvider(config);
        if (config.getIamRole().isEmpty()) {
            this.stsClient = Optional.empty();
            this.credentialsProvider = sourceCredentialsProvider;
            return;
        }
        StsClient stsClient = configureClient(StsClient.builder())
                .credentialsProvider(sourceCredentialsProvider)
                .build();
        this.stsClient = Optional.of(stsClient);
        this.credentialsProvider = StsAssumeRoleCredentialsProvider.builder()
                .refreshRequest(request -> request
                        .roleArn(config.getIamRole().get())
                        .roleSessionName(config.getRoleSessionName())
                        .externalId(config.getExternalId().orElse(null)))
                .stsClient(stsClient)
                .build();
    }

    public Region region()
    {
        return region;
    }

    public <B extends AwsClientBuilder<B, ?> & AwsSyncClientBuilder<B, ?>> B configure(B builder)
    {
        return configureClient(builder)
                .credentialsProvider(credentialsProvider);
    }

    @Override
    public void close()
    {
        try {
            if (credentialsProvider instanceof SdkAutoCloseable closeable) {
                closeable.close();
            }
        }
        finally {
            stsClient.ifPresent(StsClient::close);
        }
    }

    private static Region resolveRegion(AwsConfig config)
    {
        return knownRegion(config.getRegion().orElseGet(() -> defaultRegionName(config.getProfile())), "aws.region");
    }

    static Region knownRegion(String regionName, String propertyName)
    {
        Region region = Region.of(regionName);
        checkArgument(Region.regions().contains(region),
                "Unknown AWS region '%s', set %s to one of: %s",
                regionName,
                propertyName,
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

    private <B extends AwsClientBuilder<B, ?> & AwsSyncClientBuilder<B, ?>> B configureClient(B builder)
    {
        builder.region(region);
        endpoint.ifPresent(builder::endpointOverride);
        builder.httpClientBuilder(Apache5HttpClient.builder()
                .maxConnections(maxConnections));
        builder.overrideConfiguration(ClientOverrideConfiguration.builder()
                .retryStrategy(retryStrategy)
                .addExecutionInterceptor(tracingInterceptor)
                .build());
        return builder;
    }

    private static RetryStrategy retryStrategy(RetryMode retryMode)
    {
        return switch (retryMode) {
            case STANDARD -> AwsRetryStrategy.standardRetryStrategy();
            case LEGACY -> AwsRetryStrategy.legacyRetryStrategy();
            case ADAPTIVE -> AwsRetryStrategy.adaptiveRetryStrategy();
        };
    }
}
