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

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;
import io.airlift.configuration.LegacyConfig;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import java.net.URI;
import java.util.Optional;

public class AwsConfig
{
    private String region;
    private URI endpoint;
    private String accessKey;
    private String secretKey;
    private String profile;
    private String iamRole;
    private String externalId;
    private String roleSessionName = "trino-cloud";
    private int maxConnections = 50;
    private RetryMode retryMode = RetryMode.ADAPTIVE;
    private int maxErrorRetries = 10;
    private boolean readOnly = true;

    public Optional<String> getRegion()
    {
        return Optional.ofNullable(region);
    }

    @Config("aws.region")
    @LegacyConfig("region")
    @ConfigDescription("AWS region of the EC2 and S3 endpoints, defaults to the region resolved by the AWS SDK default region provider chain")
    public AwsConfig setRegion(String region)
    {
        this.region = region;
        return this;
    }

    public Optional<URI> getEndpoint()
    {
        return Optional.ofNullable(endpoint);
    }

    @Config("aws.endpoint")
    @ConfigDescription("Endpoint URL overriding the AWS service endpoints, for example to use an emulator; enables S3 path-style access")
    public AwsConfig setEndpoint(URI endpoint)
    {
        this.endpoint = endpoint;
        return this;
    }

    public Optional<String> getAccessKey()
    {
        return Optional.ofNullable(accessKey);
    }

    @Config("aws.access-key")
    @ConfigDescription("AWS access key ID, used together with aws.secret-key instead of the AWS SDK default credentials provider chain")
    public AwsConfig setAccessKey(String accessKey)
    {
        this.accessKey = accessKey;
        return this;
    }

    public Optional<String> getSecretKey()
    {
        return Optional.ofNullable(secretKey);
    }

    @Config("aws.secret-key")
    @ConfigSecuritySensitive
    @ConfigDescription("AWS secret access key, used together with aws.access-key")
    public AwsConfig setSecretKey(String secretKey)
    {
        this.secretKey = secretKey;
        return this;
    }

    public Optional<String> getProfile()
    {
        return Optional.ofNullable(profile);
    }

    @Config("aws.profile")
    @ConfigDescription("Name of a profile from the AWS shared configuration and credentials files")
    public AwsConfig setProfile(String profile)
    {
        this.profile = profile;
        return this;
    }

    public Optional<String> getIamRole()
    {
        return Optional.ofNullable(iamRole);
    }

    @Config("aws.iam-role")
    @ConfigDescription("ARN of an IAM role to assume using STS")
    public AwsConfig setIamRole(String iamRole)
    {
        this.iamRole = iamRole;
        return this;
    }

    public Optional<String> getExternalId()
    {
        return Optional.ofNullable(externalId);
    }

    @Config("aws.external-id")
    @ConfigDescription("External ID required by the trust policy of the role set in aws.iam-role")
    public AwsConfig setExternalId(String externalId)
    {
        this.externalId = externalId;
        return this;
    }

    @NotBlank
    public String getRoleSessionName()
    {
        return roleSessionName;
    }

    @Config("aws.role-session-name")
    @ConfigDescription("Session name used when assuming the role set in aws.iam-role")
    public AwsConfig setRoleSessionName(String roleSessionName)
    {
        this.roleSessionName = roleSessionName;
        return this;
    }

    @Min(1)
    public int getMaxConnections()
    {
        return maxConnections;
    }

    @Config("aws.max-connections")
    @ConfigDescription("Maximum number of concurrent connections to each AWS service")
    public AwsConfig setMaxConnections(int maxConnections)
    {
        this.maxConnections = maxConnections;
        return this;
    }

    @NotNull
    public RetryMode getRetryMode()
    {
        return retryMode;
    }

    @Config("aws.retry-mode")
    @ConfigDescription("AWS SDK retry mode used when requests fail or are throttled")
    public AwsConfig setRetryMode(RetryMode retryMode)
    {
        this.retryMode = retryMode;
        return this;
    }

    @Min(0)
    public int getMaxErrorRetries()
    {
        return maxErrorRetries;
    }

    @Config("aws.max-error-retries")
    @ConfigDescription("Maximum number of retries of a failed AWS request")
    public AwsConfig setMaxErrorRetries(int maxErrorRetries)
    {
        this.maxErrorRetries = maxErrorRetries;
        return this;
    }

    public boolean isReadOnly()
    {
        return readOnly;
    }

    @Config("aws.read-only")
    @ConfigDescription("Reject INSERT, UPDATE and DELETE statements, which create, modify and remove AWS resources")
    public AwsConfig setReadOnly(boolean readOnly)
    {
        this.readOnly = readOnly;
        return this;
    }

    @AssertTrue(message = "aws.access-key and aws.secret-key must be set together")
    public boolean isStaticCredentialsComplete()
    {
        return (accessKey == null) == (secretKey == null);
    }

    @AssertTrue(message = "aws.profile cannot be set together with aws.access-key and aws.secret-key")
    public boolean isProfileExclusiveWithStaticCredentials()
    {
        return profile == null || (accessKey == null && secretKey == null);
    }

    @AssertTrue(message = "aws.external-id can only be set together with aws.iam-role")
    public boolean isExternalIdUsedWithIamRole()
    {
        return externalId == null || iamRole != null;
    }

    public enum RetryMode
    {
        STANDARD,
        LEGACY,
        ADAPTIVE,
    }
}
