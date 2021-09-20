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
import io.airlift.configuration.ConfigSecuritySensitive;

public class AwsConfig
{
    private String accessKeyId;
    private String secretAccessKey;
    private String defaultRegion;

    public String getAccessKeyId()
    {
        return accessKeyId;
    }

    public String getSecretAccessKey()
    {
        return secretAccessKey;
    }

    public String getDefaultRegion()
    {
        return defaultRegion;
    }

    @Config("access_key_id")
    public AwsConfig setAccessKeyId(String accessKeyId)
    {
        this.accessKeyId = accessKeyId;
        return this;
    }

    @Config("secret_access_key")
    @ConfigSecuritySensitive
    public AwsConfig setSecretAccessKey(String secretAccessKey)
    {
        this.secretAccessKey = secretAccessKey;
        return this;
    }

    @Config("default_region")
    public AwsConfig setDefaultRegion(String defaultRegion)
    {
        this.defaultRegion = defaultRegion;
        return this;
    }
}
