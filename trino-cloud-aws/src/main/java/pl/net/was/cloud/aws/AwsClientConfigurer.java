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
import software.amazon.awssdk.regions.Region;

public class AwsClientConfigurer
{
    private final Region region;

    @Inject
    public AwsClientConfigurer(AwsConfig config)
    {
        this.region = Region.of(config.getRegion());
    }

    public <B extends AwsClientBuilder<B, ?>> B configure(B builder)
    {
        return builder.region(region);
    }
}
