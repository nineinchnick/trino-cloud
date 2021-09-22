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

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.trino.spi.NodeManager;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.type.TypeManager;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.s3.S3Client;

import javax.inject.Inject;
import javax.inject.Provider;

import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static java.util.Objects.requireNonNull;

public class AwsModule
        implements Module
{
    private final NodeManager nodeManager;
    private final TypeManager typeManager;

    public AwsModule(NodeManager nodeManager, TypeManager typeManager)
    {
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    @Override
    public void configure(Binder binder)
    {
        binder.bind(NodeManager.class).toInstance(nodeManager);
        binder.bind(TypeManager.class).toInstance(typeManager);

        binder.bind(AwsConnector.class).in(Scopes.SINGLETON);
        binder.bind(AwsMetadata.class).in(Scopes.SINGLETON);
        binder.bind(AwsSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(AwsRecordSetProvider.class).in(Scopes.SINGLETON);
        binder.bind(AwsPageSinkProvider.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(AwsConfig.class);

        binder.bind(Ec2Client.class).toProvider(Ec2ClientProvider.class);
        binder.bind(S3Client.class).toProvider(S3ClientProvider.class);
    }

    private static class Ec2ClientProvider
            implements Provider<Ec2Client>
    {
        private final String region;

        @Inject
        public Ec2ClientProvider(AwsConfig config)
        {
            requireNonNull(config, "config is null");
            this.region = config.getRegion();
        }

        @Override
        public Ec2Client get()
        {
            return Ec2Client.builder()
                    .region(Region.of(region))
                    .build();
        }
    }

    private static class S3ClientProvider
            implements Provider<S3Client>
    {
        private final String region;

        @Inject
        public S3ClientProvider(AwsConfig config)
        {
            requireNonNull(config, "config is null");
            this.region = config.getRegion();
        }

        @Override
        public S3Client get()
        {
            return S3Client.builder()
                    .region(Region.of(region))
                    .build();
        }
    }
}
