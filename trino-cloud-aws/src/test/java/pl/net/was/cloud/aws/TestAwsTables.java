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

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;

import static org.assertj.core.api.Assertions.assertThat;

final class TestAwsTables
{
    @Test
    void testBucketLocationConstraint()
    {
        CreateBucketRequest request = CreateBucketRequest.builder().bucket("my-bucket").build();

        assertThat(AwsTables.withLocationConstraint(request, "us-east-1")).isEqualTo(request);
        assertThat(AwsTables.withLocationConstraint(request, "eu-central-1").createBucketConfiguration().locationConstraintAsString()).isEqualTo("eu-central-1");

        CreateBucketRequest withRegion = request.toBuilder()
                .createBucketConfiguration(configuration -> configuration.locationConstraint("eu-west-1"))
                .build();
        assertThat(AwsTables.withLocationConstraint(withRegion, "eu-central-1")).isEqualTo(withRegion);
    }
}
