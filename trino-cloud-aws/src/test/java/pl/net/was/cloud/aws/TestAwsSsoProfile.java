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
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.profiles.ProfileFile;

import java.io.ByteArrayInputStream;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestAwsSsoProfile
{
    @Test
    void testSsoProfileIsSupported()
    {
        String config =
                """
                [profile sso-test]
                sso_session = test-session
                sso_account_id = 123456789012
                sso_role_name = ReadOnly
                region = us-east-1

                [sso-session test-session]
                sso_start_url = https://example.awsapps.com/start
                sso_region = us-east-1
                """;
        ProfileFile profileFile = ProfileFile.builder()
                .content(new ByteArrayInputStream(config.getBytes(UTF_8)))
                .type(ProfileFile.Type.CONFIGURATION)
                .build();

        // without the sso modules, the SDK fails before looking for a cached SSO token
        try (ProfileCredentialsProvider provider = ProfileCredentialsProvider.builder()
                .profileFile(profileFile)
                .profileName("sso-test")
                .build()) {
            assertThatThrownBy(provider::resolveCredentials)
                    .hasMessageNotContaining("must be on the class path");
        }
    }
}
