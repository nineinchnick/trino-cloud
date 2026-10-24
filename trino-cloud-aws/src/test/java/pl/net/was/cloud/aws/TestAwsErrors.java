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

import io.trino.spi.TrinoException;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.ec2.model.Ec2Exception;

import static io.trino.spi.StandardErrorCode.INVALID_ARGUMENTS;
import static io.trino.spi.StandardErrorCode.NOT_FOUND;
import static io.trino.spi.StandardErrorCode.PERMISSION_DENIED;
import static org.assertj.core.api.Assertions.assertThat;
import static pl.net.was.cloud.aws.AwsErrorCode.AWS_API_ERROR;
import static pl.net.was.cloud.aws.AwsErrorCode.AWS_THROTTLED;

final class TestAwsErrors
{
    @Test
    void testAccessDenied()
    {
        TrinoException exception = AwsErrors.translate(ec2Exception(403, "UnauthorizedOperation", "You are not authorized to perform this operation. Encoded authorization failure message: abc123"));

        assertThat(exception.getErrorCode()).isEqualTo(PERMISSION_DENIED.toErrorCode());
        assertThat(exception)
                .hasMessage("AWS request failed with UnauthorizedOperation (request ID request-1): You are not authorized to perform this operation.")
                .hasMessageNotContaining("abc123");
    }

    @Test
    void testThrottling()
    {
        assertThat(AwsErrors.translate(ec2Exception(503, "RequestLimitExceeded", "Request limit exceeded.")).getErrorCode())
                .isEqualTo(AWS_THROTTLED.toErrorCode());
    }

    @Test
    void testInvalidRequest()
    {
        assertThat(AwsErrors.translate(ec2Exception(400, "InvalidParameterValue", "Invalid value")).getErrorCode())
                .isEqualTo(INVALID_ARGUMENTS.toErrorCode());
    }

    @Test
    void testNotFound()
    {
        assertThat(AwsErrors.translate(ec2Exception(404, "NoSuchBucket", "The specified bucket does not exist")).getErrorCode())
                .isEqualTo(NOT_FOUND.toErrorCode());
    }

    @Test
    void testOtherServiceError()
    {
        assertThat(AwsErrors.translate(ec2Exception(500, "InternalError", "An internal error has occurred")).getErrorCode())
                .isEqualTo(AWS_API_ERROR.toErrorCode());
    }

    @Test
    void testClientError()
    {
        TrinoException exception = AwsErrors.translate(SdkClientException.create("Unable to execute HTTP request"));

        assertThat(exception.getErrorCode()).isEqualTo(AWS_API_ERROR.toErrorCode());
        assertThat(exception).hasMessage("AWS request failed: Unable to execute HTTP request");
    }

    private static Ec2Exception ec2Exception(int statusCode, String errorCode, String message)
    {
        return (Ec2Exception) Ec2Exception.builder()
                .statusCode(statusCode)
                .requestId("request-1")
                .awsErrorDetails(AwsErrorDetails.builder()
                        .errorCode(errorCode)
                        .errorMessage(message)
                        .build())
                .build();
    }
}
