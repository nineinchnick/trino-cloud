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

import com.google.common.collect.ImmutableSet;
import io.trino.spi.ErrorCodeSupplier;
import io.trino.spi.TrinoException;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkException;

import java.util.Set;

import static io.trino.spi.StandardErrorCode.INVALID_ARGUMENTS;
import static io.trino.spi.StandardErrorCode.NOT_FOUND;
import static io.trino.spi.StandardErrorCode.PERMISSION_DENIED;
import static pl.net.was.cloud.aws.AwsErrorCode.AWS_API_ERROR;
import static pl.net.was.cloud.aws.AwsErrorCode.AWS_THROTTLED;

public final class AwsErrors
{
    private static final Set<String> ACCESS_DENIED_CODES = ImmutableSet.of(
            "AccessDenied",
            "AccessDeniedException",
            "UnauthorizedOperation",
            "AuthFailure",
            "InvalidClientTokenId",
            "SignatureDoesNotMatch",
            "ExpiredToken");
    private static final Set<String> NOT_FOUND_CODES = ImmutableSet.of(
            "NoSuchBucket");
    private static final Set<String> INVALID_REQUEST_CODES = ImmutableSet.of(
            "InvalidParameter",
            "InvalidParameterValue",
            "InvalidParameterCombination",
            "MissingParameter",
            "ValidationError",
            "InvalidRequest",
            "InvalidBucketName");
    // EC2 appends an encoded policy evaluation, which is long, and can only be decoded by someone with sts:DecodeAuthorizationMessage anyway
    private static final String ENCODED_AUTHORIZATION_MESSAGE = "Encoded authorization failure message:";

    private AwsErrors() {}

    public static TrinoException translate(SdkException exception)
    {
        return translate(exception, "AWS request");
    }

    /**
     * Names the region, because a query can read several regions, and some errors, like failed authentication
     * in a region that's not enabled for the account, only happen in one of them.
     */
    public static TrinoException translateInRegion(SdkException exception, String region)
    {
        return translate(exception, "AWS request in region " + region);
    }

    /**
     * The request name describes the failed request in the error message.
     */
    public static TrinoException translate(SdkException exception, String requestName)
    {
        if (!(exception instanceof AwsServiceException serviceException) || serviceException.awsErrorDetails() == null) {
            return new TrinoException(AWS_API_ERROR, "%s failed: %s".formatted(requestName, exception.getMessage()), exception);
        }
        AwsErrorDetails details = serviceException.awsErrorDetails();
        String message = "%s failed with %s (request ID %s): %s".formatted(
                requestName,
                details.errorCode(),
                serviceException.requestId(),
                stripEncodedAuthorizationMessage(details.errorMessage()));
        return new TrinoException(errorCode(serviceException), message, exception);
    }

    public static boolean hasErrorCode(SdkException exception, Set<String> errorCodes)
    {
        return exception instanceof AwsServiceException serviceException &&
                serviceException.awsErrorDetails() != null &&
                errorCodes.contains(serviceException.awsErrorDetails().errorCode());
    }

    private static ErrorCodeSupplier errorCode(AwsServiceException exception)
    {
        if (exception.isThrottlingException()) {
            return AWS_THROTTLED;
        }
        if (hasErrorCode(exception, ACCESS_DENIED_CODES)) {
            return PERMISSION_DENIED;
        }
        if (hasErrorCode(exception, NOT_FOUND_CODES)) {
            return NOT_FOUND;
        }
        if (hasErrorCode(exception, INVALID_REQUEST_CODES)) {
            return INVALID_ARGUMENTS;
        }
        return AWS_API_ERROR;
    }

    private static String stripEncodedAuthorizationMessage(String message)
    {
        if (message == null) {
            return "";
        }
        int index = message.indexOf(ENCODED_AUTHORIZATION_MESSAGE);
        if (index < 0) {
            return message;
        }
        return message.substring(0, index).strip();
    }
}
