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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import io.trino.spi.TrinoException;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.services.ec2.model.Ec2Exception;

import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static io.trino.spi.StandardErrorCode.INVALID_ARGUMENTS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static pl.net.was.cloud.aws.SdkIterables.emptyOnErrorCodes;
import static pl.net.was.cloud.aws.SdkIterables.fallbackOnErrorCodes;
import static pl.net.was.cloud.aws.SdkIterables.translateErrors;

final class TestSdkIterables
{
    @Test
    void testTranslateErrorsThrownWhileIterating()
    {
        Iterable<String> items = translateErrors(failingAfter(List.of("a"), "InvalidParameterValue"));

        assertThatThrownBy(() -> ImmutableList.copyOf(items))
                .isInstanceOfSatisfying(TrinoException.class, e -> assertThat(e.getErrorCode()).isEqualTo(INVALID_ARGUMENTS.toErrorCode()))
                .hasMessageContaining("InvalidParameterValue");
    }

    @Test
    void testEmptyOnErrorCodes()
    {
        assertThat(emptyOnErrorCodes(failingAfter(List.of(), "InvalidInstanceID.NotFound"), Set.of("InvalidInstanceID.NotFound")))
                .isEmpty();
        assertThatThrownBy(() -> ImmutableList.copyOf(emptyOnErrorCodes(failingAfter(List.of(), "InternalError"), Set.of("InvalidInstanceID.NotFound"))))
                .isInstanceOf(Ec2Exception.class);
    }

    @Test
    void testFallbackOnErrorCodes()
    {
        Set<String> errorCodes = Set.of("InvalidAMIID.NotFound");
        assertThat(fallbackOnErrorCodes(List.of("a", "b"), errorCodes, () -> List.of("c")))
                .containsExactly("a", "b");
        assertThat(fallbackOnErrorCodes(failingAfter(List.of(), "InvalidAMIID.NotFound"), errorCodes, () -> List.of("c")))
                .containsExactly("c");
        assertThatThrownBy(() -> ImmutableList.copyOf(fallbackOnErrorCodes(failingAfter(List.of(), "InternalError"), errorCodes, () -> List.of("c"))))
                .isInstanceOf(Ec2Exception.class);
        // falling back after returning some items would return them twice
        assertThatThrownBy(() -> ImmutableList.copyOf(fallbackOnErrorCodes(failingAfter(List.of("a"), "InvalidAMIID.NotFound"), errorCodes, () -> List.of("c"))))
                .isInstanceOf(Ec2Exception.class);
        assertThatThrownBy(() -> ImmutableList.copyOf(fallbackOnErrorCodes(failingAfter(List.of(), "InvalidAMIID.NotFound"), errorCodes, () -> failingAfter(List.of(), "InvalidAMIID.NotFound"))))
                .isInstanceOf(Ec2Exception.class);
    }

    @Test
    void testDelegateIsNotIteratedUntilNeeded()
    {
        AtomicInteger iterations = new AtomicInteger();
        Iterable<String> items = translateErrors(() -> {
            iterations.incrementAndGet();
            return List.of("a", "b").iterator();
        });

        assertThat(iterations).hasValue(0);
        assertThat(items.iterator().next()).isEqualTo("a");
        assertThat(iterations).hasValue(1);
    }

    private static Iterable<String> failingAfter(List<String> items, String errorCode)
    {
        return () -> Iterators.concat(items.iterator(), new Iterator<>()
        {
            @Override
            public boolean hasNext()
            {
                throw Ec2Exception.builder()
                        .statusCode(400)
                        .awsErrorDetails(AwsErrorDetails.builder()
                                .errorCode(errorCode)
                                .errorMessage("failed")
                                .build())
                        .build();
            }

            @Override
            public String next()
            {
                throw new UnsupportedOperationException();
            }
        });
    }
}
