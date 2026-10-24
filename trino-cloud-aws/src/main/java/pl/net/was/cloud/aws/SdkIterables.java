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

import com.google.common.collect.AbstractIterator;
import software.amazon.awssdk.core.exception.SdkException;

import java.util.Iterator;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

/**
 * Handles AWS SDK errors thrown while iterating, because paginated results only call AWS when the next page is needed.
 */
final class SdkIterables
{
    private SdkIterables() {}

    static <T> Iterable<T> translateErrors(Iterable<T> items)
    {
        return handleErrors(items, e -> {
            throw AwsErrors.translate(e);
        });
    }

    /**
     * The request name describes the failed request in the error message, like AWS request in region us-east-1.
     */
    static <T> Iterable<T> translateErrors(Iterable<T> items, String requestName)
    {
        requireNonNull(requestName, "requestName is null");
        return handleErrors(items, e -> {
            throw AwsErrors.translate(e, requestName);
        });
    }

    static <T> Iterable<T> emptyOnErrorCodes(Iterable<T> items, Set<String> errorCodes)
    {
        requireNonNull(errorCodes, "errorCodes is null");
        return handleErrors(items, e -> {
            if (!AwsErrors.hasErrorCode(e, errorCodes)) {
                throw e;
            }
        });
    }

    /**
     * Iterates the fallback instead when the items fail with one of the error codes before returning any of them.
     */
    static <T> Iterable<T> fallbackOnErrorCodes(Iterable<T> items, Set<String> errorCodes, Supplier<Iterable<T>> fallback)
    {
        requireNonNull(items, "items is null");
        requireNonNull(errorCodes, "errorCodes is null");
        requireNonNull(fallback, "fallback is null");
        return () -> new AbstractIterator<>()
        {
            private Iterator<T> delegate;
            private boolean canFallBack = true;

            @Override
            protected T computeNext()
            {
                while (true) {
                    try {
                        if (delegate == null) {
                            delegate = items.iterator();
                        }
                        if (!delegate.hasNext()) {
                            return endOfData();
                        }
                        T next = delegate.next();
                        canFallBack = false;
                        return next;
                    }
                    catch (SdkException e) {
                        if (!canFallBack || !AwsErrors.hasErrorCode(e, errorCodes)) {
                            throw e;
                        }
                        canFallBack = false;
                        delegate = fallback.get().iterator();
                    }
                }
            }
        };
    }

    private static <T> Iterable<T> handleErrors(Iterable<T> items, Consumer<SdkException> errorHandler)
    {
        requireNonNull(items, "items is null");
        return () -> new AbstractIterator<>()
        {
            private Iterator<T> delegate;

            @Override
            protected T computeNext()
            {
                try {
                    if (delegate == null) {
                        delegate = items.iterator();
                    }
                    if (delegate.hasNext()) {
                        return delegate.next();
                    }
                }
                catch (SdkException e) {
                    errorHandler.accept(e);
                }
                return endOfData();
            }
        };
    }
}
