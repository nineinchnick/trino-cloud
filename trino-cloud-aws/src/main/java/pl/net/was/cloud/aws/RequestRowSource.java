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
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import pl.net.was.cloud.aws.filters.TableFilters;
import pl.net.was.cloud.aws.filters.TableFilters.MissingValues;
import pl.net.was.cloud.aws.filters.TableFilters.Partition;
import software.amazon.awssdk.core.SdkField;
import software.amazon.awssdk.core.SdkPojo;
import software.amazon.awssdk.core.SdkRequest;
import software.amazon.awssdk.utils.builder.SdkBuilder;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;
import static pl.net.was.cloud.aws.SdkIterables.emptyOnErrorCodes;
import static pl.net.was.cloud.aws.SdkIterables.fallbackOnErrorCodes;
import static pl.net.was.cloud.aws.SdkIterables.translateErrors;

/**
 * Reads the rows of a table by sending a single AWS request, with the values of the pushed-down filters set by the column filters of the table.
 * The constraint of the table handle already includes the constraint of the split.
 */
public final class RequestRowSource<C, R extends SdkRequest>
        implements AwsTable.RowSource
{
    private final Function<Optional<Partition>, C> clients;
    private final Supplier<? extends SdkBuilder<?, R>> request;
    private final BiFunction<C, R, Iterable<? extends SdkPojo>> executor;
    private final List<SdkField<?>> requestFields;

    private RequestRowSource(Function<Optional<Partition>, C> clients, Supplier<? extends SdkBuilder<?, R>> request, BiFunction<C, R, Iterable<? extends SdkPojo>> executor)
    {
        this.clients = requireNonNull(clients, "clients is null");
        this.request = requireNonNull(request, "request is null");
        this.executor = requireNonNull(executor, "executor is null");
        this.requestFields = ImmutableList.copyOf(((SdkPojo) request.get()).sdkFields());
    }

    /**
     * The executor sends the request using the client, and returns the SDK objects of the rows.
     */
    public static <C, R extends SdkRequest> RequestRowSource<C, R> of(C client, Supplier<? extends SdkBuilder<?, R>> request, BiFunction<C, R, Iterable<? extends SdkPojo>> executor)
    {
        requireNonNull(client, "client is null");
        return new RequestRowSource<>(_ -> client, request, executor);
    }

    /**
     * The client is chosen by the value of the partition filter, which is a single value in every split.
     */
    public static <C, R extends SdkRequest> RequestRowSource<C, R> partitioned(Function<String, C> clients, Supplier<? extends SdkBuilder<?, R>> request, BiFunction<C, R, Iterable<? extends SdkPojo>> executor)
    {
        requireNonNull(clients, "clients is null");
        return new RequestRowSource<>(
                partition -> clients.apply(partition.orElseThrow(() -> new IllegalStateException("Scan without a single value of the partition column")).value()),
                request,
                executor);
    }

    @Override
    public Iterable<SdkRow> rows(AwsTable table, AwsTableHandle handle)
    {
        TableFilters filters = table.filters();
        TupleDomain<ColumnHandle> constraint = handle.constraint();
        if (constraint.isNone()) {
            return ImmutableList.of();
        }
        Optional<Partition> partition = filters.partition(constraint);
        C client = clients.apply(partition);
        Iterable<SdkPojo> objects = filters.missingValues(constraint)
                .map(missingValues -> skippingMissingValues(client, filters, constraint, missingValues))
                .orElseGet(() -> execute(client, filters, constraint));
        // names the partition, because a query can read several, and some errors, like failed authentication in a region, only happen in one of them
        String requestName = partition
                .map(value -> "AWS request in %s %s".formatted(value.column(), value.value()))
                .orElse("AWS request");
        Map<String, Object> requestValues = filters.requestValues(constraint);
        return Iterables.transform(translateErrors(objects, requestName), object -> new SdkRow(object, requestValues));
    }

    @Override
    public List<SdkField<?>> requestFields()
    {
        return requestFields;
    }

    private Iterable<SdkPojo> skippingMissingValues(C client, TableFilters filters, TupleDomain<ColumnHandle> constraint, MissingValues missingValues)
    {
        if (missingValues.values().size() == 1) {
            return emptyOnErrorCodes(execute(client, filters, constraint), missingValues.errorCodes());
        }
        return fallbackOnErrorCodes(
                execute(client, filters, constraint),
                missingValues.errorCodes(),
                () -> Iterables.concat(Lists.transform(
                        missingValues.values(),
                        value -> emptyOnErrorCodes(
                                execute(client, filters, constraint.intersect(TupleDomain.withColumnDomains(Map.of(missingValues.column(), Domain.singleValue(missingValues.column().type(), value))))),
                                missingValues.errorCodes()))));
    }

    // the request is only sent when the rows are iterated, so errors are handled while iterating
    private Iterable<SdkPojo> execute(C client, TableFilters filters, TupleDomain<ColumnHandle> constraint)
    {
        SdkBuilder<?, R> builder = request.get();
        filters.applyTo((SdkPojo) builder, constraint);
        R builtRequest = builder.build();
        return () -> Iterators.unmodifiableIterator(executor.apply(client, builtRequest).iterator());
    }
}
