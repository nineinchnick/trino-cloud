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

import com.google.common.collect.Iterables;
import io.airlift.slice.Slices;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorRecordSetProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.InMemoryRecordSet;
import io.trino.spi.connector.RecordSet;
import io.trino.spi.type.Type;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.DescribeInstancesRequest;
import software.amazon.awssdk.services.ec2.model.DescribeInstancesResponse;

import javax.inject.Inject;

import java.time.Instant;
import java.util.List;

import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_SECOND;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class AwsRecordSetProvider
        implements ConnectorRecordSetProvider
{
    private final AwsMetadata metadata;
    private final Ec2Client ec2;

    @Inject
    public AwsRecordSetProvider(AwsMetadata metadata, AwsConfig config)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        requireNonNull(config, "config is null");
        this.ec2 = Ec2Client.builder()
                .region(Region.of(config.getRegion()))
                .build();
    }

    @Override
    public RecordSet getRecordSet(
            ConnectorTransactionHandle connectorTransactionHandle,
            ConnectorSession connectorSession,
            ConnectorSplit connectorSplit,
            ConnectorTableHandle table,
            List<? extends ColumnHandle> list)
    {
        List<AwsColumnHandle> restColumnHandles = list
                .stream()
                .map(c -> (AwsColumnHandle) c)
                .collect(toList());
        ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(connectorSession, table);

        List<Integer> columnIndexes = restColumnHandles
                .stream()
                .map(column -> {
                    int index = 0;
                    for (ColumnMetadata columnMetadata : tableMetadata.getColumns()) {
                        if (columnMetadata.getName().equalsIgnoreCase(column.getName())) {
                            return index;
                        }
                        index++;
                    }
                    throw new IllegalStateException("Unknown column: " + column.getName());
                })
                .collect(toList());

        List<Type> mappedTypes = restColumnHandles
                .stream()
                .map(AwsColumnHandle::getType)
                .collect(toList());

        //noinspection StaticPseudoFunctionalStyleMethod
        Iterable<List<?>> mappedRows = Iterables.transform(
                getRows((AwsTableHandle) table),
                row -> columnIndexes
                        .stream()
                        .map(row::get)
                        .collect(toList()));

        return new InMemoryRecordSet(mappedTypes, mappedRows);
    }

    private Iterable<List<?>> getRows(AwsTableHandle table)
    {
        // TODO map table to object getter
        DescribeInstancesRequest request = DescribeInstancesRequest.builder().build();
        DescribeInstancesResponse response = ec2.describeInstances(request);
        return response.reservations()
                .stream()
                .flatMap(r -> r.instances()
                        .stream()
                        .map(i -> i.sdkFields()
                                .stream()
                                .map(f -> encode(f.getValueOrDefault(i)))
                                .collect(toList())))
                .collect(toList());
    }

    // must support all types from AwsMetadata.typeMap
    private static Object encode(Object o)
    {
        if (o instanceof String) {
            return Slices.utf8Slice((String) o);
        }
        if (o instanceof Instant) {
            return ((Instant) o).getEpochSecond() * NANOSECONDS_PER_SECOND;
        }
        if (o instanceof Number || o instanceof Boolean) {
            return o;
        }
        if (o instanceof List<?>) {
            List<?> list = (List<?>) o;
            BlockBuilder values = VARCHAR.createBlockBuilder(null, list.size());
            for (Object value : list) {
                VARCHAR.writeString(values, value.toString());
            }
            return values.build();
        }
        if (o == null) {
            return "";
        }
        return o.toString();
    }
}
