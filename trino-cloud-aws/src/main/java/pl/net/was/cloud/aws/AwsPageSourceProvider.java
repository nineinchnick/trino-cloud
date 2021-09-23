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
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.airlift.slice.Slice;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.SingleMapBlock;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.RecordPageSource;
import io.trino.spi.connector.RecordSet;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.Type;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.AttributeValue;
import software.amazon.awssdk.services.ec2.model.ModifyInstanceAttributeRequest;
import software.amazon.awssdk.services.ec2.model.StartInstancesRequest;
import software.amazon.awssdk.services.ec2.model.StopInstancesRequest;
import software.amazon.awssdk.services.ec2.model.TerminateInstancesRequest;

import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static io.trino.spi.StandardErrorCode.INVALID_COLUMN_REFERENCE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class AwsPageSourceProvider
        implements ConnectorPageSourceProvider
{
    private final AwsRecordSetProvider recordSetProvider;
    private final AwsMetadata metadata;
    private final Ec2Client ec2;

    private final Map<String, Consumer<Block>> deleters;
    private final Map<String, BiConsumer<Map<String, Object>, Object>> updaters;

    @Inject
    public AwsPageSourceProvider(AwsRecordSetProvider recordSetProvider, AwsMetadata metadata, Ec2Client ec2)
    {
        this.recordSetProvider = requireNonNull(recordSetProvider, "recordSetProvider is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.ec2 = requireNonNull(ec2, "ec2 is null");

        this.deleters = new ImmutableMap.Builder<String, Consumer<Block>>()
                .put("ec2.instances", rowIds -> ec2.terminateInstances(
                        TerminateInstancesRequest.builder()
                                .instanceIds(
                                        getStringRowIds(rowIds, metadata.getRowIdHandle(new SchemaTableName("ec2", "instances"))))
                                .build()))
                .build();
        this.updaters = new ImmutableMap.Builder<String, BiConsumer<Map<String, Object>, Object>>()
                .put("ec2.instances", this::updateInstance)
                .build();
    }

    @Override
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableHandle table,
            List<ColumnHandle> columns,
            DynamicFilter dynamicFilter)
    {
        RecordSet recordSet = recordSetProvider.getRecordSet(transaction, session, split, table, columns);
        AwsTableHandle awsTable = (AwsTableHandle) table;
        if (awsTable.getUpdatedColumns().isPresent()) {
            SchemaTableName tableName = ((AwsTableHandle) table).getSchemaTableName();
            return new AwsUpdatablePageSource(
                    recordSet,
                    deleters.get(tableName.toString()),
                    wrapUpdater(
                            awsTable.getUpdatedColumns().get(),
                            metadata.getRowIdHandle(tableName),
                            updaters.get(tableName.toString())));
        }
        return new RecordPageSource(recordSet);
    }

    private BiConsumer<Page, List<Integer>> wrapUpdater(
            List<ColumnHandle> updatedColumns,
            AwsColumnHandle rowId,
            BiConsumer<Map<String, Object>, Object> updater)
    {
        return (page, channels) -> {
            for (int position = 0; position < page.getPositionCount(); position++) {
                ImmutableMap.Builder<String, Object> values = new ImmutableMap.Builder<>();
                for (int i = 0; i < updatedColumns.size(); i++) {
                    Block block = page.getBlock(channels.get(i));
                    if (block.isNull(position)) {
                        continue;
                    }

                    AwsColumnHandle column = (AwsColumnHandle) updatedColumns.get(i);
                    values.put(column.getName(), getValue(column.getType(), position, block));
                }
                Object rowIdValue = getValue(rowId.getType(), position, page.getBlock(channels.get(updatedColumns.size())));
                updater.accept(values.build(), rowIdValue);
            }
        };
    }

    private void updateInstance(Map<String, Object> values, Object rowId)
    {
        Set<String> updatedColumnNames = new HashSet<>(values.keySet());
        updatedColumnNames.removeAll(Set.of("state", "instance_type"));
        if (updatedColumnNames.size() != 0) {
            throw new TrinoException(INVALID_COLUMN_REFERENCE, format("Updates of %s ec2.instances columns are not supported", String.join(", ", updatedColumnNames)));
        }
        if (values.containsKey("state")) {
            String desiredState = (String) getValue(VARCHAR, 1, ((SingleMapBlock) values.get("state")).getLoadedBlock());
            switch (desiredState) {
                case "running":
                    ec2.startInstances(StartInstancesRequest.builder().instanceIds((String) rowId).build());
                    break;
                case "stopped":
                    ec2.stopInstances(StopInstancesRequest.builder().instanceIds((String) rowId).build());
                    break;
                default:
                    throw new TrinoException(INVALID_COLUMN_REFERENCE, format("Updates of ec2.instances.state column to %s are not supported", desiredState));
            }
        }
        if (values.containsKey("instance_type")) {
            String desiredInstanceType = ((String) values.get("instance_type")).toLowerCase(Locale.US);
            ec2.modifyInstanceAttribute(
                    ModifyInstanceAttributeRequest
                            .builder()
                            .instanceType(AttributeValue.builder().value(desiredInstanceType).build())
                            .instanceId((String) rowId)
                            .build());
        }
    }

    private List<String> getStringRowIds(Block rowIds, AwsColumnHandle rowIdHandle)
    {
        ImmutableList.Builder<String> results = new ImmutableList.Builder<>();
        for (int i = 0; i < rowIds.getPositionCount(); i++) {
            results.add((String) getValue(rowIdHandle.getType(), i, rowIds));
        }
        return results.build();
    }

    private Object getValue(Type type, int position, Block block)
    {
        Class<?> javaType = type.getJavaType();
        if (javaType == boolean.class) {
            return type.getBoolean(block, position);
        }
        if (javaType == long.class) {
            return type.getLong(block, position);
        }
        if (javaType == double.class) {
            return type.getDouble(block, position);
        }
        if (javaType == Slice.class) {
            return type.getSlice(block, position).toStringUtf8();
        }
        return type.getObject(block, position);
    }
}
