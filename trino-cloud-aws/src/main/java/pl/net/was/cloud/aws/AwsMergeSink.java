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

import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.SingleMapBlock;
import io.trino.spi.connector.ConnectorMergeSink;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.Type;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.AttributeValue;
import software.amazon.awssdk.services.ec2.model.DeregisterImageRequest;
import software.amazon.awssdk.services.ec2.model.ModifyInstanceAttributeRequest;
import software.amazon.awssdk.services.ec2.model.StartInstancesRequest;
import software.amazon.awssdk.services.ec2.model.StopInstancesRequest;
import software.amazon.awssdk.services.ec2.model.TerminateInstancesRequest;
import software.amazon.awssdk.services.s3.S3Client;

import java.util.Locale;
import java.util.Map;
import java.util.function.BiConsumer;

import static io.trino.spi.StandardErrorCode.INVALID_COLUMN_REFERENCE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class AwsMergeSink
        extends AwsPageSink
        implements ConnectorMergeSink
{
    private final Map<String, BiConsumer<Block, Integer>> deleters;
    private final Map<String, BiConsumer<Page, Integer>> updaters;

    private final Ec2Client ec2;

    public AwsMergeSink(ConnectorSession session, AwsOutputTableHandle table, AwsMetadata metadata, Ec2Client ec2, S3Client s3)
    {
        super(session, table, ec2, s3);

        this.ec2 = requireNonNull(ec2, "ec2 is null");
        this.deleters = new ImmutableMap.Builder<String, BiConsumer<Block, Integer>>()
                .put("ec2.images", (rowIds, pos) -> ec2.deregisterImage(DeregisterImageRequest.builder()
                        .imageId(getStringRowId(rowIds, pos, metadata.getRowIdHandle(new SchemaTableName("ec2", "images"))))
                        .build()))
                .put("ec2.instances", (rowIds, pos) -> ec2.terminateInstances(
                        TerminateInstancesRequest.builder()
                                .instanceIds(
                                        getStringRowId(rowIds, pos, metadata.getRowIdHandle(new SchemaTableName("ec2", "instances"))))
                                .build()))
                .build();
        this.updaters = new ImmutableMap.Builder<String, BiConsumer<Page, Integer>>()
                .put("ec2.instances", this::updateInstance)
                .build();
    }

    @Override
    public void storeMergedRows(Page page)
    {
        Block rowIds = page.getBlock(page.getChannelCount() - 1);
        Block ops = page.getBlock(page.getChannelCount() - 2);
        BiConsumer<Page, Integer> inserter = inserters.get(tableName);
        BiConsumer<Page, Integer> updater = updaters.get(tableName);
        BiConsumer<Block, Integer> deleter = deleters.get(tableName);
        for (int position = 0; position < page.getPositionCount(); position++) {
            int op = ops.getShort(position, 0);
            switch (op) {
                case INSERT_OPERATION_NUMBER:
                    inserter.accept(page, position);
                    break;
                case UPDATE_OPERATION_NUMBER:
                    updater.accept(page, position);
                case DELETE_OPERATION_NUMBER:
                    // TODO support batch operations, passing multiple IDs to the deleter
                    deleter.accept(rowIds, position);
            }
        }
    }

    private void updateInstance(Page page, int position)
    {
        String rowId = getRowId(page, position);
        String desiredState = getState(page, position);
        switch (desiredState) {
            case "running" -> ec2.startInstances(StartInstancesRequest.builder().instanceIds(rowId).build());
            case "stopped" -> ec2.stopInstances(StopInstancesRequest.builder().instanceIds(rowId).build());
            default -> throw new TrinoException(INVALID_COLUMN_REFERENCE, format("Updates of ec2.instances.state column to %s are not supported", desiredState));
        }
        String desiredInstanceType = getInstanceType(page, position);
        ec2.modifyInstanceAttribute(
                ModifyInstanceAttributeRequest
                        .builder()
                        .instanceType(AttributeValue.builder().value(desiredInstanceType).build())
                        .instanceId(rowId)
                        .build());
    }

    private String getRowId(Page page, int position)
    {
        int channel = table.getColumnNames().indexOf("row_id");
        Block block = page.getBlock(channel);
        Object value = getValue(table.getColumnTypes().get(channel), position, block);
        return ((String) value).toLowerCase(Locale.US);
    }

    private String getState(Page page, int position)
    {
        int channel = table.getColumnNames().indexOf("state");
        Block block = page.getBlock(channel);
        Object value = getValue(table.getColumnTypes().get(channel), position, block);
        return (String) getValue(VARCHAR, 1, ((SingleMapBlock) value).getLoadedBlock());
    }

    private String getInstanceType(Page page, int position)
    {
        int channel = table.getColumnNames().indexOf("instance_type");
        Block block = page.getBlock(channel);
        Object value = getValue(table.getColumnTypes().get(channel), position, block);
        return ((String) value).toLowerCase(Locale.US);
    }

    private String getStringRowId(Block rowIds, int position, AwsColumnHandle rowIdHandle)
    {
        return (String) getValue(rowIdHandle.getType(), position, rowIds);
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
