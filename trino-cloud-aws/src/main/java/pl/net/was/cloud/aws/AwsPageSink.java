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

import com.google.common.base.CaseFormat;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.SingleMapBlock;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.MapType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import software.amazon.awssdk.awscore.AwsRequest;
import software.amazon.awssdk.core.SdkField;
import software.amazon.awssdk.core.SdkPojo;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.CreateImageRequest;
import software.amazon.awssdk.services.ec2.model.RunInstancesRequest;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.ec2.model.TagSpecification;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static io.trino.spi.StandardErrorCode.INVALID_COLUMN_REFERENCE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class AwsPageSink
        implements ConnectorPageSink
{
    private final AwsOutputTableHandle table;
    protected final String tableName;

    protected final Map<String, BiConsumer<Page, Integer>> inserters;
    private final Map<String, Map<String, SdkField<?>>> fields = new ImmutableMap.Builder<String, Map<String, SdkField<?>>>()
            .put("ec2.images", getFieldsMap(CreateImageRequest.builder()))
            .put("ec2.instances", getFieldsMap(RunInstancesRequest.builder()))
            .build();

    private Map<String, SdkField<?>> getFieldsMap(SdkPojo request)
    {
        return request
                .sdkFields()
                .stream()
                .map(f -> Map.entry(
                        CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, f.memberName()),
                        f))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    public AwsPageSink(ConnectorSession session, AwsOutputTableHandle table, Ec2Client ec2, S3Client s3)
    {
        requireNonNull(ec2, "ec2 is null");
        requireNonNull(s3, "s3 is null");

        this.table = table;
        this.tableName = table.getTableHandle().getSchemaTableName().toString();

        this.inserters = new ImmutableMap.Builder<String, BiConsumer<Page, Integer>>()
                .put("ec2.images", (page, pos) -> ec2.createImage((CreateImageRequest) setFields(page, pos, CreateImageRequest.builder()).build()))
                .put("ec2.instances", (page, pos) -> ec2.runInstances((RunInstancesRequest) setFields(page, pos, RunInstancesRequest.builder().minCount(1).maxCount(1)).build()))
                .put("s3.buckets", (page, pos) -> s3.createBucket((CreateBucketRequest) setFields(page, pos, CreateBucketRequest.builder()).build()))
                .build();
    }

    @Override
    public CompletableFuture<?> appendPage(Page page)
    {
        BiConsumer<Page, Integer> inserter = inserters.get(tableName);
        for (int position = 0; position < page.getPositionCount(); position++) {
            inserter.accept(page, position);
        }
        return NOT_BLOCKED;
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        return completedFuture(ImmutableList.of());
    }

    @SuppressWarnings("unused")
    @Override
    public void abort()
    {
    }

    private AwsRequest.Builder setFields(Page page, int position, AwsRequest.Builder request)
    {
        for (int channel = 0; channel < page.getChannelCount(); channel++) {
            Block block = page.getBlock(channel);
            if (block.isNull(position)) {
                continue;
            }

            String columnName = table.getColumnNames().get(channel);
            SdkField<?> field = fields.get(tableName).get(columnName);
            if (field == null && columnName.equals("tags")) {
                field = fields.get(tableName).get("tag_specifications");
            }
            if (field == null) {
                throw new TrinoException(INVALID_COLUMN_REFERENCE, format("Inserts to %s with %s are not supported", tableName, columnName));
            }

            VARCHAR.getSlice(block, position).toStringUtf8();
            Object value = getValue(table.getColumnTypes().get(channel), position, block);
            if (columnName.equals("tags")) {
                ImmutableList.Builder<Tag> tags = new ImmutableList.Builder<>();
                Block array = (Block) value;
                MapType tagType = new MapType(VARCHAR, VARCHAR, new TypeOperators());
                for (int i = 0; i < array.getPositionCount(); i++) {
                    SingleMapBlock map = (SingleMapBlock) getValue(tagType, i, array);
                    tags.add(Tag
                            .builder()
                            .key((String) getValue(VARCHAR, 0, map.getLoadedBlock()))
                            .value((String) getValue(VARCHAR, 1, map.getLoadedBlock()))
                            .build());
                }
                value = TagSpecification.builder().tags(tags.build()).build();
            }
            field.set(request, value);
        }
        return request;
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
