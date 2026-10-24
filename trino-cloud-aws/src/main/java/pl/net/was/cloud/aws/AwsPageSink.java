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
import io.airlift.slice.Slice;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.SqlMap;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.type.MapType;
import io.trino.spi.type.TypeOperators;
import pl.net.was.cloud.aws.AwsTable.Inserter;
import software.amazon.awssdk.awscore.AwsRequest;
import software.amazon.awssdk.core.SdkField;
import software.amazon.awssdk.core.SdkPojo;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.ec2.model.TagSpecification;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static pl.net.was.cloud.aws.PageValues.getValue;

public class AwsPageSink
        implements ConnectorPageSink
{
    private final AwsInsertTableHandle handle;
    private final String tableName;
    private final Inserter inserter;
    private final Map<String, SdkField<?>> fields;

    public AwsPageSink(AwsInsertTableHandle handle, AwsTable table)
    {
        this.handle = requireNonNull(handle, "handle is null");
        this.tableName = table.name().toString();
        this.inserter = table.inserter()
                .orElseThrow(() -> new TrinoException(NOT_SUPPORTED, "Inserts are not supported for " + tableName));
        this.fields = ((SdkPojo) inserter.requestBuilder().get())
                .sdkFields()
                .stream()
                .collect(toImmutableMap(
                        (SdkField<?> field) -> CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, field.memberName()),
                        (SdkField<?> field) -> field));
    }

    @Override
    public CompletableFuture<?> appendPage(Page page)
    {
        for (int position = 0; position < page.getPositionCount(); position++) {
            insert(page, position);
        }
        return NOT_BLOCKED;
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        return completedFuture(ImmutableList.of());
    }

    @Override
    public void abort() {}

    void insert(Page page, int position)
    {
        AwsRequest.Builder request = setFields(page, position, inserter.requestBuilder().get());
        try {
            inserter.executor().accept(request.build());
        }
        catch (SdkException e) {
            throw AwsErrors.translate(e);
        }
    }

    private AwsRequest.Builder setFields(Page page, int position, AwsRequest.Builder request)
    {
        for (int channel = 0; channel < page.getChannelCount(); channel++) {
            Block block = page.getBlock(channel);
            if (block.isNull(position)) {
                continue;
            }

            String columnName = handle.columnNames().get(channel);
            SdkField<?> field = fields.get(columnName);
            if (field == null && columnName.equals("tags")) {
                field = fields.get("tag_specifications");
            }
            if (field == null) {
                throw new TrinoException(NOT_SUPPORTED, "Inserts to %s with %s are not supported".formatted(tableName, columnName));
            }

            VARCHAR.getSlice(block, position).toStringUtf8();
            Object value = getValue(handle.columnTypes().get(channel), position, block);
            if (columnName.equals("tags")) {
                ImmutableList.Builder<Tag> tags = ImmutableList.builder();
                Block array = (Block) value;
                MapType tagType = new MapType(VARCHAR, VARCHAR, new TypeOperators());
                for (int i = 0; i < array.getPositionCount(); i++) {
                    SqlMap map = (SqlMap) getValue(tagType, i, array);
                    tags.add(Tag
                            .builder()
                            .key((String) getValue(VARCHAR, 0, map.getRawKeyBlock()))
                            .value((String) getValue(VARCHAR, 1, map.getRawValueBlock()))
                            .build());
                }
                value = TagSpecification.builder().tags(tags.build()).build();
            }
            field.set(request, value);
        }
        return request;
    }
}
