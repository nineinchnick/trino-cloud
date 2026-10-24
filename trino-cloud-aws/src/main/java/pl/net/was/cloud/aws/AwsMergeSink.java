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
import io.airlift.slice.Slice;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorMergeSink;

import java.util.Collection;
import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.TinyintType.TINYINT;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static pl.net.was.cloud.aws.PageValues.getValue;

public class AwsMergeSink
        implements ConnectorMergeSink
{
    private final AwsOutputTableHandle handle;
    private final AwsTable table;
    private final Optional<AwsPageSink> insertSink;

    public AwsMergeSink(AwsOutputTableHandle handle, AwsTable table)
    {
        this.handle = requireNonNull(handle, "handle is null");
        this.table = requireNonNull(table, "table is null");
        this.insertSink = table.inserter().map(_ -> new AwsPageSink(handle, table));
    }

    @Override
    public void storeMergedRows(Page page)
    {
        Block rowIds = page.getBlock(page.getChannelCount() - 1);
        Block ops = page.getBlock(page.getChannelCount() - 2);
        for (int position = 0; position < page.getPositionCount(); position++) {
            byte op = TINYINT.getByte(ops, position);
            int currentPosition = position;
            switch (op) {
                case INSERT_OPERATION_NUMBER -> insertSink.orElseThrow(notSupported("Inserts")).insert(page, position);
                case UPDATE_OPERATION_NUMBER -> table.updater().orElseThrow(notSupported("Updates"))
                        .update(getRowId(page, position), column -> getColumnValue(page, currentPosition, column));
                // TODO support batch operations, passing multiple IDs to the deleter
                case DELETE_OPERATION_NUMBER -> table.deleter().orElseThrow(notSupported("Deletes"))
                        .delete(getDeletedRowId(rowIds, position));
                default -> throw new TrinoException(NOT_SUPPORTED, "Unsupported merge operation: " + op);
            }
        }
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        return completedFuture(ImmutableList.of());
    }

    private Supplier<TrinoException> notSupported(String operation)
    {
        return () -> new TrinoException(NOT_SUPPORTED, "%s are not supported for %s".formatted(operation, table.name()));
    }

    private String getRowId(Page page, int position)
    {
        return ((String) getColumnValue(page, position, AwsTable.ROW_ID)).toLowerCase(Locale.US);
    }

    private Object getColumnValue(Page page, int position, String columnName)
    {
        int channel = handle.getColumnNames().indexOf(columnName);
        Block block = page.getBlock(channel);
        return getValue(handle.getColumnTypes().get(channel), position, block);
    }

    private String getDeletedRowId(Block rowIds, int position)
    {
        AwsColumnHandle rowIdColumn = table.rowIdColumn().orElseThrow(notSupported("Deletes"));
        return (String) getValue(rowIdColumn.type(), position, rowIds);
    }
}
