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
import io.airlift.slice.Slice;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorMergeSink;
import software.amazon.awssdk.core.exception.SdkException;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.TinyintType.TINYINT;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static pl.net.was.cloud.aws.PageValues.getValue;

public class AwsMergeSink
        implements ConnectorMergeSink
{
    private final AwsInsertTableHandle handle;
    private final Map<Integer, List<String>> updateCaseColumns;
    private final AwsTable table;
    private final Optional<AwsPageSink> insertSink;

    public AwsMergeSink(AwsMergeTableHandle handle, AwsTable table, Optional<AwsPageSink> insertSink)
    {
        requireNonNull(handle, "handle is null");
        this.handle = handle.insertHandle();
        this.updateCaseColumns = handle.updateCaseColumns();
        this.table = requireNonNull(table, "table is null");
        this.insertSink = requireNonNull(insertSink, "insertSink is null");
    }

    // the layout of the page is described in ConnectorMergeSink.storeMergedRows
    @Override
    public void storeMergedRows(Page page)
    {
        int dataChannelCount = page.getChannelCount() - 3;
        checkArgument(dataChannelCount == handle.columnNames().size(), "Expected %s data channels, got %s", handle.columnNames().size(), dataChannelCount);
        Page dataPage = page.getColumns(IntStream.range(0, dataChannelCount).toArray());
        Block operations = page.getBlock(dataChannelCount);
        Block caseNumbers = page.getBlock(dataChannelCount + 1);
        Block rowIds = page.getBlock(dataChannelCount + 2);
        for (int position = 0; position < page.getPositionCount(); position++) {
            byte operation = TINYINT.getByte(operations, position);
            try {
                switch (operation) {
                    case INSERT_OPERATION_NUMBER -> insertSink.orElseThrow(notSupported("Inserts")).insert(dataPage, position);
                    case UPDATE_OPERATION_NUMBER -> table.updater().orElseThrow(notSupported("Updates")).executor()
                            .accept(getRowId(rowIds, position), getUpdatedValues(dataPage, position, INTEGER.getInt(caseNumbers, position)));
                    // TODO support batch operations, passing multiple IDs to the deleter
                    case DELETE_OPERATION_NUMBER -> table.deleter().orElseThrow(notSupported("Deletes"))
                            .delete(getRowId(rowIds, position));
                    default -> throw new TrinoException(NOT_SUPPORTED, "Unsupported merge operation: " + operation);
                }
            }
            catch (SdkException e) {
                throw AwsErrors.translate(e);
            }
        }
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        return insertSink
                .map(AwsPageSink::finish)
                .orElseGet(() -> completedFuture(ImmutableList.of()));
    }

    @Override
    public void abort()
    {
        insertSink.ifPresent(AwsPageSink::abort);
    }

    private Supplier<TrinoException> notSupported(String operation)
    {
        return () -> new TrinoException(NOT_SUPPORTED, "%s are not supported for %s".formatted(operation, table.name()));
    }

    private String getRowId(Block rowIds, int position)
    {
        AwsColumnHandle rowIdColumn = table.rowIdColumn().orElseThrow(notSupported("Deletes and updates"));
        return (String) getValue(rowIdColumn.type(), position, rowIds);
    }

    private Map<String, Object> getUpdatedValues(Page dataPage, int position, int caseNumber)
    {
        List<String> columns = updateCaseColumns.get(caseNumber);
        checkArgument(columns != null, "Unknown update case number: %s", caseNumber);
        ImmutableMap.Builder<String, Object> values = ImmutableMap.builder();
        for (String column : columns) {
            values.put(column, getColumnValue(dataPage, position, column));
        }
        return values.buildOrThrow();
    }

    private Object getColumnValue(Page dataPage, int position, String columnName)
    {
        int channel = handle.columnNames().indexOf(columnName);
        checkArgument(channel >= 0, "Column %s does not exist in %s", columnName, table.name());
        Block block = dataPage.getBlock(channel);
        if (block.isNull(position)) {
            throw new TrinoException(NOT_SUPPORTED, "Updating %s in %s to NULL is not supported".formatted(columnName, table.name()));
        }
        return getValue(handle.columnTypes().get(channel), position, block);
    }
}
