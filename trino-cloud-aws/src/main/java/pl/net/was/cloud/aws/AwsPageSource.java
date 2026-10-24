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

import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.MemoryContext;
import io.trino.spi.connector.SourcePage;
import pl.net.was.cloud.aws.SdkRowEncoder.ColumnWriter;

import java.util.Iterator;
import java.util.List;
import java.util.OptionalLong;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

/**
 * Reads rows lazily, so AWS is only called when the next page of results is needed,
 * and encodes only the projected columns. Stops reading after the pushed-down limit.
 */
class AwsPageSource
        implements ConnectorPageSource
{
    // bounds the page size when the columns are small or no columns are projected, like in count(*)
    static final int MAX_ROWS_PER_PAGE = 1024;

    private final AwsTable table;
    private final AwsTableHandle tableHandle;
    private final List<ColumnWriter> columnWriters;
    private final PageBuilder pageBuilder;
    private final MemoryContext memoryContext;

    private Iterator<SdkRow> rows;
    private boolean finished;
    private long completedBytes;
    private long completedPositions;
    private long readTimeNanos;
    private long reportedMemoryBytes;

    AwsPageSource(AwsTable table, AwsTableHandle tableHandle, List<AwsColumnHandle> columns, MemoryContext memoryContext)
    {
        this.table = requireNonNull(table, "table is null");
        this.tableHandle = requireNonNull(tableHandle, "tableHandle is null");
        requireNonNull(columns, "columns is null");
        this.columnWriters = columns.stream()
                .map(table::columnWriter)
                .collect(toImmutableList());
        this.pageBuilder = new PageBuilder(columns.stream()
                .map(AwsColumnHandle::type)
                .collect(toImmutableList()));
        this.memoryContext = requireNonNull(memoryContext, "memoryContext is null");
        reportMemory(pageBuilder.getRetainedSizeInBytes());
    }

    @Override
    public long getCompletedBytes()
    {
        return completedBytes;
    }

    @Override
    public OptionalLong getCompletedPositions()
    {
        return OptionalLong.of(completedPositions);
    }

    @Override
    public long getReadTimeNanos()
    {
        return readTimeNanos;
    }

    @Override
    public boolean isFinished()
    {
        return finished;
    }

    @Override
    public SourcePage getNextSourcePage()
    {
        if (finished) {
            return null;
        }
        while (!pageBuilder.isFull() && pageBuilder.getPositionCount() < MAX_ROWS_PER_PAGE) {
            if (isLimitReached() || !hasNextRow()) {
                finished = true;
                break;
            }
            SdkRow row = rows.next();
            pageBuilder.declarePosition();
            for (int column = 0; column < columnWriters.size(); column++) {
                columnWriters.get(column).write(row, pageBuilder.getBlockBuilder(column));
            }
            reportMemory(pageBuilder.getRetainedSizeInBytes());
        }
        if (pageBuilder.isEmpty()) {
            return null;
        }
        Page page = pageBuilder.build();
        pageBuilder.reset();
        reportMemory(pageBuilder.getRetainedSizeInBytes());
        completedBytes += page.getSizeInBytes();
        completedPositions += page.getPositionCount();
        return SourcePage.create(page);
    }

    @Override
    public void close()
    {
        finished = true;
        reportMemory(0);
    }

    private void reportMemory(long bytes)
    {
        if (bytes == reportedMemoryBytes) {
            return;
        }
        memoryContext.setBytes(bytes);
        reportedMemoryBytes = bytes;
    }

    // checked before getting the next row, so AWS isn't asked for the next page of results that wouldn't be read
    private boolean isLimitReached()
    {
        OptionalLong limit = tableHandle.limit();
        return limit.isPresent() && completedPositions + pageBuilder.getPositionCount() >= limit.orElseThrow();
    }

    // AWS is called when getting the rows of tables that are not paginated, and when the iterator needs the next page of results
    private boolean hasNextRow()
    {
        long start = System.nanoTime();
        try {
            if (rows == null) {
                rows = table.rows(tableHandle).iterator();
            }
            return rows.hasNext();
        }
        finally {
            readTimeNanos += System.nanoTime() - start;
        }
    }
}
