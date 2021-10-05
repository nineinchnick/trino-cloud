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

import io.airlift.slice.Slice;
import io.trino.spi.Page;
import io.trino.spi.StandardErrorCode;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.connector.RecordPageSource;
import io.trino.spi.connector.RecordSet;
import io.trino.spi.connector.UpdatablePageSource;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static java.lang.String.format;

public class AwsUpdatablePageSource
        implements UpdatablePageSource
{
    private final RecordPageSource inner;
    private final Consumer<Block> deleter;
    private final BiConsumer<Page, List<Integer>> updater;
    private final String tableName;

    public AwsUpdatablePageSource(RecordSet recordSet, Consumer<Block> deleter, BiConsumer<Page, List<Integer>> updater, String tableName)
    {
        this.inner = new RecordPageSource(recordSet);
        this.deleter = deleter;
        this.updater = updater;
        this.tableName = tableName;
    }

    @Override
    public void deleteRows(Block rowIds)
    {
        if (deleter == null) {
            throw new TrinoException(StandardErrorCode.NOT_SUPPORTED, format("Deletes are not supported for %s", tableName));
        }
        deleter.accept(rowIds);
    }

    @Override
    public void updateRows(Page page, List<Integer> columnValueAndRowIdChannels)
    {
        if (updater == null) {
            throw new TrinoException(StandardErrorCode.NOT_SUPPORTED, format("Updates are not supported for %s", tableName));
        }
        updater.accept(page, columnValueAndRowIdChannels);
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        CompletableFuture<Collection<Slice>> cf = new CompletableFuture<>();
        cf.complete(Collections.emptyList());
        return cf;
    }

    @Override
    public long getCompletedBytes()
    {
        return inner.getCompletedBytes();
    }

    @Override
    public long getReadTimeNanos()
    {
        return inner.getReadTimeNanos();
    }

    @Override
    public boolean isFinished()
    {
        return inner.isFinished();
    }

    @Override
    public Page getNextPage()
    {
        return inner.getNextPage();
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return inner.getSystemMemoryUsage();
    }

    @Override
    public void close()
    {
        inner.close();
    }
}
