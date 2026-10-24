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

import com.google.inject.Inject;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorMergeSink;
import io.trino.spi.connector.ConnectorMergeTableHandle;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorPageSinkId;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableCredentials;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.MemoryContext;

import java.util.Optional;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.util.Objects.requireNonNull;

public class AwsPageSinkProvider
        implements ConnectorPageSinkProvider
{
    private final AwsTables tables;
    private final int maxInsertedRows;

    @Inject
    public AwsPageSinkProvider(AwsTables tables, AwsConfig config)
    {
        this.tables = requireNonNull(tables, "tables is null");
        this.maxInsertedRows = config.getMaxInsertedRows();
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorOutputTableHandle tableHandle, Optional<ConnectorTableCredentials> tableCredentials, ConnectorPageSinkId pageSinkId)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support creating tables");
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorInsertTableHandle tableHandle, Optional<ConnectorTableCredentials> tableCredentials, ConnectorPageSinkId pageSinkId)
    {
        return createPageSink((AwsInsertTableHandle) tableHandle);
    }

    @Override
    public ConnectorMergeSink createMergeSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorMergeTableHandle tableHandle, Optional<ConnectorTableCredentials> tableCredentials, ConnectorPageSinkId pageSinkId, MemoryContext memoryContext)
    {
        AwsMergeTableHandle handle = (AwsMergeTableHandle) tableHandle;
        AwsTable table = tables.require(handle.tableHandle().schemaTableName());
        return new AwsMergeSink(handle, table, createInsertSink(handle.insertHandle(), table));
    }

    private Optional<AwsPageSink> createInsertSink(AwsInsertTableHandle handle, AwsTable table)
    {
        if (table.inserter().isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(createPageSink(handle));
    }

    private AwsPageSink createPageSink(AwsInsertTableHandle handle)
    {
        return new AwsPageSink(handle, tables.require(handle.tableHandle().schemaTableName()), maxInsertedRows);
    }
}
