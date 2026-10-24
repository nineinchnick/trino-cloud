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
import com.google.inject.Inject;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorRecordSetProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.InMemoryRecordSet;
import io.trino.spi.connector.RecordSet;
import io.trino.spi.type.Type;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class AwsRecordSetProvider
        implements ConnectorRecordSetProvider
{
    private final AwsTables tables;

    @Inject
    public AwsRecordSetProvider(AwsTables tables)
    {
        this.tables = requireNonNull(tables, "tables is null");
    }

    @Override
    public RecordSet getRecordSet(
            ConnectorTransactionHandle connectorTransactionHandle,
            ConnectorSession connectorSession,
            ConnectorSplit connectorSplit,
            ConnectorTableHandle table,
            List<? extends ColumnHandle> list)
    {
        AwsTableHandle tableHandle = (AwsTableHandle) table;
        AwsTable awsTable = tables.require(tableHandle.schemaTableName());
        List<AwsColumnHandle> restColumnHandles = list
                .stream()
                .map(c -> (AwsColumnHandle) c)
                .toList();

        List<Integer> columnIndexes = restColumnHandles
                .stream()
                .map(column -> {
                    int index = 0;
                    for (ColumnMetadata columnMetadata : awsTable.columns()) {
                        if (columnMetadata.getName().equalsIgnoreCase(column.name())) {
                            return index;
                        }
                        index++;
                    }
                    throw new IllegalStateException("Unknown column: " + column.name());
                })
                .toList();

        List<Type> mappedTypes = restColumnHandles
                .stream()
                .map(AwsColumnHandle::type)
                .toList();

        //noinspection StaticPseudoFunctionalStyleMethod
        Iterable<List<?>> mappedRows = Iterables.transform(
                awsTable.rows(tableHandle),
                row -> columnIndexes
                        .stream()
                        .map(row::get)
                        .toList());

        return new InMemoryRecordSet(mappedTypes, mappedRows);
    }
}
