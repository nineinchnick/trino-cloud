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
import com.google.inject.Inject;
import io.airlift.slice.Slice;
import io.trino.spi.StandardErrorCode;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorMergeTableHandle;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorOutputMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.RelationColumnsMetadata;
import io.trino.spi.connector.RetryMode;
import io.trino.spi.connector.RowChangeParadigm;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.statistics.ComputedStatistics;
import io.trino.spi.type.Type;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.UnaryOperator;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.connector.RowChangeParadigm.CHANGE_ONLY_UPDATED_COLUMNS;
import static java.util.Objects.requireNonNull;

public class AwsMetadata
        implements ConnectorMetadata
{
    private final AwsTables tables;

    @Inject
    public AwsMetadata(AwsTables tables)
    {
        this.tables = requireNonNull(tables, "tables is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession connectorSession)
    {
        return List.of("ec2", "s3");
    }

    @Override
    public ConnectorTableHandle getTableHandle(
            ConnectorSession session,
            SchemaTableName tableName,
            Optional<ConnectorTableVersion> startVersion,
            Optional<ConnectorTableVersion> endVersion)
    {
        if (startVersion.isPresent() || endVersion.isPresent()) {
            throw new TrinoException(StandardErrorCode.NOT_SUPPORTED, "This connector does not support versioned tables");
        }
        return tables.get(tableName)
                .map(table -> new AwsTableHandle(table.name()))
                .orElse(null);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(
            ConnectorSession connectorSession,
            ConnectorTableHandle connectorTableHandle)
    {
        AwsTableHandle tableHandle = (AwsTableHandle) connectorTableHandle;
        SchemaTableName schemaTableName = tableHandle.getSchemaTableName();
        return new ConnectorTableMetadata(
                schemaTableName,
                tables.require(schemaTableName).columns());
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        return tables.all().stream()
                .map(AwsTable::name)
                .filter(name -> schemaName.isEmpty() || name.getSchemaName().equals(schemaName.get()))
                .collect(toImmutableList());
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(
            ConnectorSession connectorSession,
            ConnectorTableHandle connectorTableHandle)
    {
        return tables.require(((AwsTableHandle) connectorTableHandle).getSchemaTableName()).columnHandles();
    }

    @Override
    public ColumnMetadata getColumnMetadata(
            ConnectorSession connectorSession,
            ConnectorTableHandle connectorTableHandle,
            ColumnHandle columnHandle)
    {
        AwsColumnHandle restColumnHandle = (AwsColumnHandle) columnHandle;
        return new ColumnMetadata(restColumnHandle.name(), restColumnHandle.type());
    }

    @Override
    public Iterator<RelationColumnsMetadata> streamRelationColumns(
            ConnectorSession session,
            Optional<String> schemaName,
            UnaryOperator<Set<SchemaTableName>> relationFilter)
    {
        Map<SchemaTableName, RelationColumnsMetadata> relationColumns = tables.all()
                .stream()
                .filter(table -> schemaName.isEmpty() || table.name().getSchemaName().equals(schemaName.get()))
                .collect(toImmutableMap(AwsTable::name, table -> RelationColumnsMetadata.forTable(table.name(), table.columns())));
        return relationFilter.apply(relationColumns.keySet())
                .stream()
                .map(relationColumns::get)
                .iterator();
    }

    @Override
    public ConnectorInsertTableHandle beginInsert(
            ConnectorSession session,
            ConnectorTableHandle tableHandle,
            List<ColumnHandle> columns,
            RetryMode retryMode)
    {
        if (retryMode != RetryMode.NO_RETRIES) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support query retries");
        }
        AwsTableHandle awsTableHandle = (AwsTableHandle) tableHandle;
        List<AwsColumnHandle> columnHandles = columns.stream()
                .map(AwsColumnHandle.class::cast)
                .collect(toImmutableList());
        ImmutableList.Builder<String> columnNames = ImmutableList.builder();
        ImmutableList.Builder<Type> columnTypes = ImmutableList.builder();
        for (AwsColumnHandle column : columnHandles) {
            columnNames.add(column.name());
            columnTypes.add(column.type());
        }
        return new AwsOutputTableHandle(awsTableHandle, columnNames.build(), columnTypes.build());
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(
            ConnectorSession session,
            ConnectorInsertTableHandle insertHandle,
            List<ConnectorTableHandle> sourceTableHandles,
            Collection<Slice> fragments,
            Collection<ComputedStatistics> computedStatistics)
    {
        return Optional.empty();
    }

    @Override
    public RowChangeParadigm getRowChangeParadigm(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return CHANGE_ONLY_UPDATED_COLUMNS;
    }

    @Override
    public ColumnHandle getMergeRowIdColumnHandle(
            ConnectorSession session,
            ConnectorTableHandle tableHandle)
    {
        return getRowId((AwsTableHandle) tableHandle);
    }

    @Override
    public ConnectorMergeTableHandle beginMerge(
            ConnectorSession session,
            ConnectorTableHandle tableHandle,
            Map<Integer, Collection<ColumnHandle>> updateCaseColumns,
            RetryMode retryMode)
    {
        if (retryMode != RetryMode.NO_RETRIES) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support query retries");
        }
        AwsTableHandle awsTableHandle = (AwsTableHandle) tableHandle;
        if (!tables.require(awsTableHandle.getSchemaTableName()).supportsMerge()) {
            throw new TrinoException(StandardErrorCode.NOT_SUPPORTED, "Deletes and updates are not supported for %s".formatted(awsTableHandle.getSchemaTableName()));
        }
        return new AwsOutputTableHandle(awsTableHandle, List.of(), List.of());
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishMerge(
            ConnectorSession session,
            ConnectorMergeTableHandle mergeTableHandle,
            List<ConnectorTableHandle> sourceTableHandles,
            Collection<Slice> fragments,
            Collection<ComputedStatistics> computedStatistics)
    {
        return Optional.empty();
    }

    private ColumnHandle getRowId(AwsTableHandle tableHandle)
    {
        return tables.require(tableHandle.getSchemaTableName())
                .rowIdColumn()
                .orElseThrow(() -> new TrinoException(NOT_SUPPORTED, "Deletes and updates on table %s are not supported".formatted(tableHandle.getSchemaTableName())));
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(
            ConnectorSession session,
            ConnectorTableHandle table,
            Constraint constraint)
    {
        AwsTableHandle tableHandle = (AwsTableHandle) table;
        AwsTable awsTable = tables.require(tableHandle.getSchemaTableName());
        return awsTable.filterApplier().flatMap(filterApplier -> filterApplier.applyFilter(
                tableHandle,
                awsTable.columnHandles(),
                filterApplier.getSupportedFilters(),
                constraint));
    }
}
