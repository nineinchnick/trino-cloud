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
import io.airlift.log.Logger;
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
import io.trino.spi.connector.LimitApplicationResult;
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
import static java.util.stream.Collectors.joining;
import static pl.net.was.cloud.aws.AwsErrorCode.AWS_MISSING_PREDICATE;

public class AwsMetadata
        implements ConnectorMetadata
{
    private static final Logger log = Logger.get(AwsMetadata.class);

    private final AwsTables tables;
    private final boolean readOnly;

    @Inject
    public AwsMetadata(AwsTables tables, AwsConfig config)
    {
        this.tables = requireNonNull(tables, "tables is null");
        this.readOnly = config.isReadOnly();
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
        SchemaTableName schemaTableName = tableHandle.schemaTableName();
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
        return tables.require(((AwsTableHandle) connectorTableHandle).schemaTableName()).columnHandles();
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
        checkWritesEnabled();
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
        return new AwsInsertTableHandle(awsTableHandle, columnNames.build(), columnTypes.build());
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(
            ConnectorSession session,
            ConnectorInsertTableHandle insertHandle,
            List<ConnectorTableHandle> sourceTableHandles,
            Collection<Slice> fragments,
            Collection<ComputedStatistics> computedStatistics)
    {
        logCreatedResources(session, ((AwsInsertTableHandle) insertHandle).tableHandle(), fragments);
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
        checkWritesEnabled();
        if (retryMode != RetryMode.NO_RETRIES) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support query retries");
        }
        AwsTableHandle awsTableHandle = (AwsTableHandle) tableHandle;
        AwsTable table = tables.require(awsTableHandle.schemaTableName());
        if (!table.supportsMerge()) {
            throw new TrinoException(StandardErrorCode.NOT_SUPPORTED, "Deletes and updates are not supported for %s".formatted(awsTableHandle.schemaTableName()));
        }
        checkPrimaryKeyFilter(table, awsTableHandle);
        Map<Integer, List<String>> updateCaseColumnNames = getUpdateCaseColumnNames(table, updateCaseColumns);
        // the merge page has a channel for every column that's not hidden, in the same order as in the table metadata
        List<ColumnMetadata> dataColumns = table.columns().stream()
                .filter(column -> !column.isHidden())
                .collect(toImmutableList());
        AwsInsertTableHandle insertHandle = new AwsInsertTableHandle(
                awsTableHandle,
                dataColumns.stream().map(ColumnMetadata::getName).collect(toImmutableList()),
                dataColumns.stream().map(ColumnMetadata::getType).collect(toImmutableList()));
        return new AwsMergeTableHandle(awsTableHandle, insertHandle, updateCaseColumnNames);
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishMerge(
            ConnectorSession session,
            ConnectorMergeTableHandle mergeTableHandle,
            List<ConnectorTableHandle> sourceTableHandles,
            Collection<Slice> fragments,
            Collection<ComputedStatistics> computedStatistics)
    {
        logCreatedResources(session, ((AwsMergeTableHandle) mergeTableHandle).tableHandle(), fragments);
        return Optional.empty();
    }

    private static void logCreatedResources(ConnectorSession session, AwsTableHandle tableHandle, Collection<Slice> fragments)
    {
        if (fragments.isEmpty()) {
            return;
        }
        String resourceIds = fragments.stream()
                .map(Slice::toStringUtf8)
                .collect(joining(", "));
        log.info("Query %s created resources in %s: %s", session.getQueryId(), tableHandle.schemaTableName(), resourceIds);
    }

    private void checkWritesEnabled()
    {
        if (readOnly) {
            throw new TrinoException(NOT_SUPPORTED, "This catalog is read-only, set aws.read-only=false to enable INSERT, UPDATE and DELETE");
        }
    }

    // without a filter on the primary key, a single statement could change or remove every resource the IAM identity can see
    private static void checkPrimaryKeyFilter(AwsTable table, AwsTableHandle tableHandle)
    {
        AwsColumnHandle primaryKey = table.primaryKeyColumn().orElseThrow();
        boolean hasFilter = tableHandle.constraint().getDomains()
                .map(domains -> domains.containsKey(primaryKey))
                // a constraint that matches no rows can't change anything
                .orElse(true);
        if (!hasFilter) {
            throw new TrinoException(AWS_MISSING_PREDICATE, "Deletes and updates on %1$s require a filter on %2$s, like WHERE %2$s = '...'".formatted(table.name(), primaryKey.name()));
        }
    }

    private static Map<Integer, List<String>> getUpdateCaseColumnNames(AwsTable table, Map<Integer, Collection<ColumnHandle>> updateCaseColumns)
    {
        Map<Integer, List<String>> updateCaseColumnNames = updateCaseColumns.entrySet().stream()
                .collect(toImmutableMap(
                        Map.Entry::getKey,
                        entry -> entry.getValue().stream()
                                .map(column -> ((AwsColumnHandle) column).name())
                                .collect(toImmutableList())));
        if (updateCaseColumnNames.isEmpty()) {
            return updateCaseColumnNames;
        }
        Set<String> supportedColumns = table.updater()
                .orElseThrow(() -> new TrinoException(NOT_SUPPORTED, "Updates are not supported for %s".formatted(table.name())))
                .columns();
        updateCaseColumnNames.values().stream()
                .flatMap(Collection::stream)
                .filter(column -> !supportedColumns.contains(column))
                .findFirst()
                .ifPresent(column -> {
                    throw new TrinoException(NOT_SUPPORTED, "Updating %s in %s is not supported, only these columns can be updated: %s".formatted(
                            column,
                            table.name(),
                            supportedColumns.stream().sorted().collect(joining(", "))));
                });
        return updateCaseColumnNames;
    }

    private ColumnHandle getRowId(AwsTableHandle tableHandle)
    {
        return tables.require(tableHandle.schemaTableName())
                .rowIdColumn()
                .orElseThrow(() -> new TrinoException(NOT_SUPPORTED, "Deletes and updates on table %s are not supported".formatted(tableHandle.schemaTableName())));
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(
            ConnectorSession session,
            ConnectorTableHandle table,
            Constraint constraint)
    {
        AwsTableHandle tableHandle = (AwsTableHandle) table;
        // the limit is applied while reading rows, so it would be applied before the filter
        if (tableHandle.limit().isPresent()) {
            return Optional.empty();
        }
        AwsTable awsTable = tables.require(tableHandle.schemaTableName());
        return awsTable.filterApplier().flatMap(filterApplier -> filterApplier.applyFilter(
                tableHandle,
                awsTable.columnHandles(),
                filterApplier.getSupportedFilters(),
                constraint));
    }

    /**
     * Every split stops reading rows from AWS after reaching the limit, so the engine still applies it to the rows of all splits.
     */
    @Override
    public Optional<LimitApplicationResult<ConnectorTableHandle>> applyLimit(
            ConnectorSession session,
            ConnectorTableHandle table,
            long limit)
    {
        AwsTableHandle tableHandle = (AwsTableHandle) table;
        if (tableHandle.limit().isPresent() && tableHandle.limit().orElseThrow() <= limit) {
            return Optional.empty();
        }
        return Optional.of(new LimitApplicationResult<>(tableHandle.withLimit(limit), false, false));
    }
}
