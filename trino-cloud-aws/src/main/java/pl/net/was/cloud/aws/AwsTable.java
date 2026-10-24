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
import com.google.common.collect.ImmutableSet;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.SchemaTableName;
import pl.net.was.cloud.aws.filters.FilterApplier;
import software.amazon.awssdk.awscore.AwsRequest;
import software.amazon.awssdk.core.exception.SdkException;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;
import static pl.net.was.cloud.aws.SdkIterables.translateErrors;

public final class AwsTable
{
    public static final String ROW_ID = "row_id";

    private final SchemaTableName name;
    private final List<ColumnMetadata> columns;
    private final Map<String, ColumnHandle> columnHandles;
    private final RowSource rowSource;
    private final Optional<FilterApplier> filterApplier;
    private final Optional<String> primaryKey;
    private final Optional<Inserter> inserter;
    private final Optional<Updater> updater;
    private final Optional<Deleter> deleter;

    private AwsTable(Builder builder)
    {
        this.name = requireNonNull(builder.name, "name is null");
        this.columns = ImmutableList.copyOf(requireNonNull(builder.columns, "columns is null"));
        this.columnHandles = columns.stream()
                .collect(toImmutableMap(ColumnMetadata::getName, column -> new AwsColumnHandle(column.getName(), column.getType())));
        this.rowSource = requireNonNull(builder.rowSource, "rowSource is null");
        this.filterApplier = Optional.ofNullable(builder.filterApplier);
        this.primaryKey = Optional.ofNullable(builder.primaryKey);
        this.inserter = Optional.ofNullable(builder.inserter);
        this.updater = Optional.ofNullable(builder.updater);
        this.deleter = Optional.ofNullable(builder.deleter);
        checkArgument((updater.isEmpty() && deleter.isEmpty()) || columnHandles.containsKey(ROW_ID), "%s supports updates or deletes, so it must have a %s column", name, ROW_ID);
        checkArgument((updater.isEmpty() && deleter.isEmpty()) || primaryKey.isPresent(), "%s supports updates or deletes, so it must have a primary key", name);
        primaryKey.ifPresent(column -> checkArgument(columnHandles.containsKey(column), "Primary key column %s does not exist in %s", column, name));
        updater.ifPresent(value -> value.columns().forEach(column -> checkArgument(columnHandles.containsKey(column), "Updated column %s does not exist in %s", column, name)));
    }

    public static Builder builder(SchemaTableName name, List<ColumnMetadata> columns)
    {
        return new Builder(name, columns);
    }

    public SchemaTableName name()
    {
        return name;
    }

    public List<ColumnMetadata> columns()
    {
        return columns;
    }

    public Map<String, ColumnHandle> columnHandles()
    {
        return columnHandles;
    }

    public AwsColumnHandle columnHandle(String columnName)
    {
        ColumnHandle column = columnHandles.get(columnName);
        checkArgument(column != null, "Column %s does not exist in %s", columnName, name);
        return (AwsColumnHandle) column;
    }

    public Optional<AwsColumnHandle> rowIdColumn()
    {
        return Optional.ofNullable((AwsColumnHandle) columnHandles.get(ROW_ID));
    }

    public Optional<AwsColumnHandle> primaryKeyColumn()
    {
        return primaryKey.map(this::columnHandle);
    }

    public Iterable<List<?>> rows(AwsTableHandle table)
    {
        try {
            return translateErrors(rowSource.rows(this, table));
        }
        catch (SdkException e) {
            throw AwsErrors.translate(e);
        }
    }

    public FilterApplier requireFilterApplier()
    {
        checkState(filterApplier.isPresent(), "%s does not support filters", name);
        return filterApplier.get();
    }

    public Optional<FilterApplier> filterApplier()
    {
        return filterApplier;
    }

    public Optional<Inserter> inserter()
    {
        return inserter;
    }

    public Optional<Updater> updater()
    {
        return updater;
    }

    public Optional<Deleter> deleter()
    {
        return deleter;
    }

    public boolean supportsMerge()
    {
        return updater.isPresent() || deleter.isPresent();
    }

    @FunctionalInterface
    public interface RowSource
    {
        Iterable<List<?>> rows(AwsTable table, AwsTableHandle handle);
    }

    /**
     * The executor sends the request and returns the IDs of the resources it created.
     */
    public record Inserter(Supplier<AwsRequest.Builder> requestBuilder, Function<AwsRequest, List<String>> executor)
    {
        public Inserter
        {
            requireNonNull(requestBuilder, "requestBuilder is null");
            requireNonNull(executor, "executor is null");
        }
    }

    /**
     * The executor receives the row ID and the new values of the updated columns, which are a subset of the supported columns.
     */
    public record Updater(Set<String> columns, BiConsumer<String, Map<String, Object>> executor)
    {
        public Updater
        {
            columns = ImmutableSet.copyOf(requireNonNull(columns, "columns is null"));
            requireNonNull(executor, "executor is null");
        }
    }

    @FunctionalInterface
    public interface Deleter
    {
        void delete(String rowId);
    }

    public static final class Builder
    {
        private final SchemaTableName name;
        private final List<ColumnMetadata> columns;
        private RowSource rowSource;
        private FilterApplier filterApplier;
        private String primaryKey;
        private Inserter inserter;
        private Updater updater;
        private Deleter deleter;

        private Builder(SchemaTableName name, List<ColumnMetadata> columns)
        {
            this.name = requireNonNull(name, "name is null");
            this.columns = requireNonNull(columns, "columns is null");
        }

        public Builder rowSource(RowSource rowSource)
        {
            this.rowSource = rowSource;
            return this;
        }

        public Builder filterApplier(FilterApplier filterApplier)
        {
            this.filterApplier = filterApplier;
            return this;
        }

        public Builder primaryKey(String primaryKey)
        {
            this.primaryKey = primaryKey;
            return this;
        }

        public Builder inserter(Inserter inserter)
        {
            this.inserter = inserter;
            return this;
        }

        public Builder updater(Updater updater)
        {
            this.updater = updater;
            return this;
        }

        public Builder deleter(Deleter deleter)
        {
            this.deleter = deleter;
            return this;
        }

        public AwsTable build()
        {
            return new AwsTable(this);
        }
    }
}
