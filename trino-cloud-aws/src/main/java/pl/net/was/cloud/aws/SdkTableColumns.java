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

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ListMultimap;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.Type;
import pl.net.was.cloud.aws.SdkRowEncoder.ColumnWriter;
import software.amazon.awssdk.core.SdkField;
import software.amazon.awssdk.core.SdkPojo;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;
import static pl.net.was.cloud.aws.SdkColumns.mappedFields;

/**
 * Columns of a table mapped from the fields of an SDK object, and derived columns with values computed from the {@link SdkRow},
 * placed before the mapped columns or after one of them.
 */
public final class SdkTableColumns
{
    static final String INPUT_ONLY_COMMENT = "Only used by INSERT, always NULL when reading";

    private final List<ColumnMetadata> columnMetadata;
    private final List<ColumnWriter> columnWriters;

    private SdkTableColumns(List<ColumnMetadata> columnMetadata, List<ColumnWriter> columnWriters)
    {
        this.columnMetadata = ImmutableList.copyOf(columnMetadata);
        this.columnWriters = ImmutableList.copyOf(columnWriters);
    }

    /**
     * The model is an SDK model object or builder, like {@code Instance.builder()}.
     */
    public static SdkTableColumns of(SdkPojo model)
    {
        return builder(model).build();
    }

    public static Builder builder(SdkPojo model)
    {
        return new Builder(model);
    }

    public List<ColumnMetadata> columnMetadata()
    {
        return columnMetadata;
    }

    /**
     * Writers of the values of every column, in the same order as {@link #columnMetadata()}.
     */
    public List<ColumnWriter> columnWriters()
    {
        return columnWriters;
    }

    public static final class Builder
    {
        private final SdkPojo model;
        private final List<DerivedColumn> prependedColumns = new ArrayList<>();
        private final ListMultimap<String, DerivedColumn> insertedColumns = ArrayListMultimap.create();

        private Builder(SdkPojo model)
        {
            this.model = requireNonNull(model, "model is null");
        }

        /**
         * The value must be in the Trino native representation of the column type.
         */
        public Builder prepend(ColumnMetadata column, Function<SdkRow, Object> value)
        {
            prependedColumns.add(new DerivedColumn(column, value));
            return this;
        }

        /**
         * Input-only columns set request fields when inserting, but the SDK model of the listed resources doesn't have them,
         * so they're always NULL when reading.
         */
        public Builder prependInputOnly(String name, Type type)
        {
            return prepend(
                    ColumnMetadata.builder()
                            .setName(name)
                            .setType(type)
                            .setComment(Optional.of(INPUT_ONLY_COMMENT))
                            .build(),
                    _ -> null);
        }

        /**
         * The value must be in the Trino native representation of the column type.
         */
        public Builder insertAfter(String columnName, ColumnMetadata column, Function<SdkRow, Object> value)
        {
            insertedColumns.put(columnName, new DerivedColumn(column, value));
            return this;
        }

        public SdkTableColumns build()
        {
            List<SdkField<?>> fields = mappedFields(model);
            List<ColumnMetadata> fieldColumns = SdkColumns.columns(model);
            Set<String> fieldColumnNames = fieldColumns.stream().map(ColumnMetadata::getName).collect(toImmutableSet());
            checkArgument(fieldColumnNames.containsAll(insertedColumns.keySet()), "Columns %s are not all in %s", insertedColumns.keySet(), fieldColumnNames);

            List<ColumnMetadata> columns = new ArrayList<>();
            List<ColumnWriter> writers = new ArrayList<>();
            for (DerivedColumn column : prependedColumns) {
                columns.add(column.column());
                writers.add(column.writer());
            }
            for (int i = 0; i < fields.size(); i++) {
                ColumnMetadata fieldColumn = fieldColumns.get(i);
                columns.add(fieldColumn);
                writers.add(SdkRowEncoder.fieldWriter(fields.get(i)));
                for (DerivedColumn column : insertedColumns.get(fieldColumn.getName())) {
                    columns.add(column.column());
                    writers.add(column.writer());
                }
            }
            SdkColumns.checkUniqueNames(columns.stream().map(ColumnMetadata::getName).collect(toImmutableList()));
            return new SdkTableColumns(columns, writers);
        }
    }

    private record DerivedColumn(ColumnMetadata column, Function<SdkRow, Object> value)
    {
        private DerivedColumn
        {
            requireNonNull(column, "column is null");
            requireNonNull(value, "value is null");
        }

        ColumnWriter writer()
        {
            return SdkRowEncoder.derivedWriter(column.getType(), value);
        }
    }
}
