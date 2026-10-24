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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import io.airlift.slice.Slices;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.FixedSplitSource;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.VarcharType;
import pl.net.was.cloud.aws.filters.FilterApplier;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class AwsSplitManager
        implements ConnectorSplitManager
{
    private final AwsTables tables;

    @Inject
    public AwsSplitManager(AwsTables tables)
    {
        this.tables = requireNonNull(tables, "tables is null");
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle table,
            Set<ColumnHandle> dynamicFilterColumns,
            Constraint constraint)
    {
        AwsTableHandle tableHandle = (AwsTableHandle) table;
        AwsTable awsTable = tables.require(tableHandle.schemaTableName());
        return new FixedSplitSource(awsTable.filterApplier()
                .map(filterApplier -> splits(awsTable, filterApplier, tableHandle.constraint()))
                .orElse(ImmutableList.of(AwsConnectorSplit.WHOLE_TABLE)));
    }

    /**
     * Every filter with a split batch size divides the scan into batches of its values, so the splits are all combinations of the batches of every filter.
     */
    @VisibleForTesting
    static List<AwsConnectorSplit> splits(AwsTable table, FilterApplier filterApplier, TupleDomain<ColumnHandle> constraint)
    {
        if (constraint.getDomains().isEmpty()) {
            return ImmutableList.of(AwsConnectorSplit.WHOLE_TABLE);
        }
        Map<ColumnHandle, Domain> domains = constraint.getDomains().get();
        Map<String, Supplier<List<String>>> knownValues = filterApplier.getKnownValues();
        Map<String, Supplier<List<String>>> defaultValues = filterApplier.getDefaultValues();
        List<List<TupleDomain<ColumnHandle>>> columnBatches = filterApplier.getSplitBatchSizes().entrySet().stream()
                .map(batchSize -> columnBatches(
                        table.columnHandle(batchSize.getKey()),
                        batchSize.getValue(),
                        Optional.ofNullable(domains.get(table.columnHandle(batchSize.getKey()))),
                        Optional.ofNullable(knownValues.get(batchSize.getKey())),
                        Optional.ofNullable(defaultValues.get(batchSize.getKey()))))
                .collect(toImmutableList());
        return Lists.cartesianProduct(columnBatches).stream()
                .map(batches -> new AwsConnectorSplit(batches.stream().reduce(TupleDomain.all(), TupleDomain::intersect)))
                .collect(toImmutableList());
    }

    // a column that doesn't divide the scan has a single batch without a constraint
    private static List<TupleDomain<ColumnHandle>> columnBatches(
            AwsColumnHandle column,
            int batchSize,
            Optional<Domain> filteredDomain,
            Optional<Supplier<List<String>>> knownValues,
            Optional<Supplier<List<String>>> defaultValues)
    {
        Optional<Domain> domain = filteredDomain
                .map(filtered -> knownValues
                        .map(values -> knownDomain(column, values.get()).intersect(filtered))
                        .orElse(filtered))
                .or(() -> knownValues.or(() -> defaultValues).map(values -> knownDomain(column, values.get())));
        if (domain.isPresent() && domain.get().isNone()) {
            return ImmutableList.of();
        }
        if (domain.isEmpty() || domain.get().isNullAllowed() || !domain.get().getValues().isDiscreteSet()) {
            return ImmutableList.of(TupleDomain.all());
        }
        List<Object> values = domain.get().getValues().getDiscreteSet();
        // the table handle already selects these values, and a single request accepts all of them
        if (domain.equals(filteredDomain) && values.size() <= batchSize) {
            return ImmutableList.of(TupleDomain.all());
        }
        return Lists.partition(values, batchSize).stream()
                .map(batch -> TupleDomain.<ColumnHandle>withColumnDomains(Map.of(column, Domain.multipleValues(column.type(), batch))))
                .collect(toImmutableList());
    }

    private static Domain knownDomain(AwsColumnHandle column, List<String> values)
    {
        checkArgument(column.type() instanceof VarcharType, "Column %s with known values is not a varchar: %s", column.name(), column.type());
        return Domain.create(ValueSet.copyOf(column.type(), values.stream().map(Slices::utf8Slice).collect(toImmutableList())), false);
    }
}
