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
import com.google.common.collect.Lists;
import com.google.inject.Inject;
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
import pl.net.was.cloud.aws.filters.FilterApplier;

import java.util.List;
import java.util.Map;
import java.util.Set;

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
        // applyFilter isn't called for queries without a WHERE clause, so check the final table handle before any rows are read
        AwsTable awsTable = tables.require(tableHandle.schemaTableName());
        awsTable.filterApplier().ifPresent(filterApplier -> filterApplier.checkRequiredFilters(tableHandle));
        return new FixedSplitSource(awsTable.filterApplier()
                .map(filterApplier -> splits(awsTable, filterApplier, tableHandle.constraint()))
                .orElse(ImmutableList.of(AwsConnectorSplit.WHOLE_TABLE)));
    }

    // divides the values of the first filter that has more values than a single request accepts
    private static List<AwsConnectorSplit> splits(AwsTable table, FilterApplier filterApplier, TupleDomain<ColumnHandle> constraint)
    {
        if (constraint.getDomains().isEmpty()) {
            return ImmutableList.of(AwsConnectorSplit.WHOLE_TABLE);
        }
        for (Map.Entry<String, Integer> batchSize : filterApplier.getSplitBatchSizes().entrySet()) {
            ColumnHandle column = table.columnHandle(batchSize.getKey());
            Domain domain = constraint.getDomains().get().get(column);
            if (domain == null || domain.isNullAllowed() || !domain.getValues().isDiscreteSet()) {
                continue;
            }
            List<Object> values = domain.getValues().getDiscreteSet();
            if (values.size() <= batchSize.getValue()) {
                continue;
            }
            return Lists.partition(values, batchSize.getValue()).stream()
                    .map(batch -> new AwsConnectorSplit(TupleDomain.withColumnDomains(Map.of(column, Domain.multipleValues(domain.getType(), batch)))))
                    .collect(toImmutableList());
        }
        return ImmutableList.of(AwsConnectorSplit.WHOLE_TABLE);
    }
}
