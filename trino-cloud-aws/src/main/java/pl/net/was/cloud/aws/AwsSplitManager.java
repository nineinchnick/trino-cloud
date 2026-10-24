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
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.FixedSplitSource;

import java.util.Set;

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
        tables.require(tableHandle.schemaTableName())
                .filterApplier()
                .ifPresent(filterApplier -> filterApplier.checkRequiredFilters(tableHandle));
        return new FixedSplitSource(ImmutableList.of(new AwsConnectorSplit(tableHandle)));
    }
}
