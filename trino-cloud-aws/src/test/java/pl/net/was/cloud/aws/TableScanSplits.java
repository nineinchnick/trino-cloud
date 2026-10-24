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

import io.trino.execution.QueryStats;
import io.trino.operator.OperatorStats;

import static com.google.common.collect.MoreCollectors.onlyElement;

final class TableScanSplits
{
    private TableScanSplits() {}

    // every split is processed by a separate driver of the table scan
    static long count(QueryStats stats)
    {
        return stats.getOperatorSummaries().stream()
                .filter(summary -> summary.getOperatorType().startsWith("TableScan") || summary.getOperatorType().startsWith("ScanFilterAndProject"))
                .map(OperatorStats::getTotalDrivers)
                .collect(onlyElement());
    }
}
