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

package pl.net.was.cloud.aws.filters;

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import org.junit.jupiter.api.Test;
import pl.net.was.cloud.aws.AwsColumnHandle;
import pl.net.was.cloud.aws.AwsTableHandle;

import java.util.Map;
import java.util.Optional;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;
import static pl.net.was.cloud.aws.filters.FilterType.EQUAL;
import static pl.net.was.cloud.aws.filters.FilterType.GREATER_THAN_EQUAL;

final class TestFilterApplier
{
    private static final AwsColumnHandle ID = new AwsColumnHandle("id", VARCHAR);
    private static final AwsColumnHandle SIZE = new AwsColumnHandle("size", BIGINT);
    private static final Map<String, FilterType> SUPPORTED_FILTERS = Map.of("id", EQUAL, "size", GREATER_THAN_EQUAL);
    private static final Map<String, ColumnHandle> COLUMNS = Map.of("id", ID, "size", SIZE);
    private static final FilterApplier FILTER_APPLIER = () -> SUPPORTED_FILTERS;

    @Test
    void testPushesDownIntoUnconstrainedTable()
    {
        AwsTableHandle table = new AwsTableHandle(new SchemaTableName("ec2", "images"));

        Optional<ConstraintApplicationResult<ConnectorTableHandle>> result = applyFilter(table, Map.of(ID, Domain.singleValue(VARCHAR, utf8Slice("a"))));

        assertThat(result).isPresent();
        AwsTableHandle newTable = (AwsTableHandle) result.orElseThrow().getHandle();
        assertThat(newTable.constraint()).isEqualTo(TupleDomain.withColumnDomains(Map.of(ID, Domain.singleValue(VARCHAR, utf8Slice("a")))));
        assertThat(result.orElseThrow().getRemainingFilter().isAll()).isTrue();
        assertThat(FILTER_APPLIER.getFilter(ID, newTable.constraint())).isEqualTo("a");
    }

    @Test
    void testMissingFilterOnUnconstrainedTable()
    {
        AwsTableHandle table = new AwsTableHandle(new SchemaTableName("ec2", "images"));

        assertThat(FILTER_APPLIER.getFilter(ID, table.constraint())).isNull();
        assertThat(FILTER_APPLIER.getFilter(ID, table.constraint(), "default")).isEqualTo("default");
    }

    @Test
    void testDeclinesSameEqualFilter()
    {
        AwsTableHandle table = new AwsTableHandle(new SchemaTableName("ec2", "images"))
                .withConstraint(TupleDomain.withColumnDomains(Map.of(ID, Domain.singleValue(VARCHAR, utf8Slice("a")))));

        assertThat(applyFilter(table, Map.of(ID, Domain.singleValue(VARCHAR, utf8Slice("a"))))).isEmpty();
    }

    @Test
    void testDeclinesDifferentEqualFilter()
    {
        AwsTableHandle table = new AwsTableHandle(new SchemaTableName("ec2", "images"))
                .withConstraint(TupleDomain.withColumnDomains(Map.of(ID, Domain.singleValue(VARCHAR, utf8Slice("a")))));

        assertThat(applyFilter(table, Map.of(ID, Domain.singleValue(VARCHAR, utf8Slice("b"))))).isEmpty();
    }

    @Test
    void testIntersectsLowerBounds()
    {
        AwsTableHandle table = new AwsTableHandle(new SchemaTableName("s3", "objects"))
                .withConstraint(TupleDomain.withColumnDomains(Map.of(SIZE, greaterThanOrEqual(10))));

        Optional<ConstraintApplicationResult<ConnectorTableHandle>> result = applyFilter(table, Map.of(SIZE, greaterThanOrEqual(20)));

        assertThat(result).isPresent();
        AwsTableHandle newTable = (AwsTableHandle) result.orElseThrow().getHandle();
        assertThat(newTable.constraint()).isEqualTo(TupleDomain.withColumnDomains(Map.of(SIZE, greaterThanOrEqual(20))));
    }

    @Test
    void testDeclinesWeakerLowerBound()
    {
        AwsTableHandle table = new AwsTableHandle(new SchemaTableName("s3", "objects"))
                .withConstraint(TupleDomain.withColumnDomains(Map.of(SIZE, greaterThanOrEqual(20))));

        assertThat(applyFilter(table, Map.of(SIZE, greaterThanOrEqual(10)))).isEmpty();
    }

    @Test
    void testAddsFilterOnAnotherColumn()
    {
        AwsTableHandle table = new AwsTableHandle(new SchemaTableName("s3", "objects"))
                .withConstraint(TupleDomain.withColumnDomains(Map.of(ID, Domain.singleValue(VARCHAR, utf8Slice("a")))));

        Optional<ConstraintApplicationResult<ConnectorTableHandle>> result = applyFilter(table, Map.of(SIZE, greaterThanOrEqual(10)));

        assertThat(result).isPresent();
        AwsTableHandle newTable = (AwsTableHandle) result.orElseThrow().getHandle();
        assertThat(newTable.constraint()).isEqualTo(TupleDomain.withColumnDomains(Map.of(
                ID, Domain.singleValue(VARCHAR, utf8Slice("a")),
                SIZE, greaterThanOrEqual(10))));
    }

    private static Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(AwsTableHandle table, Map<ColumnHandle, Domain> domains)
    {
        return FILTER_APPLIER.applyFilter(table, COLUMNS, SUPPORTED_FILTERS, new Constraint(TupleDomain.withColumnDomains(domains)));
    }

    private static Domain greaterThanOrEqual(long value)
    {
        return Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(BIGINT, value)), false);
    }
}
