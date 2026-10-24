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

import io.trino.spi.TrinoException;
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

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
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
    void testPushesDownInList()
    {
        AwsTableHandle table = new AwsTableHandle(new SchemaTableName("ec2", "images"));
        Domain inList = Domain.multipleValues(VARCHAR, List.of(utf8Slice("a"), utf8Slice("b")));

        Optional<ConstraintApplicationResult<ConnectorTableHandle>> result = applyFilter(table, Map.of(ID, inList));

        assertThat(result).isPresent();
        AwsTableHandle newTable = (AwsTableHandle) result.orElseThrow().getHandle();
        assertThat(newTable.constraint()).isEqualTo(TupleDomain.withColumnDomains(Map.of(ID, inList)));
        assertThat(FILTER_APPLIER.getFilterValues(ID, newTable.constraint())).containsExactly("a", "b");
    }

    @Test
    void testDeclinesNullFilters()
    {
        AwsTableHandle table = new AwsTableHandle(new SchemaTableName("ec2", "images"));
        Domain valueOrNull = Domain.create(ValueSet.of(VARCHAR, utf8Slice("a")), true);

        assertThat(applyFilter(table, Map.of(ID, Domain.onlyNull(VARCHAR)))).isEmpty();
        assertThat(applyFilter(table, Map.of(ID, Domain.notNull(VARCHAR)))).isEmpty();
        assertThat(applyFilter(table, Map.of(ID, valueOrNull))).isEmpty();
    }

    @Test
    void testDeclinesRanges()
    {
        AwsTableHandle table = new AwsTableHandle(new SchemaTableName("ec2", "images"));
        Domain range = Domain.create(ValueSet.ofRanges(Range.greaterThan(VARCHAR, utf8Slice("a"))), false);

        assertThat(applyFilter(table, Map.of(ID, range))).isEmpty();
    }

    @Test
    void testKeepsUnenforcedFilterInRemainingFilter()
    {
        FilterApplier filterApplier = new FilterApplier()
        {
            @Override
            public Map<String, FilterType> getSupportedFilters()
            {
                return SUPPORTED_FILTERS;
            }

            @Override
            public Set<String> getUnenforcedFilters()
            {
                return Set.of("id");
            }
        };
        TupleDomain<ColumnHandle> summary = TupleDomain.withColumnDomains(Map.of(ID, Domain.singleValue(VARCHAR, utf8Slice("a"))));
        AwsTableHandle table = new AwsTableHandle(new SchemaTableName("ec2", "images"));

        Optional<ConstraintApplicationResult<ConnectorTableHandle>> result = filterApplier.applyFilter(table, COLUMNS, SUPPORTED_FILTERS, new Constraint(summary));

        assertThat(result).isPresent();
        AwsTableHandle newTable = (AwsTableHandle) result.orElseThrow().getHandle();
        assertThat(newTable.constraint()).isEqualTo(summary);
        assertThat(result.orElseThrow().getRemainingFilter()).isEqualTo(summary);
        assertThat(filterApplier.applyFilter(newTable, COLUMNS, SUPPORTED_FILTERS, new Constraint(summary))).isEmpty();
    }

    @Test
    void testFilterValues()
    {
        AwsTableHandle table = new AwsTableHandle(new SchemaTableName("ec2", "images"));

        assertThat(FILTER_APPLIER.getFilterValues(ID, table.constraint())).isEmpty();
        assertThat(FILTER_APPLIER.getFilterValues(ID, TupleDomain.withColumnDomains(Map.of(ID, Domain.singleValue(VARCHAR, utf8Slice("a"))))))
                .containsExactly("a");
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
    void testRemovesLowerBoundFromRemainingFilter()
    {
        AwsTableHandle table = new AwsTableHandle(new SchemaTableName("s3", "objects"));

        Optional<ConstraintApplicationResult<ConnectorTableHandle>> result = applyFilter(table, Map.of(SIZE, greaterThanOrEqual(10)));

        assertThat(result).isPresent();
        assertThat(result.orElseThrow().getRemainingFilter().isAll()).isTrue();
    }

    @Test
    void testKeepsUpperBoundInRemainingFilter()
    {
        AwsTableHandle table = new AwsTableHandle(new SchemaTableName("s3", "objects"));
        Domain range = Domain.create(ValueSet.ofRanges(Range.range(BIGINT, 10L, true, 20L, false)), false);
        TupleDomain<ColumnHandle> summary = TupleDomain.withColumnDomains(Map.of(SIZE, range));

        Optional<ConstraintApplicationResult<ConnectorTableHandle>> result = FILTER_APPLIER.applyFilter(table, COLUMNS, SUPPORTED_FILTERS, new Constraint(summary));

        assertThat(result).isPresent();
        AwsTableHandle newTable = (AwsTableHandle) result.orElseThrow().getHandle();
        assertThat(newTable.constraint()).isEqualTo(TupleDomain.withColumnDomains(Map.of(SIZE, greaterThanOrEqual(10))));
        assertThat(result.orElseThrow().getRemainingFilter()).isEqualTo(summary);
        assertThat(FILTER_APPLIER.applyFilter(newTable, COLUMNS, SUPPORTED_FILTERS, new Constraint(summary))).isEmpty();
    }

    @Test
    void testDeclinesLowerBoundAllowingNulls()
    {
        AwsTableHandle table = new AwsTableHandle(new SchemaTableName("s3", "objects"));
        Domain lowerBoundOrNull = Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(BIGINT, 10L)), true);

        assertThat(applyFilter(table, Map.of(SIZE, lowerBoundOrNull))).isEmpty();
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

    @Test
    void testRequiredFilters()
    {
        FilterApplier filterApplier = new FilterApplier()
        {
            @Override
            public Map<String, FilterType> getSupportedFilters()
            {
                return SUPPORTED_FILTERS;
            }

            @Override
            public Set<String> getRequiredFilters()
            {
                return Set.of("id");
            }
        };
        AwsTableHandle table = new AwsTableHandle(new SchemaTableName("s3", "objects"));

        assertThatThrownBy(() -> filterApplier.checkRequiredFilters(table))
                .isInstanceOf(TrinoException.class)
                .hasMessage("Missing required filter on s3.objects.id. Add a condition like id = '...' or id IN ('...', '...') to the WHERE clause");
        assertThatThrownBy(() -> filterApplier.checkRequiredFilters(table.withConstraint(TupleDomain.withColumnDomains(Map.of(SIZE, greaterThanOrEqual(10))))))
                .isInstanceOf(TrinoException.class);
        filterApplier.checkRequiredFilters(table.withConstraint(TupleDomain.withColumnDomains(Map.of(ID, Domain.singleValue(VARCHAR, utf8Slice("a"))))));
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
