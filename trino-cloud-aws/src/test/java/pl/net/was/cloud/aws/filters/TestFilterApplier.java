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
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.FunctionName;
import io.trino.spi.expression.Variable;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.TypeOperators;
import org.junit.jupiter.api.Test;
import pl.net.was.cloud.aws.AwsColumnHandle;
import pl.net.was.cloud.aws.AwsTableHandle;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.expression.StandardFunctions.AND_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.ARRAY_CONSTRUCTOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.IN_PREDICATE_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.NOT_EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static pl.net.was.cloud.aws.filters.FilterType.EQUAL;
import static pl.net.was.cloud.aws.filters.FilterType.GREATER_THAN_EQUAL;
import static pl.net.was.cloud.aws.filters.FilterType.PREFIX;

final class TestFilterApplier
{
    private static final AwsColumnHandle ID = new AwsColumnHandle("id", VARCHAR);
    private static final AwsColumnHandle SIZE = new AwsColumnHandle("size", BIGINT);
    private static final AwsColumnHandle TAGS = new AwsColumnHandle("tags", new MapType(VARCHAR, VARCHAR, new TypeOperators()));
    private static final Map<String, FilterType> SUPPORTED_FILTERS = Map.of("id", EQUAL, "size", GREATER_THAN_EQUAL);
    private static final Map<String, ColumnHandle> COLUMNS = Map.of("id", ID, "size", SIZE);
    private static final FilterApplier FILTER_APPLIER = () -> SUPPORTED_FILTERS;
    private static final AwsColumnHandle KEY = new AwsColumnHandle("key", VARCHAR);
    private static final Map<String, FilterType> PREFIX_FILTERS = Map.of("key", PREFIX);
    private static final FilterApplier PREFIX_FILTER_APPLIER = () -> PREFIX_FILTERS;

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

    @Test
    void testPushesDownPrefix()
    {
        AwsTableHandle table = new AwsTableHandle(new SchemaTableName("s3", "objects"));
        // the engine translates key LIKE 'dir/%' to a range
        Domain like = varcharRange("dir/", "dir0");

        Optional<ConstraintApplicationResult<ConnectorTableHandle>> result = PREFIX_FILTER_APPLIER.applyFilter(table, Map.of("key", KEY), PREFIX_FILTERS, new Constraint(TupleDomain.withColumnDomains(Map.of(KEY, like))));

        AwsTableHandle newTable = (AwsTableHandle) result.orElseThrow().getHandle();
        assertThat(newTable.constraint()).isEqualTo(TupleDomain.withColumnDomains(Map.of(KEY, like)));
        assertThat(result.orElseThrow().getRemainingFilter()).isEqualTo(TupleDomain.withColumnDomains(Map.of(KEY, like)));
        assertThat(PREFIX_FILTER_APPLIER.getFilterPrefix(KEY, newTable.constraint())).hasValue("dir/");
        assertThat(PREFIX_FILTER_APPLIER.getFilterValues(KEY, newTable.constraint())).isEmpty();
        // a range without a common prefix of its values would list all keys
        assertThat(PREFIX_FILTER_APPLIER.applyFilter(table, Map.of("key", KEY), PREFIX_FILTERS, new Constraint(TupleDomain.withColumnDomains(Map.of(KEY, varcharRange("a", "c")))))).isEmpty();
    }

    @Test
    void testPushesDownPrefixOfValues()
    {
        AwsTableHandle table = new AwsTableHandle(new SchemaTableName("s3", "objects"));
        Domain values = Domain.multipleValues(VARCHAR, List.of(utf8Slice("dir/a.txt"), utf8Slice("dir/b.txt")));

        AwsTableHandle newTable = (AwsTableHandle) PREFIX_FILTER_APPLIER.applyFilter(table, Map.of("key", KEY), PREFIX_FILTERS, new Constraint(TupleDomain.withColumnDomains(Map.of(KEY, values))))
                .orElseThrow()
                .getHandle();

        assertThat(newTable.constraint()).isEqualTo(TupleDomain.withColumnDomains(Map.of(KEY, values)));
        assertThat(PREFIX_FILTER_APPLIER.getFilterPrefix(KEY, newTable.constraint())).hasValue("dir/");
        assertThat(PREFIX_FILTER_APPLIER.getFilterValues(KEY, newTable.constraint())).containsExactly("dir/a.txt", "dir/b.txt");
    }

    @Test
    void testFilterPrefix()
    {
        assertThat(filterPrefix(Domain.singleValue(VARCHAR, utf8Slice("a.txt")))).hasValue("a.txt");
        assertThat(filterPrefix(varcharRange("logs/2024", "logs/2025"))).hasValue("logs/2024");
        assertThat(filterPrefix(varcharRange("logs/2024-01", "logs/2024-02"))).hasValue("logs/2024-01");
        // the engine increments the last ASCII character of a LIKE prefix to get the high bound
        assertThat(filterPrefix(varcharRange("diré", "dis"))).hasValue("dir");
        assertThat(filterPrefix(Domain.create(ValueSet.ofRanges(Range.range(VARCHAR, utf8Slice("dir/"), true, utf8Slice("dir0"), true)), false))).hasValue("dir");
        assertThat(filterPrefix(varcharRange("a", "c"))).isEmpty();
        assertThat(filterPrefix(Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(VARCHAR, utf8Slice("dir/"))), false))).isEmpty();
        assertThat(filterPrefix(Domain.create(ValueSet.ofRanges(Range.lessThan(VARCHAR, utf8Slice("dir/"))), false))).isEmpty();
        assertThat(PREFIX_FILTER_APPLIER.getFilterPrefix(KEY, TupleDomain.all())).isEmpty();
    }

    @Test
    void testPushesDownMapEntryEquality()
    {
        AwsTableHandle table = new AwsTableHandle(new SchemaTableName("ec2", "instances"));
        ConnectorExpression expression = equal(subscript("$operator$subscript", "Name"), varchar("a"));

        Optional<ConstraintApplicationResult<ConnectorTableHandle>> result = applyMapEntryFilter(table, expression);

        assertThat(result).isPresent();
        AwsTableHandle newTable = (AwsTableHandle) result.orElseThrow().getHandle();
        assertThat(newTable.constraint()).isEqualTo(TupleDomain.withColumnDomains(Map.of(
                AwsColumnHandle.mapEntry(TAGS, "Name"), Domain.singleValue(VARCHAR, utf8Slice("a")))));
        // the engine still applies the predicate, because EC2 filter values treat * and ? as wildcards
        assertThat(result.orElseThrow().getRemainingExpression()).hasValue(expression);
        // pushing down the same predicate again would make the planner loop
        assertThat(applyMapEntryFilter(newTable, expression)).isEmpty();
    }

    @Test
    void testPushesDownMapEntryInList()
    {
        AwsTableHandle table = new AwsTableHandle(new SchemaTableName("ec2", "instances"));
        ConnectorExpression expression = new Call(BOOLEAN, IN_PREDICATE_FUNCTION_NAME, List.of(
                subscript("element_at", "Name"),
                new Call(new ArrayType(VARCHAR), ARRAY_CONSTRUCTOR_FUNCTION_NAME, List.of(varchar("a"), varchar("b")))));

        AwsTableHandle newTable = (AwsTableHandle) applyMapEntryFilter(table, expression).orElseThrow().getHandle();

        assertThat(newTable.constraint()).isEqualTo(TupleDomain.withColumnDomains(Map.of(
                AwsColumnHandle.mapEntry(TAGS, "Name"), Domain.multipleValues(VARCHAR, List.of(utf8Slice("a"), utf8Slice("b"))))));
    }

    @Test
    void testPushesDownMapEntriesInConjuncts()
    {
        AwsTableHandle table = new AwsTableHandle(new SchemaTableName("ec2", "instances"));
        ConnectorExpression expression = new Call(BOOLEAN, AND_FUNCTION_NAME, List.of(
                equal(subscript("$operator$subscript", "Name"), varchar("a")),
                equal(subscript("$operator$subscript", "Owner"), varchar("b")),
                equal(subscript("$operator$subscript", "Ignored"), new Variable("id", VARCHAR))));

        AwsTableHandle newTable = (AwsTableHandle) applyMapEntryFilter(table, expression).orElseThrow().getHandle();

        assertThat(newTable.constraint()).isEqualTo(TupleDomain.withColumnDomains(Map.of(
                AwsColumnHandle.mapEntry(TAGS, "Name"), Domain.singleValue(VARCHAR, utf8Slice("a")),
                AwsColumnHandle.mapEntry(TAGS, "Owner"), Domain.singleValue(VARCHAR, utf8Slice("b")))));
    }

    @Test
    void testIgnoresOtherMapEntryPredicates()
    {
        AwsTableHandle table = new AwsTableHandle(new SchemaTableName("ec2", "instances"));

        assertThat(applyMapEntryFilter(table, new Call(BOOLEAN, NOT_EQUAL_OPERATOR_FUNCTION_NAME, List.of(subscript("$operator$subscript", "Name"), varchar("a"))))).isEmpty();
        assertThat(applyMapEntryFilter(table, equal(subscript("$operator$subscript", "Name"), new Variable("id", VARCHAR)))).isEmpty();
        assertThat(applyMapEntryFilter(table, equal(new Call(VARCHAR, new FunctionName("lower"), List.of(new Variable("id", VARCHAR))), varchar("a")))).isEmpty();
        assertThat(FILTER_APPLIER.applyFilter(table, COLUMNS, SUPPORTED_FILTERS, new Constraint(
                TupleDomain.all(),
                equal(subscript("$operator$subscript", "Name"), varchar("a")),
                Map.of("id", ID, "tags", TAGS)))).isEmpty();
    }

    private static Optional<String> filterPrefix(Domain domain)
    {
        return PREFIX_FILTER_APPLIER.getFilterPrefix(KEY, TupleDomain.withColumnDomains(Map.of(KEY, domain)));
    }

    private static Domain varcharRange(String low, String high)
    {
        return Domain.create(ValueSet.ofRanges(Range.range(VARCHAR, utf8Slice(low), true, utf8Slice(high), false)), false);
    }

    private static Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyMapEntryFilter(AwsTableHandle table, ConnectorExpression expression)
    {
        FilterApplier filterApplier = new FilterApplier()
        {
            @Override
            public Map<String, FilterType> getSupportedFilters()
            {
                return SUPPORTED_FILTERS;
            }

            @Override
            public Set<String> getMapEntryFilters()
            {
                return Set.of("tags");
            }
        };
        return filterApplier.applyFilter(table, COLUMNS, SUPPORTED_FILTERS, new Constraint(TupleDomain.all(), expression, Map.of("id", ID, "tags", TAGS)));
    }

    private static Call subscript(String functionName, String key)
    {
        return new Call(VARCHAR, new FunctionName(functionName), List.of(new Variable("tags", TAGS.type()), varchar(key)));
    }

    private static Call equal(ConnectorExpression left, ConnectorExpression right)
    {
        return new Call(BOOLEAN, EQUAL_OPERATOR_FUNCTION_NAME, List.of(left, right));
    }

    private static Constant varchar(String value)
    {
        return new Constant(utf8Slice(value), VARCHAR);
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
