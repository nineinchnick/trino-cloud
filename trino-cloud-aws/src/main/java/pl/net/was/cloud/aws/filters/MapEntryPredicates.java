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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.FunctionName;
import io.trino.spi.expression.Variable;
import io.trino.spi.predicate.Domain;
import io.trino.spi.type.VarcharType;
import pl.net.was.cloud.aws.AwsColumnHandle;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.trino.plugin.base.expression.ConnectorExpressions.extractConjuncts;
import static io.trino.spi.expression.StandardFunctions.ARRAY_CONSTRUCTOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.IN_PREDICATE_FUNCTION_NAME;

/**
 * Finds predicates on the values of map entries in the expression of a constraint, like tags['Name'] = 'value',
 * which can't be a part of the constraint summary, because it only has domains of whole columns.
 */
final class MapEntryPredicates
{
    // the engine translates the map subscript operator to a call of the mangled operator name
    private static final Set<FunctionName> MAP_ENTRY_FUNCTIONS = Set.of(new FunctionName("$operator$subscript"), new FunctionName("element_at"));

    private MapEntryPredicates() {}

    /**
     * Returns the domains of the values of map entries, for equality and IN predicates on entries of the given map columns,
     * which have varchar values, like map_column['key'] = 'value' or element_at(map_column, 'key') IN ('value', 'other').
     */
    static Map<AwsColumnHandle, Domain> mapEntryDomains(Constraint constraint, Set<String> mapColumns)
    {
        if (mapColumns.isEmpty()) {
            return ImmutableMap.of();
        }
        Map<AwsColumnHandle, Domain> domains = new HashMap<>();
        for (ConnectorExpression conjunct : extractConjuncts(constraint.getExpression())) {
            mapEntryDomain(conjunct, constraint.getAssignments(), mapColumns)
                    .ifPresent(entry -> domains.merge(entry.getKey(), entry.getValue(), Domain::intersect));
        }
        return ImmutableMap.copyOf(domains);
    }

    private static Optional<Map.Entry<AwsColumnHandle, Domain>> mapEntryDomain(ConnectorExpression expression, Map<String, ColumnHandle> assignments, Set<String> mapColumns)
    {
        if (!(expression instanceof Call call) || call.getArguments().size() != 2) {
            return Optional.empty();
        }
        Optional<List<ConnectorExpression>> values = comparedValues(call);
        Optional<AwsColumnHandle> mapEntry = mapEntry(call.getArguments().getFirst(), assignments, mapColumns);
        if (values.isEmpty() || mapEntry.isEmpty()) {
            return Optional.empty();
        }
        ImmutableList.Builder<Slice> slices = ImmutableList.builder();
        for (ConnectorExpression value : values.get()) {
            if (!(value instanceof Constant constant) || !(constant.getValue() instanceof Slice slice)) {
                return Optional.empty();
            }
            slices.add(slice);
        }
        return Optional.of(Map.entry(mapEntry.get(), Domain.multipleValues(mapEntry.get().type(), slices.build())));
    }

    private static Optional<List<ConnectorExpression>> comparedValues(Call call)
    {
        ConnectorExpression right = call.getArguments().get(1);
        if (call.getFunctionName().equals(EQUAL_OPERATOR_FUNCTION_NAME)) {
            return Optional.of(List.of(right));
        }
        if (call.getFunctionName().equals(IN_PREDICATE_FUNCTION_NAME) && right instanceof Call array && array.getFunctionName().equals(ARRAY_CONSTRUCTOR_FUNCTION_NAME)) {
            return Optional.of(array.getArguments());
        }
        return Optional.empty();
    }

    private static Optional<AwsColumnHandle> mapEntry(ConnectorExpression expression, Map<String, ColumnHandle> assignments, Set<String> mapColumns)
    {
        if (!(expression instanceof Call call)
                || !MAP_ENTRY_FUNCTIONS.contains(call.getFunctionName())
                || call.getArguments().size() != 2
                || !(call.getArguments().get(0) instanceof Variable variable)
                || !(call.getArguments().get(1) instanceof Constant key)
                || !(key.getValue() instanceof Slice keyValue)
                || !(assignments.get(variable.getName()) instanceof AwsColumnHandle column)
                || !mapColumns.contains(column.name())) {
            return Optional.empty();
        }
        AwsColumnHandle mapEntry = AwsColumnHandle.mapEntry(column, keyValue.toStringUtf8());
        if (!(mapEntry.type() instanceof VarcharType)) {
            return Optional.empty();
        }
        return Optional.of(mapEntry);
    }
}
