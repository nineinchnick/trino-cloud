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
import io.trino.spi.connector.ColumnMetadata;
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
import pl.net.was.cloud.aws.filters.TableFilters.MissingValues;
import pl.net.was.cloud.aws.filters.TableFilters.Partition;
import software.amazon.awssdk.services.ec2.model.DescribeNatGatewaysRequest;
import software.amazon.awssdk.services.ec2.model.DescribeVpcsRequest;
import software.amazon.awssdk.services.ec2.model.Filter;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.IntStream;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.expression.StandardFunctions.AND_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.ARRAY_CONSTRUCTOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.IN_PREDICATE_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.NOT_EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static pl.net.was.cloud.aws.filters.ColumnFilter.ec2Filter;
import static pl.net.was.cloud.aws.filters.ColumnFilter.partition;
import static pl.net.was.cloud.aws.filters.ColumnFilter.requestMember;
import static pl.net.was.cloud.aws.filters.ColumnFilter.tagFilters;
import static pl.net.was.cloud.aws.filters.TableFilters.BATCH_SIZE;

final class TestTableFilters
{
    private static final SchemaTableName TABLE_NAME = new SchemaTableName("ec2", "vpcs");
    private static final MapType TAGS_TYPE = new MapType(VARCHAR, VARCHAR, new TypeOperators());
    private static final List<ColumnMetadata> COLUMNS = List.of(
            new ColumnMetadata("region", VARCHAR),
            new ColumnMetadata("vpc_id", VARCHAR),
            new ColumnMetadata("state", VARCHAR),
            new ColumnMetadata("is_default", BOOLEAN),
            new ColumnMetadata("max_results", INTEGER),
            new ColumnMetadata("created", TIMESTAMP_TZ_MILLIS),
            new ColumnMetadata("tags", TAGS_TYPE));
    private static final AwsColumnHandle REGION = new AwsColumnHandle("region", VARCHAR);
    private static final AwsColumnHandle VPC_ID = new AwsColumnHandle("vpc_id", VARCHAR);
    private static final AwsColumnHandle STATE = new AwsColumnHandle("state", VARCHAR);
    private static final AwsColumnHandle IS_DEFAULT = new AwsColumnHandle("is_default", BOOLEAN);
    private static final AwsColumnHandle MAX_RESULTS = new AwsColumnHandle("max_results", INTEGER);
    private static final AwsColumnHandle TAGS = new AwsColumnHandle("tags", TAGS_TYPE);
    private static final TableFilters FILTERS = TableFilters.create(
            TABLE_NAME,
            COLUMNS,
            List.of(
                    partition("region"),
                    requestMember("vpc_id", "VpcIds").skipMissingValues(Set.of("InvalidVpcID.NotFound")),
                    ec2Filter("state", "state"),
                    ec2Filter("is_default", "is-default"),
                    requestMember("max_results", "MaxResults")),
            DescribeVpcsRequest.builder().sdkFields());
    private static final TableFilters TAG_FILTERS = TableFilters.create(TABLE_NAME, COLUMNS, List.of(tagFilters("tags")), DescribeVpcsRequest.builder().sdkFields());
    private static final AwsTableHandle TABLE = new AwsTableHandle(TABLE_NAME);

    @Test
    void testPushesDownEnforcedFilter()
    {
        Domain inList = values("vpc-1", "vpc-2");

        Optional<ConstraintApplicationResult<ConnectorTableHandle>> result = applyFilter(TABLE, Map.of(VPC_ID, inList));

        assertThat(result).isPresent();
        assertThat(((AwsTableHandle) result.orElseThrow().getHandle()).constraint()).isEqualTo(TupleDomain.withColumnDomains(Map.of(VPC_ID, inList)));
        assertThat(result.orElseThrow().getRemainingFilter().isAll()).isTrue();
    }

    // EC2 filter values treat * and ? as wildcards
    @Test
    void testKeepsUnenforcedFilterInRemainingFilter()
    {
        TupleDomain<ColumnHandle> summary = TupleDomain.withColumnDomains(Map.of(STATE, values("available")));

        Optional<ConstraintApplicationResult<ConnectorTableHandle>> result = FILTERS.applyFilter(TABLE, new Constraint(summary));

        AwsTableHandle newTable = (AwsTableHandle) result.orElseThrow().getHandle();
        assertThat(newTable.constraint()).isEqualTo(summary);
        assertThat(result.orElseThrow().getRemainingFilter()).isEqualTo(summary);
        // pushing down the same predicate again would make the planner loop
        assertThat(FILTERS.applyFilter(newTable, new Constraint(summary))).isEmpty();
    }

    @Test
    void testDeclinesNullsAndRanges()
    {
        assertThat(applyFilter(TABLE, Map.of(VPC_ID, Domain.onlyNull(VARCHAR)))).isEmpty();
        assertThat(applyFilter(TABLE, Map.of(VPC_ID, Domain.notNull(VARCHAR)))).isEmpty();
        assertThat(applyFilter(TABLE, Map.of(VPC_ID, Domain.create(ValueSet.of(VARCHAR, utf8Slice("vpc-1")), true)))).isEmpty();
        assertThat(applyFilter(TABLE, Map.of(VPC_ID, Domain.create(ValueSet.ofRanges(Range.greaterThan(VARCHAR, utf8Slice("vpc-1"))), false)))).isEmpty();
    }

    @Test
    void testDeclinesFilterOnColumnWithFilter()
    {
        AwsTableHandle table = TABLE.withConstraint(TupleDomain.withColumnDomains(Map.of(VPC_ID, values("vpc-1"))));

        assertThat(applyFilter(table, Map.of(VPC_ID, values("vpc-1")))).isEmpty();
        assertThat(applyFilter(table, Map.of(VPC_ID, values("vpc-2")))).isEmpty();
    }

    @Test
    void testIgnoresColumnsWithoutFilters()
    {
        assertThat(applyFilter(TABLE, Map.of(new AwsColumnHandle("created", TIMESTAMP_TZ_MILLIS), Domain.singleValue(TIMESTAMP_TZ_MILLIS, 0L)))).isEmpty();
    }

    @Test
    void testAddsFilterOnAnotherColumn()
    {
        AwsTableHandle table = TABLE.withConstraint(TupleDomain.withColumnDomains(Map.of(VPC_ID, values("vpc-1"))));

        Optional<ConstraintApplicationResult<ConnectorTableHandle>> result = applyFilter(table, Map.of(VPC_ID, values("vpc-1"), REGION, values("us-east-1")));

        assertThat(((AwsTableHandle) result.orElseThrow().getHandle()).constraint()).isEqualTo(TupleDomain.withColumnDomains(Map.of(
                VPC_ID, values("vpc-1"),
                REGION, values("us-east-1"))));
    }

    @Test
    void testSetsRequestMembersAndEc2Filters()
    {
        DescribeVpcsRequest.Builder request = DescribeVpcsRequest.builder();

        FILTERS.applyTo(request, TupleDomain.withColumnDomains(Map.of(
                REGION, values("us-east-1"),
                VPC_ID, values("vpc-1", "vpc-2"),
                STATE, values("available", "pending"),
                IS_DEFAULT, Domain.singleValue(BOOLEAN, true),
                MAX_RESULTS, Domain.singleValue(INTEGER, 5L))));

        assertThat(request.build()).isEqualTo(DescribeVpcsRequest.builder()
                .vpcIds("vpc-1", "vpc-2")
                .filters(
                        Filter.builder().name("state").values("available", "pending").build(),
                        Filter.builder().name("is-default").values("true").build())
                .maxResults(5)
                .build());
    }

    @Test
    void testEc2FiltersOnColumnsWithMatchingNames()
    {
        List<ColumnFilter> ec2Filters = ColumnFilter.ec2Filters("vpc-id", "is-default");
        DescribeVpcsRequest.Builder request = DescribeVpcsRequest.builder();

        TableFilters.create(TABLE_NAME, COLUMNS, ec2Filters, DescribeVpcsRequest.builder().sdkFields())
                .applyTo(request, TupleDomain.withColumnDomains(Map.of(VPC_ID, values("vpc-1"), IS_DEFAULT, Domain.singleValue(BOOLEAN, false))));

        assertThat(ec2Filters).extracting(ColumnFilter::column).containsExactly("vpc_id", "is_default");
        assertThat(request.build().filters()).containsExactly(
                Filter.builder().name("vpc-id").values("vpc-1").build(),
                Filter.builder().name("is-default").values("false").build());
    }

    // the list of filters of DescribeNatGateways is named Filter
    @Test
    void testEc2FiltersInDifferentlyNamedMember()
    {
        DescribeNatGatewaysRequest.Builder request = DescribeNatGatewaysRequest.builder();

        TableFilters.create(new SchemaTableName("ec2", "nat_gateways"), COLUMNS, List.of(ec2Filter("state", "state")), DescribeNatGatewaysRequest.builder().sdkFields())
                .applyTo(request, TupleDomain.withColumnDomains(Map.of(STATE, values("available"))));

        assertThat(request.build().filter()).containsExactly(Filter.builder().name("state").values("available").build());
    }

    @Test
    void testRequestWithoutFilters()
    {
        DescribeVpcsRequest.Builder request = DescribeVpcsRequest.builder();

        FILTERS.applyTo(request, TupleDomain.all());

        assertThat(request.build()).isEqualTo(DescribeVpcsRequest.builder().build());
        assertThat(request.build().hasFilters()).isFalse();
    }

    // the split manager divides scans into a split for every value of a scalar member
    @Test
    void testScalarMemberAcceptsSingleValue()
    {
        TableFilters filters = TableFilters.create(
                new SchemaTableName("s3", "objects"),
                List.of(new ColumnMetadata("bucket_name", VARCHAR)),
                List.of(requestMember("bucket_name", "Bucket")),
                ListObjectsV2Request.builder().sdkFields());
        AwsColumnHandle bucketName = new AwsColumnHandle("bucket_name", VARCHAR);
        ListObjectsV2Request.Builder request = ListObjectsV2Request.builder();

        filters.applyTo(request, TupleDomain.withColumnDomains(Map.of(bucketName, values("a"))));

        assertThat(request.build().bucket()).isEqualTo("a");
        assertThat(filters.splitConstraints(TupleDomain.withColumnDomains(Map.of(bucketName, values("a", "b")))))
                .containsExactly(
                        TupleDomain.withColumnDomains(Map.of(bucketName, values("a"))),
                        TupleDomain.withColumnDomains(Map.of(bucketName, values("b"))));
        assertThatThrownBy(() -> filters.applyTo(ListObjectsV2Request.builder(), TupleDomain.withColumnDomains(Map.of(bucketName, values("a", "b")))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Filter on bucket_name has many values for the Bucket member");
    }

    @Test
    void testPartitionAndRequestValues()
    {
        TupleDomain<ColumnHandle> constraint = TupleDomain.withColumnDomains(Map.of(
                REGION, values("us-east-1"),
                VPC_ID, values("vpc-1"),
                MAX_RESULTS, Domain.singleValue(INTEGER, 5L)));

        assertThat(FILTERS.partition(constraint)).hasValue(new Partition("region", "us-east-1"));
        assertThat(FILTERS.partition(TupleDomain.all())).isEmpty();
        // only the values of filters that accept a single value per request are the same for every row
        assertThat(FILTERS.requestValues(constraint)).isEqualTo(Map.of(
                "region", utf8Slice("us-east-1"),
                "max_results", 5L));
    }

    @Test
    void testMissingValues()
    {
        assertThat(FILTERS.missingValues(TupleDomain.withColumnDomains(Map.of(VPC_ID, values("vpc-1", "vpc-2")))))
                .hasValue(new MissingValues(VPC_ID, List.of(utf8Slice("vpc-1"), utf8Slice("vpc-2")), Set.of("InvalidVpcID.NotFound")));
        assertThat(FILTERS.missingValues(TupleDomain.withColumnDomains(Map.of(STATE, values("available"))))).isEmpty();
    }

    @Test
    void testDefaultValuesUnlessFiltered()
    {
        TableFilters filters = TableFilters.create(
                TABLE_NAME,
                COLUMNS,
                List.of(
                        requestMember("vpc_id", "VpcIds"),
                        ec2Filter("state", "state").defaultValues(() -> List.of("available"), "vpc_id")),
                DescribeVpcsRequest.builder().sdkFields());

        assertThat(filters.splitConstraints(TupleDomain.all()))
                .containsExactly(TupleDomain.withColumnDomains(Map.of(STATE, values("available"))));
        assertThat(filters.splitConstraints(TupleDomain.withColumnDomains(Map.of(VPC_ID, values("vpc-1")))))
                .containsExactly(TupleDomain.all());
        // unlike known values, default values don't restrict the filter
        assertThat(filters.splitConstraints(TupleDomain.withColumnDomains(Map.of(STATE, values("pending")))))
                .containsExactly(TupleDomain.all());
    }

    @Test
    void testPushesDownTagFilters()
    {
        ConnectorExpression expression = new Call(BOOLEAN, AND_FUNCTION_NAME, List.of(
                equal(subscript("$operator$subscript", "Name"), varchar("a")),
                new Call(BOOLEAN, IN_PREDICATE_FUNCTION_NAME, List.of(
                        subscript("element_at", "Owner"),
                        new Call(new ArrayType(VARCHAR), ARRAY_CONSTRUCTOR_FUNCTION_NAME, List.of(varchar("b"), varchar("c"))))),
                equal(subscript("$operator$subscript", "Ignored"), new Variable("vpc_id", VARCHAR))));

        Optional<ConstraintApplicationResult<ConnectorTableHandle>> result = applyTagFilter(TABLE, expression);

        AwsTableHandle newTable = (AwsTableHandle) result.orElseThrow().getHandle();
        assertThat(newTable.constraint()).isEqualTo(TupleDomain.withColumnDomains(Map.of(
                AwsColumnHandle.mapEntry(TAGS, "Name"), values("a"),
                AwsColumnHandle.mapEntry(TAGS, "Owner"), values("b", "c"))));
        // the engine still applies the predicate, because EC2 filter values treat * and ? as wildcards
        assertThat(result.orElseThrow().getRemainingExpression()).hasValue(expression);
        // pushing down the same predicate again would make the planner loop
        assertThat(applyTagFilter(newTable, expression)).isEmpty();

        DescribeVpcsRequest.Builder request = DescribeVpcsRequest.builder();
        TAG_FILTERS.applyTo(request, newTable.constraint());
        assertThat(request.build().filters()).containsExactlyInAnyOrder(
                Filter.builder().name("tag:Name").values("a").build(),
                Filter.builder().name("tag:Owner").values("b", "c").build());
    }

    @Test
    void testIgnoresOtherTagPredicates()
    {
        assertThat(applyTagFilter(TABLE, new Call(BOOLEAN, NOT_EQUAL_OPERATOR_FUNCTION_NAME, List.of(subscript("$operator$subscript", "Name"), varchar("a"))))).isEmpty();
        assertThat(applyTagFilter(TABLE, equal(subscript("$operator$subscript", "Name"), new Variable("vpc_id", VARCHAR)))).isEmpty();
        assertThat(applyTagFilter(TABLE, equal(new Call(VARCHAR, new FunctionName("lower"), List.of(new Variable("vpc_id", VARCHAR))), varchar("a")))).isEmpty();
        // the map column of a table without tag filters
        assertThat(FILTERS.applyFilter(TABLE, new Constraint(TupleDomain.all(), equal(subscript("$operator$subscript", "Name"), varchar("a")), Map.of("vpc_id", VPC_ID, "tags", TAGS))))
                .isEmpty();
    }

    @Test
    void testSplitsByTagFilterValues()
    {
        List<Object> names = IntStream.range(0, BATCH_SIZE + 1)
                .mapToObj(value -> (Object) utf8Slice("name-%03d".formatted(value)))
                .toList();
        AwsColumnHandle name = AwsColumnHandle.mapEntry(TAGS, "Name");

        assertThat(TAG_FILTERS.splitConstraints(TupleDomain.withColumnDomains(Map.of(name, Domain.multipleValues(VARCHAR, names)))))
                .containsExactly(
                        TupleDomain.withColumnDomains(Map.of(name, Domain.multipleValues(VARCHAR, names.subList(0, BATCH_SIZE)))),
                        TupleDomain.withColumnDomains(Map.of(name, Domain.multipleValues(VARCHAR, names.subList(BATCH_SIZE, names.size())))));
        assertThat(TAG_FILTERS.splitConstraints(TupleDomain.all()))
                .containsExactly(TupleDomain.all());
    }

    @Test
    void testRejectsInvalidFilters()
    {
        assertThatThrownBy(() -> create(requestMember("unknown", "VpcIds")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Filter column unknown does not exist in ec2.vpcs");
        assertThatThrownBy(() -> create(requestMember("vpc_id", "Unknown")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Request of ec2.vpcs does not have a Unknown member for the filter on vpc_id");
        assertThatThrownBy(() -> create(requestMember("is_default", "VpcIds")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Column is_default of type boolean can't set the VpcIds member of the request of ec2.vpcs");
        assertThatThrownBy(() -> create(ec2Filter("created", "created")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Column created for EC2 filter created has an unsupported type: timestamp(3) with time zone");
        assertThatThrownBy(() -> create(partition("region"), partition("vpc_id")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("ec2.vpcs has many partition filters");
        assertThatThrownBy(() -> create(ec2Filter("vpc_id", "vpc-id"), requestMember("vpc_id", "VpcIds")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("ec2.vpcs has many filters on the same column");
        assertThatThrownBy(() -> create(tagFilters("vpc_id")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Column vpc_id for tag filters is not a map with varchar keys and values: varchar");
        assertThatThrownBy(() -> TableFilters.create(TABLE_NAME, COLUMNS, List.of(ec2Filter("state", "state")), ListObjectsV2Request.builder().sdkFields()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Request of ec2.vpcs does not have EC2 filters for the filter on state");
    }

    private static Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyTagFilter(AwsTableHandle table, ConnectorExpression expression)
    {
        return TAG_FILTERS.applyFilter(table, new Constraint(TupleDomain.all(), expression, Map.of("vpc_id", VPC_ID, "tags", TAGS)));
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

    private static TableFilters create(ColumnFilter... filters)
    {
        return TableFilters.create(TABLE_NAME, COLUMNS, List.of(filters), DescribeVpcsRequest.builder().sdkFields());
    }

    private static Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(AwsTableHandle table, Map<ColumnHandle, Domain> domains)
    {
        return FILTERS.applyFilter(table, new Constraint(TupleDomain.withColumnDomains(domains)));
    }

    private static Domain values(String... values)
    {
        return Domain.multipleValues(VARCHAR, List.of(values).stream().map(value -> (Object) utf8Slice(value)).toList());
    }
}
