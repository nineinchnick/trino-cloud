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
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import org.junit.jupiter.api.Test;
import pl.net.was.cloud.aws.AwsColumnHandle;
import pl.net.was.cloud.aws.AwsTableHandle;
import pl.net.was.cloud.aws.filters.TableFilters.MissingValues;
import pl.net.was.cloud.aws.filters.TableFilters.Partition;
import software.amazon.awssdk.services.ec2.model.DescribeVpcsRequest;
import software.amazon.awssdk.services.ec2.model.Filter;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static pl.net.was.cloud.aws.filters.ColumnFilter.ec2Filter;
import static pl.net.was.cloud.aws.filters.ColumnFilter.partition;
import static pl.net.was.cloud.aws.filters.ColumnFilter.requestMember;

final class TestTableFilters
{
    private static final SchemaTableName TABLE_NAME = new SchemaTableName("ec2", "vpcs");
    private static final List<ColumnMetadata> COLUMNS = List.of(
            new ColumnMetadata("region", VARCHAR),
            new ColumnMetadata("vpc_id", VARCHAR),
            new ColumnMetadata("state", VARCHAR),
            new ColumnMetadata("is_default", BOOLEAN),
            new ColumnMetadata("max_results", INTEGER),
            new ColumnMetadata("created", TIMESTAMP_TZ_MILLIS));
    private static final AwsColumnHandle REGION = new AwsColumnHandle("region", VARCHAR);
    private static final AwsColumnHandle VPC_ID = new AwsColumnHandle("vpc_id", VARCHAR);
    private static final AwsColumnHandle STATE = new AwsColumnHandle("state", VARCHAR);
    private static final AwsColumnHandle IS_DEFAULT = new AwsColumnHandle("is_default", BOOLEAN);
    private static final AwsColumnHandle MAX_RESULTS = new AwsColumnHandle("max_results", INTEGER);
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
        assertThatThrownBy(() -> TableFilters.create(TABLE_NAME, COLUMNS, List.of(ec2Filter("state", "state")), ListObjectsV2Request.builder().sdkFields()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Request of ec2.vpcs does not have EC2 filters for the filter on state");
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
