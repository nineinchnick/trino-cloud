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
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import org.junit.jupiter.api.Test;
import pl.net.was.cloud.aws.AwsColumnHandle;
import software.amazon.awssdk.services.ec2.model.Filter;

import java.util.List;
import java.util.Map;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static pl.net.was.cloud.aws.filters.Ec2FilterNames.ec2Filters;
import static pl.net.was.cloud.aws.filters.FilterApplier.ID_BATCH_SIZE;
import static pl.net.was.cloud.aws.filters.FilterType.EQUAL;

final class TestEc2Filter
{
    private static final List<ColumnMetadata> COLUMNS = List.of(
            new ColumnMetadata("region", VARCHAR),
            new ColumnMetadata("vpc_id", VARCHAR),
            new ColumnMetadata("is_default", BOOLEAN),
            new ColumnMetadata("enable_lni_at_device_index", INTEGER),
            new ColumnMetadata("state_name", VARCHAR),
            new ColumnMetadata("launch_time", TIMESTAMP_TZ_MILLIS));
    private static final Ec2Filter FILTER = new Ec2Filter(
            COLUMNS,
            ec2Filters("vpc-id", "is-default", "enable-lni-at-device-index")
                    .withColumnFilter("state_name", "instance-state-name"));

    @Test
    void testFilterNames()
    {
        assertThat(Ec2Filter.filterName("vpc_id")).isEqualTo("vpc-id");
        assertThat(Ec2Filter.filterName("availability_zone")).isEqualTo("availability-zone");
        assertThat(Ec2Filter.filterName("state")).isEqualTo("state");
    }

    @Test
    void testSupportedFilters()
    {
        assertThat(FILTER.getSupportedFilters()).isEqualTo(Map.of(
                "vpc_id", EQUAL,
                "is_default", EQUAL,
                "enable_lni_at_device_index", EQUAL,
                "state_name", EQUAL));
        // EC2 filter values treat * and ? as wildcards
        assertThat(FILTER.getUnenforcedFilters()).isEqualTo(FILTER.getSupportedFilters().keySet());
        assertThat(FILTER.getSplitBatchSizes()).containsOnlyKeys(FILTER.getSupportedFilters().keySet())
                .allSatisfy((_, batchSize) -> assertThat(batchSize).isEqualTo(ID_BATCH_SIZE));
        assertThat(FILTER.getRequiredFilters()).isEmpty();
    }

    @Test
    void testNoFilters()
    {
        Ec2Filter filter = new Ec2Filter(COLUMNS, ec2Filters());

        assertThat(filter.getSupportedFilters()).isEmpty();
        assertThat(filter.filters(TupleDomain.all())).isEmpty();
    }

    @Test
    void testFilters()
    {
        TupleDomain<ColumnHandle> constraint = TupleDomain.withColumnDomains(Map.of(
                new AwsColumnHandle("region", VARCHAR), Domain.singleValue(VARCHAR, utf8Slice("us-east-1")),
                new AwsColumnHandle("vpc_id", VARCHAR), Domain.multipleValues(VARCHAR, List.of(utf8Slice("vpc-1"), utf8Slice("vpc-2"))),
                new AwsColumnHandle("is_default", BOOLEAN), Domain.singleValue(BOOLEAN, true),
                new AwsColumnHandle("enable_lni_at_device_index", INTEGER), Domain.singleValue(INTEGER, 1L),
                new AwsColumnHandle("state_name", VARCHAR), Domain.singleValue(VARCHAR, utf8Slice("running"))));

        assertThat(FILTER.filters(constraint)).containsExactlyInAnyOrder(
                Filter.builder().name("vpc-id").values("vpc-1", "vpc-2").build(),
                Filter.builder().name("is-default").values("true").build(),
                Filter.builder().name("enable-lni-at-device-index").values("1").build(),
                Filter.builder().name("instance-state-name").values("running").build());
        assertThat(FILTER.filters(TupleDomain.all())).isEmpty();
    }

    @Test
    void testRejectsFilterWithoutColumn()
    {
        assertThatThrownBy(() -> new Ec2Filter(COLUMNS, ec2Filters("vpc-id", "subnet-id")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Filters that don't match any column: [subnet-id]");
        assertThatThrownBy(() -> new Ec2Filter(COLUMNS, ec2Filters().withColumnFilter("state", "instance-state-name")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Filters declared for columns that don't exist: [state]");
    }

    @Test
    void testRejectsColumnWithUnsupportedType()
    {
        assertThatThrownBy(() -> new Ec2Filter(COLUMNS, ec2Filters("launch-time")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Column launch_time for filter launch-time has an unsupported type: timestamp(3) with time zone");
    }
}
