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

import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.OptionalLong;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;
import static pl.net.was.cloud.aws.HandleJsonCodecs.roundTrip;

final class TestAwsTableHandle
{
    private static final SchemaTableName TABLE_NAME = new SchemaTableName("ec2", "images");

    @Test
    void testDefaultsToNoFilterAndNoLimit()
    {
        AwsTableHandle table = new AwsTableHandle(TABLE_NAME);

        assertThat(table.constraint().isAll()).isTrue();
        assertThat(table.limit()).isEmpty();
    }

    @Test
    void testJsonRoundTrip()
    {
        AwsTableHandle table = new AwsTableHandle(TABLE_NAME);

        assertThat(roundTrip(AwsTableHandle.class, table)).isEqualTo(table);
    }

    @Test
    void testJsonRoundTripWithConstraintAndLimit()
    {
        AwsTableHandle table = constrainedTable();

        assertThat(roundTrip(AwsTableHandle.class, table)).isEqualTo(table);
    }

    @Test
    void testWithMethodsDoNotModifyOriginal()
    {
        AwsTableHandle table = new AwsTableHandle(TABLE_NAME);

        AwsTableHandle limited = table.withLimit(10);

        assertThat(limited.limit()).isEqualTo(OptionalLong.of(10));
        assertThat(table.limit()).isEmpty();
        assertThat(limited).isNotEqualTo(table);
        assertThat(table.withLimit(10)).isEqualTo(limited);
    }

    @Test
    void testToStringShowsConstraint()
    {
        assertThat(new AwsTableHandle(TABLE_NAME).toString())
                .isEqualTo("AwsTableHandle{table=ec2.images}");
        assertThat(constrainedTable().toString())
                .contains("table=ec2.images")
                .contains("image_id")
                .contains("size")
                .contains("limit=10");
    }

    private static AwsTableHandle constrainedTable()
    {
        return new AwsTableHandle(TABLE_NAME)
                .withConstraint(TupleDomain.withColumnDomains(Map.of(
                        new AwsColumnHandle("image_id", VARCHAR), Domain.singleValue(VARCHAR, utf8Slice("ami-123")),
                        new AwsColumnHandle("size", BIGINT), Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(BIGINT, 5L)), false))))
                .withLimit(10);
    }
}
