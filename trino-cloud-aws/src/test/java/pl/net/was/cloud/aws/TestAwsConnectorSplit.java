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
import io.trino.spi.predicate.TupleDomain;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;
import static pl.net.was.cloud.aws.HandleJsonCodecs.roundTrip;

final class TestAwsConnectorSplit
{
    private static final AwsTableHandle TABLE = new AwsTableHandle(new SchemaTableName("s3", "objects"));
    private static final AwsTableHandle CONSTRAINED_TABLE = TABLE.withConstraint(TupleDomain.withColumnDomains(Map.of(
            new AwsColumnHandle("bucket_name", VARCHAR), Domain.singleValue(VARCHAR, utf8Slice("bucket")))));

    @Test
    void testJsonRoundTrip()
    {
        AwsConnectorSplit split = new AwsConnectorSplit(CONSTRAINED_TABLE);

        assertThat(roundTrip(AwsConnectorSplit.class, split)).isEqualTo(split);
    }

    @Test
    void testRemotelyAccessibleWithoutAddresses()
    {
        AwsConnectorSplit split = new AwsConnectorSplit(TABLE);

        assertThat(split.isRemotelyAccessible()).isTrue();
        assertThat(split.getAddresses()).isEmpty();
    }

    @Test
    void testRetainedSizeIncludesConstraint()
    {
        long unconstrainedSize = new AwsConnectorSplit(TABLE).getRetainedSizeInBytes();
        long constrainedSize = new AwsConnectorSplit(CONSTRAINED_TABLE).getRetainedSizeInBytes();

        assertThat(unconstrainedSize).isPositive();
        assertThat(constrainedSize).isGreaterThan(unconstrainedSize);
    }
}
