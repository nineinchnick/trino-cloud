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

import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;
import static pl.net.was.cloud.aws.HandleJsonCodecs.roundTrip;

final class TestAwsConnectorSplit
{
    private static final AwsConnectorSplit BUCKET_SPLIT = new AwsConnectorSplit(TupleDomain.withColumnDomains(Map.of(
            new AwsColumnHandle("bucket_name", VARCHAR), Domain.singleValue(VARCHAR, utf8Slice("bucket")))));

    @Test
    void testJsonRoundTrip()
    {
        assertThat(roundTrip(AwsConnectorSplit.class, BUCKET_SPLIT)).isEqualTo(BUCKET_SPLIT);
        assertThat(roundTrip(AwsConnectorSplit.class, AwsConnectorSplit.WHOLE_TABLE)).isEqualTo(AwsConnectorSplit.WHOLE_TABLE);
        AwsConnectorSplit regionAndIdsSplit = new AwsConnectorSplit(TupleDomain.withColumnDomains(Map.of(
                new AwsColumnHandle("region", VARCHAR), Domain.singleValue(VARCHAR, utf8Slice("eu-west-1")),
                new AwsColumnHandle("instance_id", VARCHAR), Domain.multipleValues(VARCHAR, List.of(utf8Slice("i-1"), utf8Slice("i-2"))))));
        assertThat(roundTrip(AwsConnectorSplit.class, regionAndIdsSplit)).isEqualTo(regionAndIdsSplit);
    }

    @Test
    void testRemotelyAccessibleWithoutAddresses()
    {
        assertThat(AwsConnectorSplit.WHOLE_TABLE.isRemotelyAccessible()).isTrue();
        assertThat(AwsConnectorSplit.WHOLE_TABLE.getAddresses()).isEmpty();
    }

    @Test
    void testRetainedSizeIncludesConstraint()
    {
        long unconstrainedSize = AwsConnectorSplit.WHOLE_TABLE.getRetainedSizeInBytes();
        long constrainedSize = BUCKET_SPLIT.getRetainedSizeInBytes();

        assertThat(unconstrainedSize).isPositive();
        assertThat(constrainedSize).isGreaterThan(unconstrainedSize);
    }
}
