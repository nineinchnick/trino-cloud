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

import java.util.List;
import java.util.Map;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static pl.net.was.cloud.aws.HandleJsonCodecs.roundTrip;

final class TestAwsMergeTableHandle
{
    @Test
    void testJsonRoundTrip()
    {
        AwsTableHandle table = new AwsTableHandle(new SchemaTableName("ec2", "instances"))
                .withConstraint(TupleDomain.withColumnDomains(Map.of(
                        new AwsColumnHandle("instance_id", VARCHAR), Domain.singleValue(VARCHAR, utf8Slice("i-123")))));
        AwsMergeTableHandle handle = new AwsMergeTableHandle(table, new AwsInsertTableHandle(table, List.of("instance_id"), List.of(VARCHAR)), Map.of(0, List.of("state", "instance_type")));

        assertThat(roundTrip(AwsMergeTableHandle.class, handle)).isEqualTo(handle);
    }

    @Test
    void testRejectsInsertHandleForAnotherTable()
    {
        AwsTableHandle instances = new AwsTableHandle(new SchemaTableName("ec2", "instances"));
        AwsTableHandle images = new AwsTableHandle(new SchemaTableName("ec2", "images"));

        assertThatThrownBy(() -> new AwsMergeTableHandle(instances, new AwsInsertTableHandle(images, List.of(), List.of()), Map.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("tableHandle and insertHandle refer to different tables");
    }
}
