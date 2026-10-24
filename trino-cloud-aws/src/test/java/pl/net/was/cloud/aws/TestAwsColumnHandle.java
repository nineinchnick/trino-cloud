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

import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.TypeOperators;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static pl.net.was.cloud.aws.HandleJsonCodecs.roundTrip;

final class TestAwsColumnHandle
{
    @Test
    void testJsonRoundTrip()
    {
        AwsColumnHandle column = new AwsColumnHandle("image_id", VARCHAR);

        assertThat(roundTrip(AwsColumnHandle.class, column)).isEqualTo(column);
    }

    @Test
    void testJsonRoundTripWithParametricType()
    {
        AwsColumnHandle column = new AwsColumnHandle("tags", new ArrayType(VARCHAR));

        assertThat(roundTrip(AwsColumnHandle.class, column)).isEqualTo(column);
    }

    @Test
    void testMapEntry()
    {
        AwsColumnHandle tags = new AwsColumnHandle("tags", new MapType(VARCHAR, BIGINT, new TypeOperators()));
        AwsColumnHandle entry = AwsColumnHandle.mapEntry(tags, "Name");

        assertThat(entry).isEqualTo(new AwsColumnHandle("tags", BIGINT, Optional.of("Name")));
        assertThat(entry).isNotEqualTo(new AwsColumnHandle("tags", BIGINT));
        assertThat(roundTrip(AwsColumnHandle.class, entry)).isEqualTo(entry);
        assertThat(entry.getRetainedSizeInBytes()).isGreaterThan(tags.getRetainedSizeInBytes());
        assertThatThrownBy(() -> AwsColumnHandle.mapEntry(new AwsColumnHandle("name", VARCHAR), "Name"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Column name is not a map: varchar");
    }

    @Test
    void testEqualityIncludesType()
    {
        assertThat(new AwsColumnHandle("size", BIGINT))
                .isEqualTo(new AwsColumnHandle("size", BIGINT))
                .isNotEqualTo(new AwsColumnHandle("size", VARCHAR));
    }
}
