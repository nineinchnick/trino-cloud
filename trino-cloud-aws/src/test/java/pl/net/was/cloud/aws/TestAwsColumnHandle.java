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
import org.junit.jupiter.api.Test;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;
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
    void testEqualityIncludesType()
    {
        assertThat(new AwsColumnHandle("size", BIGINT))
                .isEqualTo(new AwsColumnHandle("size", BIGINT))
                .isNotEqualTo(new AwsColumnHandle("size", VARCHAR));
    }
}
