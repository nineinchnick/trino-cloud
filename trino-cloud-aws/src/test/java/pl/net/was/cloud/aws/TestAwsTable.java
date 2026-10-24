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
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestAwsTable
{
    private static final SchemaTableName TABLE_NAME = new SchemaTableName("test", "objects");

    @Test
    void testUnknownColumn()
    {
        AwsTable table = AwsTable.builder(TABLE_NAME, SdkTableColumns.of(S3Object.builder().sdkFields()))
                .rowSource((_, _) -> List.of())
                .build();

        assertThatThrownBy(() -> table.column("unknown"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Column unknown does not exist in test.objects");
    }
}
