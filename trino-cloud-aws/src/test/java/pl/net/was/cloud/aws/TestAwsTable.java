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
import pl.net.was.cloud.aws.filters.FilterApplier;
import pl.net.was.cloud.aws.filters.FilterType;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestAwsTable
{
    private static final SchemaTableName TABLE_NAME = new SchemaTableName("test", "objects");

    @Test
    void testRequiredFilterComments()
    {
        AwsTable table = table(
                SdkTableColumns.builder(S3Object.builder().sdkFields()),
                filterApplier(Map.of("key", FilterType.EQUAL, "last_modified", FilterType.GREATER_THAN_EQUAL, "size", FilterType.EQUAL), Set.of("key", "last_modified")));

        assertThat(table.column("key").getComment()).contains("Required filter: use = or IN");
        assertThat(table.column("last_modified").getComment()).contains("Required filter: use >= or >");
        assertThat(table.column("size").getComment()).isEmpty();
        assertThat(table.columns()).contains(table.column("key"), table.column("last_modified"));
    }

    @Test
    void testRequiredFilterCommentOfInputOnlyColumn()
    {
        AwsTable table = table(
                SdkTableColumns.builder(S3Object.builder().sdkFields()).prependInputOnly("source", VARCHAR),
                filterApplier(Map.of("source", FilterType.EQUAL), Set.of("source")));

        assertThat(table.column("source").getComment()).contains("Only used by INSERT, always NULL when reading. Required filter: use = or IN");
    }

    @Test
    void testRequiredFilterMustBeSupported()
    {
        assertThatThrownBy(() -> table(SdkTableColumns.builder(S3Object.builder().sdkFields()), filterApplier(Map.of(), Set.of("key"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Required filter key is not a supported filter");
    }

    @Test
    void testUnknownColumn()
    {
        AwsTable table = table(SdkTableColumns.builder(S3Object.builder().sdkFields()), filterApplier(Map.of(), Set.of()));

        assertThatThrownBy(() -> table.column("unknown"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Column unknown does not exist in test.objects");
    }

    private static AwsTable table(SdkTableColumns.Builder columns, FilterApplier filterApplier)
    {
        return AwsTable.builder(TABLE_NAME, columns.build())
                .rowSource((_, _) -> List.of())
                .filterApplier(filterApplier)
                .build();
    }

    private static FilterApplier filterApplier(Map<String, FilterType> supportedFilters, Set<String> requiredFilters)
    {
        return new FilterApplier()
        {
            @Override
            public Map<String, FilterType> getSupportedFilters()
            {
                return supportedFilters;
            }

            @Override
            public Set<String> getRequiredFilters()
            {
                return requiredFilters;
            }
        };
    }
}
