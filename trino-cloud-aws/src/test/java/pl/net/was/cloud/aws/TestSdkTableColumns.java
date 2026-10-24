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

import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.util.List;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.TypeUtils.readNativeValue;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Locale.ENGLISH;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestSdkTableColumns
{
    @Test
    void testDerivedColumnsArePlacedBeforeOrAfterFieldColumns()
    {
        SdkTableColumns columns = SdkTableColumns.builder(S3Object.builder().sdkFields())
                .prepend(new ColumnMetadata("bucket_name", VARCHAR), SdkRow::context)
                .insertAfter("key", new ColumnMetadata("key_length", BIGINT), row -> (long) ((S3Object) row.object()).key().length())
                .insertAfter("key", new ColumnMetadata("upper_key", VARCHAR), row -> utf8Slice(((S3Object) row.object()).key().toUpperCase(ENGLISH)))
                .build();

        List<String> names = columns.columnMetadata().stream().map(ColumnMetadata::getName).toList();
        assertThat(names).startsWith("bucket_name", "key", "key_length", "upper_key", "last_modified");
        assertThat(columns.columnWriters()).hasSameSizeAs(names);

        SdkRow row = new SdkRow(S3Object.builder().key("a/b").size(3L).build(), utf8Slice("bucket"));
        assertThat(encode(columns, row, names.indexOf("bucket_name"))).isEqualTo(utf8Slice("bucket"));
        assertThat(encode(columns, row, names.indexOf("key"))).isEqualTo(utf8Slice("a/b"));
        assertThat(encode(columns, row, names.indexOf("key_length"))).isEqualTo(3L);
        assertThat(encode(columns, row, names.indexOf("upper_key"))).isEqualTo(utf8Slice("A/B"));
        assertThat(encode(columns, row, names.indexOf("size"))).isEqualTo(3L);
        assertThat(encode(columns, row, names.indexOf("etag"))).isNull();
    }

    @Test
    void testDerivedColumnValueCanBeNull()
    {
        SdkTableColumns columns = SdkTableColumns.builder(S3Object.builder().sdkFields())
                .prepend(new ColumnMetadata("bucket_name", VARCHAR), SdkRow::context)
                .build();

        assertThat(encode(columns, new SdkRow(S3Object.builder().build()), 0)).isNull();
    }

    @Test
    void testInsertAfterUnknownColumn()
    {
        assertThatThrownBy(() -> SdkTableColumns.builder(S3Object.builder().sdkFields())
                .insertAfter("unknown", new ColumnMetadata("derived", VARCHAR), _ -> null)
                .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageStartingWith("Columns [unknown] are not all in [");
    }

    @Test
    void testDuplicateDerivedColumn()
    {
        assertThatThrownBy(() -> SdkTableColumns.builder(S3Object.builder().sdkFields())
                .insertAfter("key", new ColumnMetadata("size", BIGINT), _ -> null)
                .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageStartingWith("More than one field is mapped to size in [");
    }

    private static Object encode(SdkTableColumns columns, SdkRow row, int index)
    {
        Type type = columns.columnMetadata().get(index).getType();
        BlockBuilder output = type.createBlockBuilder(null, 1);
        columns.columnWriters().get(index).write(row, output);
        return readNativeValue(type, output.build(), 0);
    }
}
