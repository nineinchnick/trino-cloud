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

import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.MemoryContext;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SourcePage;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.StandardErrorCode.NOT_FOUND;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.TypeUtils.readNativeValue;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static pl.net.was.cloud.aws.AwsPageSource.MAX_ROWS_PER_PAGE;

final class TestAwsPageSource
{
    private static final SchemaTableName TABLE_NAME = new SchemaTableName("s3", "objects");

    @Test
    void testOnlyProjectedColumnsInRequestedOrder()
    {
        AwsTable table = table(List.of(
                S3Object.builder().key("a").size(1L).build(),
                S3Object.builder().key("b").build()));
        AwsPageSource pageSource = pageSource(table, "size", "bucket_name", "key");

        List<Page> pages = readAll(pageSource);

        assertThat(pages).hasSize(1);
        Page page = pages.getFirst();
        assertThat(page.getChannelCount()).isEqualTo(3);
        assertThat(page.getPositionCount()).isEqualTo(2);
        assertThat(readNativeValue(BIGINT, page.getBlock(0), 0)).isEqualTo(1L);
        assertThat(readNativeValue(VARCHAR, page.getBlock(1), 0)).isEqualTo(utf8Slice("bucket"));
        assertThat(readNativeValue(VARCHAR, page.getBlock(2), 0)).isEqualTo(utf8Slice("a"));
        assertThat(readNativeValue(VARCHAR, page.getBlock(2), 1)).isEqualTo(utf8Slice("b"));
    }

    @Test
    void testUnsetFieldsAreNull()
    {
        AwsTable table = table(List.of(S3Object.builder().key("a").build()));

        Page page = readAll(pageSource(table, "size", "etag", "owner", "checksum_algorithm")).getFirst();

        for (int channel = 0; channel < page.getChannelCount(); channel++) {
            assertThat(page.getBlock(channel).isNull(0)).isTrue();
        }
    }

    @Test
    void testRowsSpanningManyPages()
    {
        int rowCount = 2 * MAX_ROWS_PER_PAGE + 10;
        AwsPageSource pageSource = pageSource(table(objects(rowCount, 1)), "key");

        List<Page> pages = readAll(pageSource);

        assertThat(pages).extracting(Page::getPositionCount).containsExactly(MAX_ROWS_PER_PAGE, MAX_ROWS_PER_PAGE, 10);
        assertThat(readNativeValue(VARCHAR, pages.getLast().getBlock(0), 9)).isEqualTo(utf8Slice(key(rowCount - 1, 1)));
        assertThat(pageSource.getCompletedPositions()).hasValue(rowCount);
        assertThat(pageSource.getCompletedBytes()).isEqualTo(pages.stream().mapToLong(Page::getSizeInBytes).sum());
        assertThat(pageSource.isFinished()).isTrue();
    }

    @Test
    void testNoProjectedColumns()
    {
        List<Page> pages = readAll(pageSource(table(objects(MAX_ROWS_PER_PAGE + 1, 1))));

        assertThat(pages).extracting(Page::getChannelCount).containsOnly(0);
        assertThat(pages).extracting(Page::getPositionCount).containsExactly(MAX_ROWS_PER_PAGE, 1);
    }

    @Test
    void testEmptyTable()
    {
        AwsPageSource pageSource = pageSource(table(List.of()), "key");

        assertThat(readAll(pageSource)).isEmpty();
        assertThat(pageSource.getCompletedPositions()).hasValue(0);
        assertThat(pageSource.getCompletedBytes()).isZero();
    }

    @Test
    void testMemoryUsageIsBoundedByPageSize()
    {
        int keyLength = 10_000;
        List<Long> reportedBytes = new ArrayList<>();
        AwsPageSource pageSource = pageSource(table(objects(1000, keyLength)), reportedBytes::add, "key");

        List<Page> pages = readAll(pageSource);

        assertThat(pages).extracting(Page::getPositionCount).doesNotContain(1000);
        assertThat(pages.stream().mapToInt(Page::getPositionCount).sum()).isEqualTo(1000);
        assertThat(reportedBytes).isNotEmpty().allMatch(bytes -> bytes >= 0);
        // the page builder is full at 1 MB, so it never holds all 10 MB of keys
        assertThat(reportedBytes.stream().mapToLong(Long::longValue).max().orElseThrow())
                .isGreaterThan(keyLength)
                .isLessThan(4L * 1024 * 1024);
        assertThat(pageSource.getCompletedBytes()).isGreaterThan(1000L * keyLength);

        pageSource.close();
        assertThat(reportedBytes.getLast()).isZero();
    }

    @Test
    void testRowsAreReadLazily()
    {
        AtomicInteger requests = new AtomicInteger();
        AwsTable table = tableBuilder()
                .rowSource((_, _) -> {
                    requests.incrementAndGet();
                    return Iterables.transform(List.of(S3Object.builder().key("a").build()), object -> new SdkRow(object, utf8Slice("bucket")));
                })
                .build();
        AwsPageSource pageSource = pageSource(table, "key");

        assertThat(requests).hasValue(0);
        assertThat(pageSource.getNextSourcePage().getPositionCount()).isEqualTo(1);
        assertThat(requests).hasValue(1);
        assertThat(pageSource.getReadTimeNanos()).isPositive();
    }

    @Test
    void testErrorsWhileIteratingAreTranslated()
    {
        AwsTable table = tableBuilder()
                .rowSource((_, _) -> () -> Iterators.concat(
                        List.of(new SdkRow(S3Object.builder().key("a").build(), utf8Slice("bucket"))).iterator(),
                        failingIterator("NoSuchBucket")))
                .build();
        AwsPageSource pageSource = pageSource(table, "key");

        assertThatThrownBy(pageSource::getNextSourcePage)
                .isInstanceOfSatisfying(TrinoException.class, e -> assertThat(e.getErrorCode()).isEqualTo(NOT_FOUND.toErrorCode()))
                .hasMessageContaining("NoSuchBucket");
    }

    @Test
    void testClose()
    {
        List<Long> reportedBytes = new ArrayList<>();
        AwsPageSource pageSource = pageSource(table(objects(10, 1)), reportedBytes::add, "key");
        assertThat(reportedBytes).singleElement().satisfies(bytes -> assertThat(bytes).isPositive());
        long initialBytes = reportedBytes.getFirst();

        pageSource.close();

        assertThat(reportedBytes).containsExactly(initialBytes, 0L);
        assertThat(pageSource.isFinished()).isTrue();
        assertThat(pageSource.getNextSourcePage()).isNull();
    }

    private static List<Page> readAll(AwsPageSource pageSource)
    {
        List<Page> pages = new ArrayList<>();
        while (!pageSource.isFinished()) {
            SourcePage page = pageSource.getNextSourcePage();
            if (page != null) {
                pages.add(page.getPage());
            }
        }
        return pages;
    }

    private static AwsPageSource pageSource(AwsTable table, String... columnNames)
    {
        return pageSource(table, MemoryContext.NO_LIMIT, columnNames);
    }

    private static AwsPageSource pageSource(AwsTable table, MemoryContext memoryContext, String... columnNames)
    {
        List<AwsColumnHandle> columns = List.of(columnNames).stream()
                .map(table::columnHandle)
                .toList();
        return new AwsPageSource(table, new AwsTableHandle(TABLE_NAME), columns, memoryContext);
    }

    private static AwsTable table(List<S3Object> objects)
    {
        return tableBuilder()
                .rowSource((_, _) -> Iterables.transform(objects, object -> new SdkRow(object, utf8Slice("bucket"))))
                .build();
    }

    private static AwsTable.Builder tableBuilder()
    {
        return AwsTable.builder(
                TABLE_NAME,
                SdkTableColumns.builder(S3Object.builder().sdkFields())
                        .prepend(new ColumnMetadata("bucket_name", VARCHAR), SdkRow::context)
                        .build());
    }

    private static List<S3Object> objects(int count, int keyLength)
    {
        return IntStream.range(0, count)
                .mapToObj(i -> S3Object.builder().key(key(i, keyLength)).build())
                .toList();
    }

    private static String key(int index, int length)
    {
        String key = String.valueOf(index);
        return key + "x".repeat(Math.max(0, length - key.length()));
    }

    private static Iterator<SdkRow> failingIterator(String errorCode)
    {
        return new Iterator<>()
        {
            @Override
            public boolean hasNext()
            {
                throw S3Exception.builder()
                        .statusCode(400)
                        .awsErrorDetails(AwsErrorDetails.builder()
                                .errorCode(errorCode)
                                .errorMessage("failed")
                                .build())
                        .build();
            }

            @Override
            public SdkRow next()
            {
                throw new UnsupportedOperationException();
            }
        };
    }
}
