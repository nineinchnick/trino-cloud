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

import io.airlift.slice.Slice;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.SchemaTableName;
import org.junit.jupiter.api.Test;
import pl.net.was.cloud.aws.AwsTable.Inserter;
import software.amazon.awssdk.services.ec2.model.RunInstancesRequest;

import java.util.ArrayList;
import java.util.List;

import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static pl.net.was.cloud.aws.AwsErrorCode.AWS_TOO_MANY_INSERTED_ROWS;

final class TestAwsPageSink
{
    private static final SchemaTableName TABLE_NAME = new SchemaTableName("ec2", "instances");
    private static final AwsInsertTableHandle HANDLE = new AwsInsertTableHandle(new AwsTableHandle(TABLE_NAME), List.of("image_id"), List.of(VARCHAR));

    private final List<RunInstancesRequest> requests = new ArrayList<>();

    @Test
    void testReturnsCreatedResourceIds()
    {
        AwsPageSink sink = new AwsPageSink(HANDLE, table(), 10);

        sink.appendPage(imageIdsPage("ami-1", "ami-2")).join();
        sink.appendPage(imageIdsPage("ami-3")).join();

        assertThat(requests)
                .extracting(RunInstancesRequest::imageId)
                .containsExactly("ami-1", "ami-2", "ami-3");
        assertThat(sink.finish().join())
                .extracting(Slice::toStringUtf8)
                .containsExactly("i-ami-1", "i-ami-2", "i-ami-3");
    }

    @Test
    void testMaxInsertedRows()
    {
        AwsPageSink sink = new AwsPageSink(HANDLE, table(), 2);

        sink.appendPage(imageIdsPage("ami-1")).join();
        assertThatThrownBy(() -> sink.appendPage(imageIdsPage("ami-2", "ami-3")))
                .isInstanceOfSatisfying(TrinoException.class, e -> assertThat(e.getErrorCode()).isEqualTo(AWS_TOO_MANY_INSERTED_ROWS.toErrorCode()))
                .hasMessage("Inserting more than 2 rows into ec2.instances is not allowed, increase aws.max-inserted-rows to insert more");

        assertThat(requests)
                .extracting(RunInstancesRequest::imageId)
                .containsExactly("ami-1");
    }

    private AwsTable table()
    {
        return AwsTable.builder(TABLE_NAME, List.of(new ColumnMetadata("image_id", VARCHAR)))
                .rowSource((_, _) -> List.of())
                .inserter(new Inserter(RunInstancesRequest::builder, request -> {
                    RunInstancesRequest runInstances = (RunInstancesRequest) request;
                    requests.add(runInstances);
                    return List.of("i-" + runInstances.imageId());
                }))
                .build();
    }

    private static Page imageIdsPage(String... imageIds)
    {
        BlockBuilder block = VARCHAR.createBlockBuilder(null, imageIds.length);
        for (String imageId : imageIds) {
            VARCHAR.writeString(block, imageId);
        }
        return new Page(block.build());
    }
}
