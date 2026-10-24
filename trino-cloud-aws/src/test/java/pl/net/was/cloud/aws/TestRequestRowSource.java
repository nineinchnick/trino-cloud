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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.services.ec2.model.DescribeImagesRequest;
import software.amazon.awssdk.services.ec2.model.Ec2Exception;
import software.amazon.awssdk.services.ec2.model.Filter;
import software.amazon.awssdk.services.ec2.model.Image;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static pl.net.was.cloud.aws.filters.ColumnFilter.ec2Filter;
import static pl.net.was.cloud.aws.filters.ColumnFilter.partition;
import static pl.net.was.cloud.aws.filters.ColumnFilter.requestMember;

final class TestRequestRowSource
{
    private static final String MISSING_IMAGE_ID = "ami-missing";
    private static final String FAILING_IMAGE_ID = "ami-failing";

    private final List<String> clients = new ArrayList<>();
    private final List<DescribeImagesRequest> requests = new ArrayList<>();
    private final AwsTable table = AwsTable.builder(
                    new SchemaTableName("ec2", "images"),
                    SdkTableColumns.builder(Image.builder())
                            .prepend(new ColumnMetadata("region", VARCHAR), row -> row.requestValue("region"))
                            .build())
            .rowSource(RequestRowSource.partitioned(region -> "client-" + region, DescribeImagesRequest::builder, this::describeImages))
            .filters(List.of(
                    partition("region"),
                    requestMember("image_id", "ImageIds").skipMissingValues(Set.of("InvalidAMIID.NotFound")),
                    ec2Filter("name", "name")))
            .build();

    @Test
    void testSendsRequestWithPushedDownValues()
    {
        Iterable<SdkRow> rows = table.rows(handle(Map.of("region", List.of("us-east-1"), "image_id", List.of("ami-1"), "name", List.of("name-*"))));

        assertThat(requests).isEmpty();
        List<SdkRow> readRows = ImmutableList.copyOf(rows);
        assertThat(clients).containsExactly("client-us-east-1");
        assertThat(requests).containsExactly(DescribeImagesRequest.builder()
                .imageIds("ami-1")
                .filters(Filter.builder().name("name").values("name-*").build())
                .build());
        assertThat(readRows).singleElement().satisfies(row -> {
            assertThat(((Image) row.object()).imageId()).isEqualTo("ami-1");
            assertThat(row.requestValue("region")).isEqualTo(utf8Slice("us-east-1"));
        });
    }

    @Test
    void testRequestsEveryValueWhenOneIsMissing()
    {
        List<SdkRow> rows = ImmutableList.copyOf(table.rows(handle(Map.of("region", List.of("us-east-1"), "image_id", List.of("ami-1", MISSING_IMAGE_ID)))));

        assertThat(rows).extracting(row -> ((Image) row.object()).imageId()).containsExactly("ami-1");
        assertThat(requests).extracting(DescribeImagesRequest::imageIds).containsExactly(
                List.of("ami-1", MISSING_IMAGE_ID),
                List.of("ami-1"),
                List.of(MISSING_IMAGE_ID));
    }

    @Test
    void testErrorsNameThePartition()
    {
        assertThatThrownBy(() -> ImmutableList.copyOf(table.rows(handle(Map.of("region", List.of("eu-west-1"), "image_id", List.of(FAILING_IMAGE_ID))))))
                .isInstanceOf(TrinoException.class)
                .hasMessageStartingWith("AWS request in region eu-west-1 failed with InternalError");
    }

    @Test
    void testRequiresPartition()
    {
        assertThatThrownBy(() -> table.rows(new AwsTableHandle(table.name())))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Scan without a single value of the partition column");
    }

    // AWS fails the whole request when any of the image IDs doesn't exist
    private Iterable<Image> describeImages(String client, DescribeImagesRequest request)
    {
        clients.add(client);
        requests.add(request);
        if (request.imageIds().contains(MISSING_IMAGE_ID)) {
            throw ec2Exception("InvalidAMIID.NotFound");
        }
        if (request.imageIds().contains(FAILING_IMAGE_ID)) {
            throw ec2Exception("InternalError");
        }
        return request.imageIds().stream()
                .map(imageId -> Image.builder().imageId(imageId).build())
                .toList();
    }

    private AwsTableHandle handle(Map<String, List<String>> filters)
    {
        ImmutableMap.Builder<ColumnHandle, Domain> domains = ImmutableMap.builder();
        filters.forEach((column, values) -> domains.put(table.columnHandle(column), Domain.multipleValues(VARCHAR, values.stream().map(value -> (Object) utf8Slice(value)).toList())));
        return new AwsTableHandle(table.name()).withConstraint(TupleDomain.withColumnDomains(domains.buildOrThrow()));
    }

    private static Ec2Exception ec2Exception(String errorCode)
    {
        return (Ec2Exception) Ec2Exception.builder()
                .statusCode(400)
                .awsErrorDetails(AwsErrorDetails.builder()
                        .errorCode(errorCode)
                        .errorMessage("failed")
                        .build())
                .build();
    }
}
