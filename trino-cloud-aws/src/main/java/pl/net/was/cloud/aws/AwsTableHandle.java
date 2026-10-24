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

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;

import java.util.OptionalLong;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.util.Objects.requireNonNull;

public record AwsTableHandle(SchemaTableName schemaTableName, TupleDomain<ColumnHandle> constraint, OptionalLong limit)
        implements ConnectorTableHandle
{
    private static final int INSTANCE_SIZE = instanceSize(AwsTableHandle.class);

    public AwsTableHandle
    {
        requireNonNull(schemaTableName, "schemaTableName is null");
        requireNonNull(constraint, "constraint is null");
        requireNonNull(limit, "limit is null");
    }

    public AwsTableHandle(SchemaTableName schemaTableName)
    {
        this(schemaTableName, TupleDomain.all(), OptionalLong.empty());
    }

    public AwsTableHandle withConstraint(TupleDomain<ColumnHandle> constraint)
    {
        return new AwsTableHandle(schemaTableName, constraint, limit);
    }

    public AwsTableHandle withLimit(long limit)
    {
        return new AwsTableHandle(schemaTableName, constraint, OptionalLong.of(limit));
    }

    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE
                + schemaTableName.getRetainedSizeInBytes()
                + constraint.getRetainedSizeInBytes(column -> ((AwsColumnHandle) column).getRetainedSizeInBytes())
                + sizeOf(limit);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .omitNullValues()
                .add("table", schemaTableName)
                .add("constraint", constraint.isAll() ? null : constraint)
                .add("limit", limit.isPresent() ? limit.orElseThrow() : null)
                .toString();
    }
}
