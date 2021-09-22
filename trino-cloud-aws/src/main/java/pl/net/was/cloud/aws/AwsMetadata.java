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

import com.google.common.base.CaseFormat;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorOutputMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableProperties;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.connector.TableColumnsMetadata;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.statistics.ComputedStatistics;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import pl.net.was.cloud.aws.filters.BucketFilter;
import pl.net.was.cloud.aws.filters.FilterApplier;
import software.amazon.awssdk.core.SdkField;
import software.amazon.awssdk.core.protocol.MarshallingType;
import software.amazon.awssdk.core.traits.ListTrait;
import software.amazon.awssdk.services.ec2.model.AvailabilityZone;
import software.amazon.awssdk.services.ec2.model.Image;
import software.amazon.awssdk.services.ec2.model.Instance;
import software.amazon.awssdk.services.ec2.model.InstanceTypeInfo;
import software.amazon.awssdk.services.ec2.model.KeyPairInfo;
import software.amazon.awssdk.services.ec2.model.LaunchTemplate;
import software.amazon.awssdk.services.ec2.model.NatGateway;
import software.amazon.awssdk.services.ec2.model.NetworkInterface;
import software.amazon.awssdk.services.ec2.model.PlacementGroup;
import software.amazon.awssdk.services.ec2.model.PrefixList;
import software.amazon.awssdk.services.ec2.model.PublicIpv4Pool;
import software.amazon.awssdk.services.ec2.model.Region;
import software.amazon.awssdk.services.ec2.model.RouteTable;
import software.amazon.awssdk.services.ec2.model.SecurityGroup;
import software.amazon.awssdk.services.ec2.model.Snapshot;
import software.amazon.awssdk.services.ec2.model.Subnet;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.ec2.model.Volume;
import software.amazon.awssdk.services.ec2.model.Vpc;
import software.amazon.awssdk.services.ec2.model.VpcEndpoint;
import software.amazon.awssdk.services.ec2.model.VpcPeeringConnection;
import software.amazon.awssdk.services.ec2.model.VpnConnection;
import software.amazon.awssdk.services.ec2.model.VpnGateway;
import software.amazon.awssdk.services.s3.model.Bucket;
import software.amazon.awssdk.services.s3.model.DeleteMarkerEntry;
import software.amazon.awssdk.services.s3.model.ObjectVersion;
import software.amazon.awssdk.services.s3.model.S3Object;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.StandardErrorCode.TABLE_NOT_FOUND;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimestampType.TIMESTAMP_SECONDS;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

public class AwsMetadata
        implements ConnectorMetadata
{
    // all types must be handled in AwsRecordSetProvider.encode()
    // missing: Void, BigDecimal, SdkBytes, Document
    // SdkPojo, List<?> and Map<String, ?> handled separately
    private static final Map<MarshallingType<?>, Type> typeMap = new ImmutableMap.Builder<MarshallingType<?>, Type>()
            .put(MarshallingType.SHORT, SMALLINT)
            .put(MarshallingType.INTEGER, INTEGER)
            .put(MarshallingType.LONG, BIGINT)
            .put(MarshallingType.FLOAT, REAL)
            .put(MarshallingType.DOUBLE, DOUBLE)
            .put(MarshallingType.BOOLEAN, BOOLEAN)
            .put(MarshallingType.STRING, VARCHAR)
            .put(MarshallingType.INSTANT, TIMESTAMP_SECONDS)
            .put(MarshallingType.LIST, new ArrayType(VARCHAR))
            .build();

    public final Map<SchemaTableName, List<ColumnMetadata>> columns;
    public final Map<String, Map<String, ColumnHandle>> columnHandles;

    public final Map<String, ? extends FilterApplier> filterAppliers = new ImmutableMap.Builder<String, FilterApplier>()
            .put("s3.objects", new BucketFilter())
            .put("s3.object_versions", new BucketFilter())
            .put("s3.deleted_objects", new BucketFilter())
            .build();

    @Inject
    public AwsMetadata()
    {
        // must match AwsRecordSetProvider.rowGetters
        columns = new ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>>()
                .put(new SchemaTableName("ec2", "availability_zones"), fieldsToColumns(AvailabilityZone.builder().sdkFields()))
                .put(new SchemaTableName("ec2", "images"), fieldsToColumns(Image.builder().sdkFields()))
                .put(new SchemaTableName("ec2", "instance_types"), fieldsToColumns(InstanceTypeInfo.builder().sdkFields()))
                .put(new SchemaTableName("ec2", "instances"), fieldsToColumns(Instance.builder().sdkFields()))
                .put(new SchemaTableName("ec2", "key_pairs"), fieldsToColumns(KeyPairInfo.builder().sdkFields()))
                .put(new SchemaTableName("ec2", "launch_templates"), fieldsToColumns(LaunchTemplate.builder().sdkFields()))
                .put(new SchemaTableName("ec2", "nat_gateways"), fieldsToColumns(NatGateway.builder().sdkFields()))
                .put(new SchemaTableName("ec2", "network_interfaces"), fieldsToColumns(NetworkInterface.builder().sdkFields()))
                .put(new SchemaTableName("ec2", "placement_groups"), fieldsToColumns(PlacementGroup.builder().sdkFields()))
                .put(new SchemaTableName("ec2", "prefix_lists"), fieldsToColumns(PrefixList.builder().sdkFields()))
                .put(new SchemaTableName("ec2", "public_ipv4_pools"), fieldsToColumns(PublicIpv4Pool.builder().sdkFields()))
                .put(new SchemaTableName("ec2", "regions"), fieldsToColumns(Region.builder().sdkFields()))
                .put(new SchemaTableName("ec2", "route_tables"), fieldsToColumns(RouteTable.builder().sdkFields()))
                .put(new SchemaTableName("ec2", "snapshots"), fieldsToColumns(Snapshot.builder().sdkFields()))
                .put(new SchemaTableName("ec2", "security_groups"), fieldsToColumns(SecurityGroup.builder().sdkFields()))
                .put(new SchemaTableName("ec2", "subnets"), fieldsToColumns(Subnet.builder().sdkFields()))
                .put(new SchemaTableName("ec2", "tags"), fieldsToColumns(Tag.builder().sdkFields()))
                .put(new SchemaTableName("ec2", "volumes"), fieldsToColumns(Volume.builder().sdkFields()))
                .put(new SchemaTableName("ec2", "vpc_endpoints"), fieldsToColumns(VpcEndpoint.builder().sdkFields()))
                .put(new SchemaTableName("ec2", "vpc_peering_connections"), fieldsToColumns(VpcPeeringConnection.builder().sdkFields()))
                .put(new SchemaTableName("ec2", "vpcs"), fieldsToColumns(Vpc.builder().sdkFields()))
                .put(new SchemaTableName("ec2", "vpn_connections"), fieldsToColumns(VpnConnection.builder().sdkFields()))
                .put(new SchemaTableName("ec2", "vpn_gateways"), fieldsToColumns(VpnGateway.builder().sdkFields()))
                .put(new SchemaTableName("s3", "buckets"), fieldsToColumns(Bucket.builder().sdkFields()))
                .put(new SchemaTableName("s3", "objects"), fieldsToColumns(
                        List.of(
                                new ColumnMetadata("bucket_name", VARCHAR)),
                        S3Object.builder().sdkFields()))
                .put(new SchemaTableName("s3", "object_versions"), fieldsToColumns(
                        List.of(
                                new ColumnMetadata("bucket_name", VARCHAR)),
                        ObjectVersion.builder().sdkFields()))
                .put(new SchemaTableName("s3", "deleted_objects"), fieldsToColumns(
                        List.of(
                                new ColumnMetadata("bucket_name", VARCHAR)),
                        DeleteMarkerEntry.builder().sdkFields()))
                .build();

        columnHandles = columns
                .entrySet()
                .stream()
                .map(e -> Map.entry(
                        e.getKey().toString(),
                        e.getValue()
                                .stream()
                                .collect(Collectors.toMap(
                                        ColumnMetadata::getName,
                                        c -> (ColumnHandle) new AwsColumnHandle(c.getName(), c.getType())))))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private List<ColumnMetadata> fieldsToColumns(List<ColumnMetadata> extraColumns, List<SdkField<?>> sdkFields)
    {
        return Stream.of(extraColumns, fieldsToColumns(sdkFields))
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
    }

    private List<ColumnMetadata> fieldsToColumns(List<SdkField<?>> sdkFields)
    {
        return sdkFields
                .stream()
                .map(f -> {
                    MarshallingType<?> sdkType = f.marshallingType();
                    Type trinoType = typeMap.getOrDefault(sdkType, VARCHAR);
                    if (sdkType == MarshallingType.SDK_POJO || sdkType == MarshallingType.MAP) {
                        trinoType = new MapType(VARCHAR, VARCHAR, new TypeOperators());
                    }

                    if (sdkType == MarshallingType.LIST &&
                            f.containsTrait(ListTrait.class) &&
                            f.getTrait(ListTrait.class).memberFieldInfo().marshallingType() == MarshallingType.SDK_POJO) {
                        trinoType = new ArrayType(new MapType(VARCHAR, VARCHAR, new TypeOperators()));
                    }

                    String name = CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, f.memberName());
                    return new ColumnMetadata(name, trinoType);
                })
                .collect(toList());
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession connectorSession)
    {
        return List.of("ec2", "s3");
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession connectorSession, SchemaTableName schemaTableName)
    {
        if (!listSchemaNames(connectorSession).contains(schemaTableName.getSchemaName())) {
            return null;
        }
        return new AwsTableHandle(
                schemaTableName,
                TupleDomain.none(),
                0,
                Integer.MAX_VALUE,
                1,
                null);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(
            ConnectorSession connectorSession,
            ConnectorTableHandle connectorTableHandle)
    {
        AwsTableHandle tableHandle = Types.checkType(connectorTableHandle, AwsTableHandle.class, "tableHandle");
        SchemaTableName schemaTableName = tableHandle.getSchemaTableName();
        return new ConnectorTableMetadata(
                schemaTableName,
                getColumns(schemaTableName));
    }

    private List<ColumnMetadata> getColumns(SchemaTableName tableName)
    {
        if (!columns.containsKey(tableName)) {
            throw new TrinoException(TABLE_NOT_FOUND, "Invalid table name: " + tableName);
        }
        return columns.get(tableName);
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        return new ArrayList<>(columns.keySet());
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(
            ConnectorSession connectorSession,
            ConnectorTableHandle connectorTableHandle)
    {
        return getTableMetadata(connectorSession, connectorTableHandle).getColumns().stream()
                .collect(toMap(ColumnMetadata::getName, column -> new AwsColumnHandle(column.getName(), column.getType())));
    }

    @Override
    public ColumnMetadata getColumnMetadata(
            ConnectorSession connectorSession,
            ConnectorTableHandle connectorTableHandle,
            ColumnHandle columnHandle)
    {
        AwsColumnHandle restColumnHandle = Types.checkType(columnHandle, AwsColumnHandle.class, "columnHandle");
        return new ColumnMetadata(restColumnHandle.getName(), restColumnHandle.getType());
    }

    @Override
    public boolean usesLegacyTableLayouts()
    {
        return false;
    }

    @Override
    public ConnectorTableProperties getTableProperties(ConnectorSession session, ConnectorTableHandle table)
    {
        return new ConnectorTableProperties();
    }

    @Override
    public Stream<TableColumnsMetadata> streamTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        return columns.entrySet()
                .stream()
                .filter(e -> e.getKey().getSchemaName().startsWith(prefix.getSchema().orElse(""))
                        && e.getKey().getTableName().startsWith(prefix.getTable().orElse("")))
                .map(e -> TableColumnsMetadata.forTable(e.getKey(), e.getValue()));
    }

    @Override
    public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle, List<ColumnHandle> columns)
    {
        AwsTableHandle awsTableHandle = Types.checkType(tableHandle, AwsTableHandle.class, "tableHandle");
        List<AwsColumnHandle> columnHandles = columns.stream()
                .map(AwsColumnHandle.class::cast)
                .collect(toImmutableList());
        ImmutableList.Builder<String> columnNames = ImmutableList.builder();
        ImmutableList.Builder<Type> columnTypes = ImmutableList.builder();
        for (AwsColumnHandle column : columnHandles) {
            columnNames.add(column.getName());
            columnTypes.add(column.getType());
        }
        return new AwsOutputTableHandle(awsTableHandle, columnNames.build(), columnTypes.build());
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(
            ConnectorSession session,
            ConnectorInsertTableHandle insertHandle,
            Collection<Slice> fragments,
            Collection<ComputedStatistics> computedStatistics)
    {
        return Optional.empty();
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(
            ConnectorSession session,
            ConnectorTableHandle table,
            Constraint constraint)
    {
        AwsTableHandle awsTable = (AwsTableHandle) table;
        String tableName = awsTable.getSchemaTableName().toString();

        FilterApplier filterApplier = filterAppliers.get(tableName);
        if (filterApplier == null) {
            return Optional.empty();
        }
        return filterApplier.applyFilter(
                awsTable,
                columnHandles.get(tableName),
                filterApplier.getSupportedFilters(),
                constraint.getSummary());
    }
}
