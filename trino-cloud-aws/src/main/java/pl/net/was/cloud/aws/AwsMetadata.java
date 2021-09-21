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
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.statistics.ComputedStatistics;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
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

import javax.inject.Inject;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

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
    public static final String SCHEMA_NAME = "ec2";

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

    public final Map<String, List<ColumnMetadata>> columns;

    @Inject
    public AwsMetadata()
    {
        // must match AwsRecordSetProvider.rowGetters
        columns = new ImmutableMap.Builder<String, List<ColumnMetadata>>()
                .put("availability_zones", fieldsToColumns(AvailabilityZone.builder().sdkFields()))
                .put("images", fieldsToColumns(Image.builder().sdkFields()))
                .put("instance_types", fieldsToColumns(InstanceTypeInfo.builder().sdkFields()))
                .put("instances", fieldsToColumns(Instance.builder().sdkFields()))
                .put("key_pairs", fieldsToColumns(KeyPairInfo.builder().sdkFields()))
                .put("launch_templates", fieldsToColumns(LaunchTemplate.builder().sdkFields()))
                .put("nat_gateways", fieldsToColumns(NatGateway.builder().sdkFields()))
                .put("network_interfaces", fieldsToColumns(NetworkInterface.builder().sdkFields()))
                .put("placement_groups", fieldsToColumns(PlacementGroup.builder().sdkFields()))
                .put("prefix_lists", fieldsToColumns(PrefixList.builder().sdkFields()))
                .put("public_ipv4_pools", fieldsToColumns(PublicIpv4Pool.builder().sdkFields()))
                .put("regions", fieldsToColumns(Region.builder().sdkFields()))
                .put("route_tables", fieldsToColumns(RouteTable.builder().sdkFields()))
                .put("snapshots", fieldsToColumns(Snapshot.builder().sdkFields()))
                .put("security_groups", fieldsToColumns(SecurityGroup.builder().sdkFields()))
                .put("subnets", fieldsToColumns(Subnet.builder().sdkFields()))
                .put("tags", fieldsToColumns(Tag.builder().sdkFields()))
                .put("volumes", fieldsToColumns(Volume.builder().sdkFields()))
                .put("vpc_endpoints", fieldsToColumns(VpcEndpoint.builder().sdkFields()))
                .put("vpc_peering_connections", fieldsToColumns(VpcPeeringConnection.builder().sdkFields()))
                .put("vpcs", fieldsToColumns(Vpc.builder().sdkFields()))
                .put("vpn_connections", fieldsToColumns(VpnConnection.builder().sdkFields()))
                .put("vpn_gateways", fieldsToColumns(VpnGateway.builder().sdkFields()))
                .build();
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
        return List.of(SCHEMA_NAME);
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession connectorSession, SchemaTableName schemaTableName)
    {
        if (!schemaTableName.getSchemaName().equals(SCHEMA_NAME)) {
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
                getColumns(schemaTableName.getTableName()));
    }

    private List<ColumnMetadata> getColumns(String tableName)
    {
        if (!columns.containsKey(tableName)) {
            throw new TrinoException(TABLE_NOT_FOUND, "Invalid table name: " + tableName);
        }
        return columns.get(tableName);
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        return columns
                .keySet()
                .stream()
                .map(table -> new SchemaTableName(SCHEMA_NAME, table))
                .collect(toList());
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
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(
            ConnectorSession connectorSession,
            SchemaTablePrefix schemaTablePrefix)
    {
        return columns.entrySet()
                .stream()
                .collect(Collectors.toMap(
                        e -> new SchemaTableName(schemaTablePrefix.getSchema().orElse(""), e.getKey()),
                        Map.Entry::getValue));
    }

    @Override
    public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle connectorTableHandle)
    {
        AwsTableHandle tableHandle = Types.checkType(connectorTableHandle, AwsTableHandle.class, "tableHandle");
        return new AwsInsertTableHandle(tableHandle);
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
}
