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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import io.airlift.slice.Slices;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorRecordSetProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.InMemoryRecordSet;
import io.trino.spi.connector.RecordSet;
import io.trino.spi.type.Type;
import software.amazon.awssdk.core.SdkPojo;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.DescribeImagesRequest;
import software.amazon.awssdk.services.ec2.model.DescribeInstanceTypesRequest;
import software.amazon.awssdk.services.ec2.model.DescribeSnapshotsRequest;

import javax.inject.Inject;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_SECOND;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class AwsRecordSetProvider
        implements ConnectorRecordSetProvider
{
    private final AwsMetadata metadata;
    private final Ec2Client ec2;

    private final Map<String, Supplier<Iterable<List<?>>>> rowGetters;

    @Inject
    public AwsRecordSetProvider(AwsMetadata metadata, AwsConfig config)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        requireNonNull(config, "config is null");
        this.ec2 = Ec2Client.builder()
                .region(Region.of(config.getRegion()))
                .build();
        // must match AwsMetadata.columns
        this.rowGetters = new ImmutableMap.Builder<String, Supplier<Iterable<List<?>>>>()
                .put("ec2.availability_zones", () -> encodeRows(ec2.describeAvailabilityZones().availabilityZones()))
                .put("ec2.images", () -> encodeRows(ec2.describeImages(DescribeImagesRequest.builder().owners("self").build()).images()))
                .put("ec2.instance_types", () -> encodeRows(ec2.describeInstanceTypes(DescribeInstanceTypesRequest.builder().build()).instanceTypes()))
                .put("ec2.instances", this::getInstances)
                .put("ec2.key_pairs", () -> encodeRows(ec2.describeKeyPairs().keyPairs()))
                .put("ec2.launch_templates", () -> encodeRows(ec2.describeLaunchTemplates().launchTemplates()))
                .put("ec2.nat_gateways", () -> encodeRows(ec2.describeNatGateways().natGateways()))
                .put("ec2.network_interfaces", () -> encodeRows(ec2.describeNetworkInterfaces().networkInterfaces()))
                .put("ec2.placement_groups", () -> encodeRows(ec2.describePlacementGroups().placementGroups()))
                .put("ec2.prefix_lists", () -> encodeRows(ec2.describePrefixLists().prefixLists()))
                .put("ec2.public_ipv4_pools", () -> encodeRows(ec2.describePublicIpv4Pools().publicIpv4Pools()))
                .put("ec2.regions", () -> encodeRows(ec2.describeRegions().regions()))
                .put("ec2.route_tables", () -> encodeRows(ec2.describeRouteTables().routeTables()))
                .put("ec2.snapshots", () -> encodeRows(ec2.describeSnapshots(DescribeSnapshotsRequest.builder().maxResults(100).build()).snapshots()))
                .put("ec2.security_groups", () -> encodeRows(ec2.describeSecurityGroups().securityGroups()))
                .put("ec2.subnets", () -> encodeRows(ec2.describeSubnets().subnets()))
                .put("ec2.tags", () -> encodeRows(ec2.describeTags().tags()))
                .put("ec2.volumes", () -> encodeRows(ec2.describeVolumes().volumes()))
                .put("ec2.vpc_endpoints", () -> encodeRows(ec2.describeVpcEndpoints().vpcEndpoints()))
                .put("ec2.vpc_peering_connections", () -> encodeRows(ec2.describeVpcPeeringConnections().vpcPeeringConnections()))
                .put("ec2.vpcs", () -> encodeRows(ec2.describeVpcs().vpcs()))
                .put("ec2.vpn_connections", () -> encodeRows(ec2.describeVpnConnections().vpnConnections()))
                .put("ec2.vpn_gateways", () -> encodeRows(ec2.describeVpnGateways().vpnGateways()))
                .build();
    }

    @Override
    public RecordSet getRecordSet(
            ConnectorTransactionHandle connectorTransactionHandle,
            ConnectorSession connectorSession,
            ConnectorSplit connectorSplit,
            ConnectorTableHandle table,
            List<? extends ColumnHandle> list)
    {
        List<AwsColumnHandle> restColumnHandles = list
                .stream()
                .map(c -> (AwsColumnHandle) c)
                .collect(toList());
        ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(connectorSession, table);

        List<Integer> columnIndexes = restColumnHandles
                .stream()
                .map(column -> {
                    int index = 0;
                    for (ColumnMetadata columnMetadata : tableMetadata.getColumns()) {
                        if (columnMetadata.getName().equalsIgnoreCase(column.getName())) {
                            return index;
                        }
                        index++;
                    }
                    throw new IllegalStateException("Unknown column: " + column.getName());
                })
                .collect(toList());

        List<Type> mappedTypes = restColumnHandles
                .stream()
                .map(AwsColumnHandle::getType)
                .collect(toList());

        //noinspection StaticPseudoFunctionalStyleMethod
        Iterable<List<?>> mappedRows = Iterables.transform(
                getRows((AwsTableHandle) table),
                row -> columnIndexes
                        .stream()
                        .map(row::get)
                        .collect(toList()));

        return new InMemoryRecordSet(mappedTypes, mappedRows);
    }

    private Iterable<List<?>> getRows(AwsTableHandle table)
    {
        return rowGetters.get(table.getSchemaTableName().toString()).get();
    }

    private Iterable<List<?>> getInstances()
    {
        return ec2.describeInstances()
                .reservations()
                .stream()
                .flatMap(r -> r.instances()
                        .stream()
                        .map(AwsRecordSetProvider::encodeRow))
                .collect(toList());
    }

    private static Iterable<List<?>> encodeRows(List<? extends SdkPojo> objects)
    {
        return objects
                .stream()
                .map(AwsRecordSetProvider::encodeRow)
                .collect(toList());
    }

    private static List<?> encodeRow(SdkPojo o)
    {
        return o.sdkFields()
                .stream()
                .map(f -> encodeField(f.marshallingType().getTargetClass(), f.getValueOrDefault(o)))
                .collect(toList());
    }

    // must support all types from AwsMetadata.typeMap
    private static Object encodeField(Class<?> klass, Object o)
    {
        if (klass == String.class) {
            if (o == null) {
                return "";
            }
            return Slices.utf8Slice((String) o);
        }
        if (klass == Instant.class) {
            if (o == null) {
                return 0;
            }
            return ((Instant) o).getEpochSecond() * NANOSECONDS_PER_SECOND;
        }
        if (klass == Integer.class || klass == Long.class || klass == Number.class || klass == Boolean.class) {
            return o;
        }
        if (klass == List.class) {
            List<?> list = (List<?>) o;
            BlockBuilder values = VARCHAR.createBlockBuilder(null, o != null ? list.size() : 0);
            if (list != null) {
                for (Object value : list) {
                    VARCHAR.writeString(values, value.toString());
                }
            }
            return values.build();
        }
        if (o == null) {
            return "";
        }
        return o.toString();
    }
}
