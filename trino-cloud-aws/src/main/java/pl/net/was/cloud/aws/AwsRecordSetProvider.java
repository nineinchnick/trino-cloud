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
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
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
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.connector.TableColumnsMetadata;
import io.trino.spi.type.MapType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import jakarta.inject.Inject;
import pl.net.was.cloud.aws.filters.FilterApplier;
import software.amazon.awssdk.core.SdkField;
import software.amazon.awssdk.core.SdkPojo;
import software.amazon.awssdk.core.protocol.MarshallingType;
import software.amazon.awssdk.core.traits.ListTrait;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.DescribeImagesRequest;
import software.amazon.awssdk.services.ec2.model.DescribeInstanceTypesRequest;
import software.amazon.awssdk.services.ec2.model.DescribeInstancesRequest;
import software.amazon.awssdk.services.ec2.model.DescribeSnapshotsRequest;
import software.amazon.awssdk.services.ec2.model.Filter;
import software.amazon.awssdk.services.ec2.model.Image;
import software.amazon.awssdk.services.ec2.model.Instance;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectVersionsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static io.trino.spi.StandardErrorCode.INVALID_ROW_FILTER;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_SECOND;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class AwsRecordSetProvider
        implements ConnectorRecordSetProvider
{
    private final AwsMetadata metadata;
    private final Ec2Client ec2;
    private final S3Client s3;

    private static final MapType mapType = new MapType(VARCHAR, VARCHAR, new TypeOperators());

    private final Map<String, Function<AwsTableHandle, Iterable<List<?>>>> rowGetters;

    @Inject
    public AwsRecordSetProvider(AwsMetadata metadata, Ec2Client ec2, S3Client s3)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.ec2 = requireNonNull(ec2, "ec2 is null");
        this.s3 = requireNonNull(s3, "s3 is null");
        Stream<TableColumnsMetadata> columnsStream = StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(metadata
                        .streamTableColumns(null, new SchemaTablePrefix()), Spliterator.ORDERED),
                false);
        Map<String, Map<String, AwsColumnHandle>> columns = columnsStream
                .map(t -> Map.entry(
                        t.getTable().toString(),
                        t.getColumns()
                                .orElse(List.of())
                                .stream()
                                .collect(Collectors.toMap(
                                        ColumnMetadata::getName,
                                        c -> new AwsColumnHandle(c.getName(), c.getType())))))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        // must match AwsMetadata.columns
        this.rowGetters = new ImmutableMap.Builder<String, Function<AwsTableHandle, Iterable<List<?>>>>()
                .put("ec2.availability_zones", t -> encodeRows(ec2.describeAvailabilityZones().availabilityZones()))
                .put("ec2.images", this::getImages)
                .put("ec2.instance_types", t -> encodeRows(ec2.describeInstanceTypes(DescribeInstanceTypesRequest.builder().build()).instanceTypes()))
                .put("ec2.instances", this::getInstances)
                .put("ec2.key_pairs", t -> encodeRows(ec2.describeKeyPairs().keyPairs()))
                .put("ec2.launch_templates", t -> encodeRows(ec2.describeLaunchTemplates().launchTemplates()))
                .put("ec2.nat_gateways", t -> encodeRows(ec2.describeNatGateways().natGateways()))
                .put("ec2.network_interfaces", t -> encodeRows(ec2.describeNetworkInterfaces().networkInterfaces()))
                .put("ec2.placement_groups", t -> encodeRows(ec2.describePlacementGroups().placementGroups()))
                .put("ec2.prefix_lists", t -> encodeRows(ec2.describePrefixLists().prefixLists()))
                .put("ec2.public_ipv4_pools", t -> encodeRows(ec2.describePublicIpv4Pools().publicIpv4Pools()))
                .put("ec2.regions", t -> encodeRows(ec2.describeRegions().regions()))
                .put("ec2.route_tables", t -> encodeRows(ec2.describeRouteTables().routeTables()))
                .put("ec2.snapshots", t -> encodeRows(ec2.describeSnapshots(DescribeSnapshotsRequest.builder().maxResults(100).build()).snapshots()))
                .put("ec2.security_groups", t -> encodeRows(ec2.describeSecurityGroups().securityGroups()))
                .put("ec2.subnets", t -> encodeRows(ec2.describeSubnets().subnets()))
                .put("ec2.tags", t -> encodeRows(ec2.describeTags().tags()))
                .put("ec2.volumes", t -> encodeRows(ec2.describeVolumes().volumes()))
                .put("ec2.vpc_endpoints", t -> encodeRows(ec2.describeVpcEndpoints().vpcEndpoints()))
                .put("ec2.vpc_peering_connections", t -> encodeRows(ec2.describeVpcPeeringConnections().vpcPeeringConnections()))
                .put("ec2.vpcs", t -> encodeRows(ec2.describeVpcs().vpcs()))
                .put("ec2.vpn_connections", t -> encodeRows(ec2.describeVpnConnections().vpnConnections()))
                .put("ec2.vpn_gateways", t -> encodeRows(ec2.describeVpnGateways().vpnGateways()))
                .put("s3.buckets", t -> encodeRows(s3.listBuckets().buckets()))
                .put("s3.objects", t -> {
                    FilterApplier filter = metadata.filterAppliers.get("s3.objects");
                    String bucketName = (String) filter.getFilter((AwsColumnHandle) metadata.columnHandles.get("s3.objects").get("bucket_name"), t.getConstraint());
                    requirePredicate(bucketName, "s3.objects.bucket_name");
                    return encodeRows(
                            s3.listObjectsV2(ListObjectsV2Request.builder().bucket(bucketName).build()).contents(),
                            List.of(o -> bucketName));
                })
                .put("s3.object_versions", t -> {
                    FilterApplier filter = metadata.filterAppliers.get("s3.object_versions");
                    String bucketName = (String) filter.getFilter((AwsColumnHandle) metadata.columnHandles.get("s3.object_versions").get("bucket_name"), t.getConstraint());
                    requirePredicate(bucketName, "s3.object_versions.bucket_name");
                    return encodeRows(
                            s3.listObjectVersions(ListObjectVersionsRequest.builder().bucket(bucketName).build()).versions(),
                            List.of(o -> bucketName));
                })
                .put("s3.deleted_objects", t -> {
                    FilterApplier filter = metadata.filterAppliers.get("s3.deleted_objects");
                    String bucketName = (String) filter.getFilter((AwsColumnHandle) metadata.columnHandles.get("s3.deleted_objects").get("bucket_name"), t.getConstraint());
                    requirePredicate(bucketName, "s3.deleted_objects.bucket_name");
                    return encodeRows(
                            s3.listObjectVersions(ListObjectVersionsRequest.builder().bucket(bucketName).build()).deleteMarkers(),
                            List.of(o -> bucketName));
                })
                .build();
    }

    private void requirePredicate(Object value, String name)
    {
        if (value == null) {
            throw new TrinoException(INVALID_ROW_FILTER, "Missing required constraint for " + name);
        }
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
        return rowGetters.get(table.getSchemaTableName().toString()).apply(table);
    }

    private Iterable<List<?>> getImages(AwsTableHandle table)
    {
        FilterApplier filter = metadata.filterAppliers.get("ec2.images");
        String imageId = (String) filter.getFilter((AwsColumnHandle) metadata.columnHandles.get("ec2.images").get("image_id"), table.getConstraint());
        String name = (String) filter.getFilter((AwsColumnHandle) metadata.columnHandles.get("ec2.images").get("name"), table.getConstraint());
        String owner = (String) filter.getFilter((AwsColumnHandle) metadata.columnHandles.get("ec2.images").get("owner_id"), table.getConstraint());
        DescribeImagesRequest.Builder request = DescribeImagesRequest.builder();
        if (imageId != null) {
            request.imageIds(imageId);
        }
        if (name != null) {
            request.filters(Filter.builder().name("name").values(name).build());
        }
        if (owner != null) {
            request.owners(owner);
        }
        if (imageId == null && name == null && owner == null && table.getLimit() == Integer.MAX_VALUE) {
            throw new TrinoException(INVALID_ROW_FILTER, "Missing a limit or constraint for image_id, name, or owner_id");
        }
        return encodeRows(
                ec2.describeImages(request.build()).images(),
                List.of(
                        // populate row_id - to support DELETE
                        o -> Slices.utf8Slice(((Image) o).imageId()),
                        // populate instance_id - to support INSERT
                        o -> null));
    }

    private Iterable<List<?>> getInstances(AwsTableHandle table)
    {
        FilterApplier filter = metadata.filterAppliers.get("ec2.instances");
        String instanceId = (String) filter.getFilter((AwsColumnHandle) metadata.columnHandles.get("ec2.instances").get("instance_id"), table.getConstraint());
        DescribeInstancesRequest.Builder request = DescribeInstancesRequest.builder();
        if (instanceId != null) {
            request.instanceIds(instanceId);
        }
        return ec2.describeInstances(request.build())
                .reservations()
                .stream()
                .flatMap(r -> r.instances()
                        .stream()
                        .map(i -> encodeRow(i, List.of(o -> Slices.utf8Slice(((Instance) o).instanceId())))))
                .collect(toList());
    }

    private static Iterable<List<?>> encodeRows(List<? extends SdkPojo> objects)
    {
        return objects
                .stream()
                .map(r -> encodeRow(r, List.of()))
                .collect(toList());
    }

    private static Iterable<List<?>> encodeRows(List<? extends SdkPojo> objects, List<Function<SdkPojo, Object>> prependValues)
    {
        return objects
                .stream()
                .map(r -> encodeRow(r, prependValues))
                .collect(toList());
    }

    private static List<?> encodeRow(SdkPojo o)
    {
        return o.sdkFields()
                .stream()
                .map(f -> encodeField(f, f.getValueOrDefault(o)))
                .collect(toList());
    }

    private static List<?> encodeRow(SdkPojo o, List<Function<SdkPojo, Object>> prependValues)
    {
        return Stream.concat(
                        prependValues
                                .stream()
                                .map(f -> f.apply(o)),
                        o.sdkFields()
                                .stream()
                                .map(f -> encodeField(f, f.getValueOrDefault(o))))
                .collect(toList());
    }

    // must support all types from AwsMetadata.typeMap
    private static Object encodeField(SdkField<?> f, Object o)
    {
        MarshallingType<?> sdkType = f.marshallingType();
        if (sdkType == MarshallingType.STRING) {
            if (o == null) {
                return "";
            }
            return Slices.utf8Slice((String) o);
        }
        if (sdkType == MarshallingType.INSTANT) {
            if (o == null) {
                return 0;
            }
            return ((Instant) o).getEpochSecond() * MICROSECONDS_PER_SECOND;
        }
        if (sdkType == MarshallingType.SHORT || sdkType == MarshallingType.INTEGER || sdkType == MarshallingType.LONG
                || sdkType == MarshallingType.FLOAT || sdkType == MarshallingType.DOUBLE
                || sdkType == MarshallingType.BOOLEAN) {
            return o;
        }
        if (sdkType == MarshallingType.SDK_POJO) {
            return encodeSdkPojo((SdkPojo) o);
        }
        if (sdkType == MarshallingType.MAP) {
            return encodeMap((Map<String, ?>) o);
        }
        if (sdkType == MarshallingType.LIST) {
            List<?> list = (List<?>) o;
            BlockBuilder values;
            if (f.containsTrait(ListTrait.class) &&
                    f.getTrait(ListTrait.class).memberFieldInfo().marshallingType() == MarshallingType.SDK_POJO) {
                values = mapType.createBlockBuilder(null, o != null ? list.size() : 0);
                if (list != null) {
                    for (Object value : list) {
                        mapType.writeObject(values, encodeSdkPojo((SdkPojo) value));
                    }
                }
            }
            else {
                values = VARCHAR.createBlockBuilder(null, o != null ? list.size() : 0);
                if (list != null) {
                    for (Object value : list) {
                        VARCHAR.writeString(values, value.toString());
                    }
                }
            }
            return values.build();
        }
        if (o == null) {
            return "";
        }
        return o.toString();
    }

    private static Block encodeSdkPojo(SdkPojo sdkPojo)
    {
        BlockBuilder values = mapType.createBlockBuilder(null, sdkPojo != null ? sdkPojo.sdkFields().size() : 0);
        if (sdkPojo == null) {
            values.appendNull();
            return values.build().getObject(0, Block.class);
        }
        BlockBuilder builder = values.beginBlockEntry();
        for (SdkField<?> field : sdkPojo.sdkFields()) {
            VARCHAR.writeString(builder, field.memberName());
            Object value = field.getValueOrDefault(sdkPojo);
            VARCHAR.writeString(builder, value != null ? value.toString() : "");
        }
        values.closeEntry();
        return values.build().getObject(0, Block.class);
    }

    private static Block encodeMap(Map<String, ?> map)
    {
        BlockBuilder values = mapType.createBlockBuilder(null, map != null ? map.size() : 0);
        if (map == null) {
            values.appendNull();
            return values.build().getObject(0, Block.class);
        }
        BlockBuilder builder = values.beginBlockEntry();
        for (Map.Entry<String, ?> entry : map.entrySet()) {
            VARCHAR.writeString(builder, entry.getKey());
            Object value = entry.getValue();
            VARCHAR.writeString(builder, value != null ? value.toString() : "");
        }
        values.closeEntry();
        return values.build().getObject(0, Block.class);
    }
}
