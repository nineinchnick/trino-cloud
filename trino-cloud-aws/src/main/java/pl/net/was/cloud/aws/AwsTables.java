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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import io.airlift.slice.Slices;
import io.trino.spi.TrinoException;
import io.trino.spi.block.SqlMap;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.SchemaTableName;
import pl.net.was.cloud.aws.AwsTable.Inserter;
import pl.net.was.cloud.aws.filters.BucketFilter;
import pl.net.was.cloud.aws.filters.FilterApplier;
import pl.net.was.cloud.aws.filters.ImageFilter;
import pl.net.was.cloud.aws.filters.InstanceFilter;
import pl.net.was.cloud.aws.filters.SnapshotFilter;
import software.amazon.awssdk.core.SdkField;
import software.amazon.awssdk.core.SdkPojo;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.AttributeValue;
import software.amazon.awssdk.services.ec2.model.AvailabilityZone;
import software.amazon.awssdk.services.ec2.model.CreateImageRequest;
import software.amazon.awssdk.services.ec2.model.DeregisterImageRequest;
import software.amazon.awssdk.services.ec2.model.DescribeImagesRequest;
import software.amazon.awssdk.services.ec2.model.DescribeInstanceTypesRequest;
import software.amazon.awssdk.services.ec2.model.DescribeInstancesRequest;
import software.amazon.awssdk.services.ec2.model.DescribePublicIpv4PoolsRequest;
import software.amazon.awssdk.services.ec2.model.DescribeSnapshotsRequest;
import software.amazon.awssdk.services.ec2.model.Filter;
import software.amazon.awssdk.services.ec2.model.Image;
import software.amazon.awssdk.services.ec2.model.Instance;
import software.amazon.awssdk.services.ec2.model.InstanceTypeInfo;
import software.amazon.awssdk.services.ec2.model.KeyPairInfo;
import software.amazon.awssdk.services.ec2.model.LaunchTemplate;
import software.amazon.awssdk.services.ec2.model.ModifyInstanceAttributeRequest;
import software.amazon.awssdk.services.ec2.model.NatGateway;
import software.amazon.awssdk.services.ec2.model.NetworkInterface;
import software.amazon.awssdk.services.ec2.model.PlacementGroup;
import software.amazon.awssdk.services.ec2.model.PrefixList;
import software.amazon.awssdk.services.ec2.model.PublicIpv4Pool;
import software.amazon.awssdk.services.ec2.model.Region;
import software.amazon.awssdk.services.ec2.model.Reservation;
import software.amazon.awssdk.services.ec2.model.RouteTable;
import software.amazon.awssdk.services.ec2.model.RunInstancesRequest;
import software.amazon.awssdk.services.ec2.model.SecurityGroup;
import software.amazon.awssdk.services.ec2.model.Snapshot;
import software.amazon.awssdk.services.ec2.model.StartInstancesRequest;
import software.amazon.awssdk.services.ec2.model.StopInstancesRequest;
import software.amazon.awssdk.services.ec2.model.Subnet;
import software.amazon.awssdk.services.ec2.model.TagDescription;
import software.amazon.awssdk.services.ec2.model.TerminateInstancesRequest;
import software.amazon.awssdk.services.ec2.model.Volume;
import software.amazon.awssdk.services.ec2.model.Vpc;
import software.amazon.awssdk.services.ec2.model.VpcEndpoint;
import software.amazon.awssdk.services.ec2.model.VpcPeeringConnection;
import software.amazon.awssdk.services.ec2.model.VpnConnection;
import software.amazon.awssdk.services.ec2.model.VpnGateway;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.Bucket;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteMarkerEntry;
import software.amazon.awssdk.services.s3.model.ListBucketsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectVersionsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ObjectVersion;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.TABLE_NOT_FOUND;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;
import static pl.net.was.cloud.aws.AwsErrorCode.AWS_MISSING_PREDICATE;
import static pl.net.was.cloud.aws.AwsTable.ROW_ID;
import static pl.net.was.cloud.aws.PageValues.getValue;
import static pl.net.was.cloud.aws.SdkColumns.columns;
import static pl.net.was.cloud.aws.SdkIterables.emptyOnErrorCodes;
import static pl.net.was.cloud.aws.SdkRowEncoder.encodeRow;

public class AwsTables
{
    private static final Set<String> MISSING_IMAGE_CODES = ImmutableSet.of("InvalidAMIID.NotFound", "InvalidAMIID.Malformed");
    private static final Set<String> MISSING_INSTANCE_CODES = ImmutableSet.of("InvalidInstanceID.NotFound", "InvalidInstanceID.Malformed");
    private static final Set<String> MISSING_SNAPSHOT_CODES = ImmutableSet.of("InvalidSnapshot.NotFound", "InvalidSnapshotID.Malformed");
    private static final String SELF_OWNER = "self";

    private final Map<SchemaTableName, AwsTable> tables;

    @Inject
    public AwsTables(Ec2Client ec2, S3Client s3)
    {
        requireNonNull(ec2, "ec2 is null");
        requireNonNull(s3, "s3 is null");
        this.tables = Stream.of(
                        simpleTable("ec2", "availability_zones", AvailabilityZone.builder().sdkFields(), () -> ec2.describeAvailabilityZones().availabilityZones()),
                        AwsTable.builder(
                                        new SchemaTableName("ec2", "images"),
                                        columns(List.of(rowIdColumn(), new ColumnMetadata("instance_id", VARCHAR)), Image.builder().sdkFields()))
                                .rowSource((table, handle) -> images(ec2, table, handle))
                                .filterApplier(new ImageFilter())
                                .inserter(new Inserter(CreateImageRequest::builder, request -> ec2.createImage((CreateImageRequest) request)))
                                .deleter(rowId -> ec2.deregisterImage(DeregisterImageRequest.builder().imageId(rowId).build()))
                                .build(),
                        simpleTable("ec2", "instance_types", InstanceTypeInfo.builder().sdkFields(), () -> ec2.describeInstanceTypesPaginator(DescribeInstanceTypesRequest.builder().build()).instanceTypes()),
                        AwsTable.builder(
                                        new SchemaTableName("ec2", "instances"),
                                        columns(List.of(rowIdColumn()), Instance.builder().sdkFields()))
                                .rowSource((table, handle) -> instances(ec2, table, handle))
                                .filterApplier(new InstanceFilter())
                                .inserter(new Inserter(() -> RunInstancesRequest.builder().minCount(1).maxCount(1), request -> ec2.runInstances((RunInstancesRequest) request)))
                                .updater((rowId, columnValues) -> updateInstance(ec2, rowId, columnValues))
                                .deleter(rowId -> ec2.terminateInstances(TerminateInstancesRequest.builder().instanceIds(rowId).build()))
                                .build(),
                        simpleTable("ec2", "key_pairs", KeyPairInfo.builder().sdkFields(), () -> ec2.describeKeyPairs().keyPairs()),
                        simpleTable("ec2", "launch_templates", LaunchTemplate.builder().sdkFields(), () -> ec2.describeLaunchTemplatesPaginator().launchTemplates()),
                        simpleTable("ec2", "nat_gateways", NatGateway.builder().sdkFields(), () -> ec2.describeNatGatewaysPaginator().natGateways()),
                        simpleTable("ec2", "network_interfaces", NetworkInterface.builder().sdkFields(), () -> ec2.describeNetworkInterfacesPaginator().networkInterfaces()),
                        simpleTable("ec2", "placement_groups", PlacementGroup.builder().sdkFields(), () -> ec2.describePlacementGroups().placementGroups()),
                        simpleTable("ec2", "prefix_lists", PrefixList.builder().sdkFields(), () -> ec2.describePrefixListsPaginator().prefixLists()),
                        simpleTable("ec2", "public_ipv4_pools", PublicIpv4Pool.builder().sdkFields(), () -> ec2.describePublicIpv4PoolsPaginator(DescribePublicIpv4PoolsRequest.builder().build()).publicIpv4Pools()),
                        simpleTable("ec2", "regions", Region.builder().sdkFields(), () -> ec2.describeRegions().regions()),
                        simpleTable("ec2", "route_tables", RouteTable.builder().sdkFields(), () -> ec2.describeRouteTablesPaginator().routeTables()),
                        AwsTable.builder(new SchemaTableName("ec2", "snapshots"), columns(Snapshot.builder().sdkFields()))
                                .rowSource((table, handle) -> snapshots(ec2, table, handle))
                                .filterApplier(new SnapshotFilter())
                                .build(),
                        simpleTable("ec2", "security_groups", SecurityGroup.builder().sdkFields(), () -> ec2.describeSecurityGroupsPaginator().securityGroups()),
                        simpleTable("ec2", "subnets", Subnet.builder().sdkFields(), () -> ec2.describeSubnetsPaginator().subnets()),
                        simpleTable("ec2", "tags", TagDescription.builder().sdkFields(), () -> ec2.describeTagsPaginator().tags()),
                        simpleTable("ec2", "volumes", Volume.builder().sdkFields(), () -> ec2.describeVolumesPaginator().volumes()),
                        simpleTable("ec2", "vpc_endpoints", VpcEndpoint.builder().sdkFields(), () -> ec2.describeVpcEndpointsPaginator().vpcEndpoints()),
                        simpleTable("ec2", "vpc_peering_connections", VpcPeeringConnection.builder().sdkFields(), () -> ec2.describeVpcPeeringConnectionsPaginator().vpcPeeringConnections()),
                        simpleTable("ec2", "vpcs", Vpc.builder().sdkFields(), () -> ec2.describeVpcsPaginator().vpcs()),
                        simpleTable("ec2", "vpn_connections", VpnConnection.builder().sdkFields(), () -> ec2.describeVpnConnections().vpnConnections()),
                        simpleTable("ec2", "vpn_gateways", VpnGateway.builder().sdkFields(), () -> ec2.describeVpnGateways().vpnGateways()),
                        AwsTable.builder(new SchemaTableName("s3", "buckets"), columns(Bucket.builder().sdkFields()))
                                .rowSource((_, _) -> encodeRows(s3.listBucketsPaginator(ListBucketsRequest.builder().build()).buckets(), List.of()))
                                .inserter(new Inserter(CreateBucketRequest::builder, request -> s3.createBucket((CreateBucketRequest) request)))
                                .build(),
                        bucketTable("objects", S3Object.builder().sdkFields(), bucketName -> s3.listObjectsV2Paginator(ListObjectsV2Request.builder().bucket(bucketName).build()).contents()),
                        bucketTable("object_versions", ObjectVersion.builder().sdkFields(), bucketName -> s3.listObjectVersionsPaginator(ListObjectVersionsRequest.builder().bucket(bucketName).build()).versions()),
                        bucketTable("deleted_objects", DeleteMarkerEntry.builder().sdkFields(), bucketName -> s3.listObjectVersionsPaginator(ListObjectVersionsRequest.builder().bucket(bucketName).build()).deleteMarkers()))
                .collect(toImmutableMap(AwsTable::name, identity()));
    }

    public Optional<AwsTable> get(SchemaTableName name)
    {
        return Optional.ofNullable(tables.get(name));
    }

    public AwsTable require(SchemaTableName name)
    {
        AwsTable table = tables.get(name);
        if (table == null) {
            throw new TrinoException(TABLE_NOT_FOUND, "Table not found: " + name);
        }
        return table;
    }

    public Collection<AwsTable> all()
    {
        return tables.values();
    }

    private static AwsTable simpleTable(String schema, String table, List<SdkField<?>> sdkFields, Supplier<Iterable<? extends SdkPojo>> rows)
    {
        return AwsTable.builder(new SchemaTableName(schema, table), columns(sdkFields))
                .rowSource((_, _) -> encodeRows(rows.get(), List.of()))
                .build();
    }

    private static AwsTable bucketTable(String table, List<SdkField<?>> sdkFields, Function<String, Iterable<? extends SdkPojo>> rows)
    {
        return AwsTable.builder(
                        new SchemaTableName("s3", table),
                        columns(List.of(new ColumnMetadata("bucket_name", VARCHAR)), sdkFields))
                .rowSource((awsTable, handle) -> {
                    String bucketName = (String) awsTable.requireFilterApplier().getFilter(awsTable.columnHandle("bucket_name"), handle.constraint());
                    requirePredicate(bucketName, awsTable.name() + ".bucket_name");
                    return encodeRows(rows.apply(bucketName), List.of(_ -> bucketName));
                })
                .filterApplier(new BucketFilter())
                .build();
    }

    private static ColumnMetadata rowIdColumn()
    {
        return ColumnMetadata.builder().setName(ROW_ID).setType(VARCHAR).setHidden(true).build();
    }

    private static Iterable<List<?>> images(Ec2Client ec2, AwsTable table, AwsTableHandle handle)
    {
        FilterApplier filter = table.requireFilterApplier();
        String imageId = (String) filter.getFilter(table.columnHandle("image_id"), handle.constraint());
        String name = (String) filter.getFilter(table.columnHandle("name"), handle.constraint());
        List<String> owners = filter.getFilterValues(table.columnHandle("owner_id"), handle.constraint());
        DescribeImagesRequest.Builder request = DescribeImagesRequest.builder();
        if (imageId != null) {
            request.imageIds(imageId);
        }
        if (name != null) {
            request.filters(Filter.builder().name("name").values(name).build());
        }
        // image IDs alone limit the results, and can refer to public or shared images
        if (imageId == null || !owners.isEmpty()) {
            request.owners(ownersOrSelf(owners));
        }
        return encodeRows(
                emptyOnErrorCodes(ec2.describeImagesPaginator(request.build()).images(), MISSING_IMAGE_CODES),
                List.of(
                        // populate row_id - to support DELETE
                        o -> Slices.utf8Slice(((Image) o).imageId()),
                        // populate instance_id - to support INSERT
                        _ -> null));
    }

    private static Iterable<List<?>> snapshots(Ec2Client ec2, AwsTable table, AwsTableHandle handle)
    {
        FilterApplier filter = table.requireFilterApplier();
        List<String> snapshotIds = filter.getFilterValues(table.columnHandle("snapshot_id"), handle.constraint());
        List<String> volumeIds = filter.getFilterValues(table.columnHandle("volume_id"), handle.constraint());
        List<String> owners = filter.getFilterValues(table.columnHandle("owner_id"), handle.constraint());
        DescribeSnapshotsRequest.Builder request = DescribeSnapshotsRequest.builder();
        if (!volumeIds.isEmpty()) {
            request.filters(Filter.builder().name("volume-id").values(volumeIds).build());
        }
        if (snapshotIds.isEmpty()) {
            return describeSnapshots(ec2, request.ownerIds(ownersOrSelf(owners)).build());
        }
        if (!owners.isEmpty()) {
            request.ownerIds(owners);
        }
        DescribeSnapshotsRequest baseRequest = request.build();
        // AWS fails the whole request when any of the snapshot IDs doesn't exist, so describe each snapshot separately
        return Iterables.concat(Lists.transform(
                snapshotIds,
                snapshotId -> describeSnapshots(ec2, baseRequest.toBuilder().snapshotIds(snapshotId).build())));
    }

    private static Iterable<List<?>> describeSnapshots(Ec2Client ec2, DescribeSnapshotsRequest request)
    {
        return encodeRows(emptyOnErrorCodes(ec2.describeSnapshotsPaginator(request).snapshots(), MISSING_SNAPSHOT_CODES), List.of());
    }

    // without an owner, AWS lists all public images or snapshots, which is slow and rarely useful
    private static List<String> ownersOrSelf(List<String> owners)
    {
        if (owners.isEmpty()) {
            return List.of(SELF_OWNER);
        }
        return owners;
    }

    private static Iterable<List<?>> instances(Ec2Client ec2, AwsTable table, AwsTableHandle handle)
    {
        String instanceId = (String) table.requireFilterApplier().getFilter(table.columnHandle("instance_id"), handle.constraint());
        DescribeInstancesRequest.Builder request = DescribeInstancesRequest.builder();
        if (instanceId != null) {
            request.instanceIds(instanceId);
        }
        Iterable<Reservation> reservations = emptyOnErrorCodes(ec2.describeInstancesPaginator(request.build()).reservations(), MISSING_INSTANCE_CODES);
        return encodeRows(
                Iterables.concat(Iterables.transform(reservations, Reservation::instances)),
                List.of(o -> Slices.utf8Slice(((Instance) o).instanceId())));
    }

    private static void updateInstance(Ec2Client ec2, String rowId, Function<String, Object> columnValues)
    {
        String desiredState = (String) getValue(VARCHAR, 1, ((SqlMap) columnValues.apply("state")).getRawKeyBlock());
        switch (desiredState) {
            case "running" -> ec2.startInstances(StartInstancesRequest.builder().instanceIds(rowId).build());
            case "stopped" -> ec2.stopInstances(StopInstancesRequest.builder().instanceIds(rowId).build());
            default -> throw new TrinoException(NOT_SUPPORTED, "Updates of ec2.instances.state column to %s are not supported".formatted(desiredState));
        }
        String desiredInstanceType = ((String) columnValues.apply("instance_type")).toLowerCase(Locale.US);
        ec2.modifyInstanceAttribute(
                ModifyInstanceAttributeRequest
                        .builder()
                        .instanceType(AttributeValue.builder().value(desiredInstanceType).build())
                        .instanceId(rowId)
                        .build());
    }

    private static <T extends SdkPojo> Iterable<List<?>> encodeRows(Iterable<T> objects, List<Function<SdkPojo, Object>> prependValues)
    {
        return Iterables.transform(objects, object -> encodeRow(object, prependValues));
    }

    private static void requirePredicate(Object value, String name)
    {
        if (value == null) {
            throw new TrinoException(AWS_MISSING_PREDICATE, "Missing required constraint for " + name);
        }
    }
}
