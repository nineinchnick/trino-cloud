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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import io.airlift.slice.Slice;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.SchemaTableName;
import pl.net.was.cloud.aws.AwsTable.ColumnSetter;
import pl.net.was.cloud.aws.AwsTable.Deleter;
import pl.net.was.cloud.aws.AwsTable.Inserter;
import pl.net.was.cloud.aws.AwsTable.Updater;
import pl.net.was.cloud.aws.filters.BucketFilter;
import pl.net.was.cloud.aws.filters.CombinedFilter;
import pl.net.was.cloud.aws.filters.FilterApplier;
import pl.net.was.cloud.aws.filters.ImageFilter;
import pl.net.was.cloud.aws.filters.InstanceFilter;
import pl.net.was.cloud.aws.filters.RegionFilter;
import pl.net.was.cloud.aws.filters.SnapshotFilter;
import software.amazon.awssdk.core.SdkField;
import software.amazon.awssdk.core.SdkPojo;
import software.amazon.awssdk.core.exception.SdkException;
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
import software.amazon.awssdk.services.ec2.model.ResourceType;
import software.amazon.awssdk.services.ec2.model.RouteTable;
import software.amazon.awssdk.services.ec2.model.RunInstancesRequest;
import software.amazon.awssdk.services.ec2.model.SecurityGroup;
import software.amazon.awssdk.services.ec2.model.Snapshot;
import software.amazon.awssdk.services.ec2.model.StartInstancesRequest;
import software.amazon.awssdk.services.ec2.model.StopInstancesRequest;
import software.amazon.awssdk.services.ec2.model.Subnet;
import software.amazon.awssdk.services.ec2.model.TagDescription;
import software.amazon.awssdk.services.ec2.model.TagSpecification;
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
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.TABLE_NOT_FOUND;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;
import static pl.net.was.cloud.aws.AwsTable.ROW_ID;
import static pl.net.was.cloud.aws.SdkIterables.emptyOnErrorCodes;
import static pl.net.was.cloud.aws.SdkIterables.fallbackOnErrorCodes;
import static pl.net.was.cloud.aws.SdkIterables.translateErrors;
import static pl.net.was.cloud.aws.SdkValueConverter.toSdkValue;
import static pl.net.was.cloud.aws.filters.RegionFilter.REGION_COLUMN;

public class AwsTables
{
    private static final Set<String> MISSING_IMAGE_CODES = ImmutableSet.of("InvalidAMIID.NotFound", "InvalidAMIID.Malformed");
    private static final Set<String> MISSING_SNAPSHOT_CODES = ImmutableSet.of("InvalidSnapshot.NotFound", "InvalidSnapshotID.Malformed");
    private static final String SELF_OWNER = "self";
    // the state is a row with a code and a name, and UPDATE changes it by setting just the name
    private static final String STATE_NAME = "state_name";

    private final Ec2Clients ec2Clients;
    private final RegionFilter regionFilter;
    private final Map<SchemaTableName, AwsTable> tables;

    @Inject
    public AwsTables(Ec2Clients ec2Clients, S3Client s3)
    {
        this.ec2Clients = requireNonNull(ec2Clients, "ec2Clients is null");
        this.regionFilter = new RegionFilter(ec2Clients::regions);
        // resources are created in the default region, but changed and removed in the region of the row
        Ec2Client ec2 = ec2Clients.defaultClient();
        requireNonNull(s3, "s3 is null");
        this.tables = Stream.of(
                        simpleRegionalTable("availability_zones", AvailabilityZone.builder().sdkFields(), client -> client.describeAvailabilityZones().availabilityZones()),
                        regionalTable(
                                "images",
                                SdkTableColumns.builder(Image.builder().sdkFields())
                                        // populate row_id - to support DELETE
                                        .prepend(rowIdColumn(), row -> regionalRowId(row, ((Image) row.object()).imageId()))
                                        // populate instance_id - to support INSERT
                                        .prependInputOnly("instance_id", VARCHAR),
                                AwsTables::images,
                                new ImageFilter())
                                .primaryKey("image_id")
                                .inserter(regionalInserter(new Inserter(CreateImageRequest::builder, request -> List.of(ec2.createImage((CreateImageRequest) request).imageId()), tagsSetter(ResourceType.IMAGE))))
                                .deleter(regionalDeleter((client, imageId) -> client.deregisterImage(DeregisterImageRequest.builder().imageId(imageId).build())))
                                .build(),
                        simpleRegionalTable("instance_types", InstanceTypeInfo.builder().sdkFields(), client -> client.describeInstanceTypesPaginator(DescribeInstanceTypesRequest.builder().build()).instanceTypes()),
                        regionalTable(
                                "instances",
                                SdkTableColumns.builder(Instance.builder().sdkFields())
                                        .prepend(rowIdColumn(), row -> regionalRowId(row, ((Instance) row.object()).instanceId()))
                                        .insertAfter("state", new ColumnMetadata(STATE_NAME, VARCHAR), row -> stateName((Instance) row.object())),
                                AwsTables::instances,
                                new InstanceFilter())
                                .primaryKey("instance_id")
                                .inserter(regionalInserter(new Inserter(
                                        () -> RunInstancesRequest.builder().minCount(1).maxCount(1),
                                        request -> ec2.runInstances((RunInstancesRequest) request).instances().stream().map(Instance::instanceId).toList(),
                                        tagsSetter(ResourceType.INSTANCE))))
                                .updater(regionalUpdater(ImmutableSet.of(STATE_NAME, "instance_type"), AwsTables::updateInstance))
                                .deleter(regionalDeleter((client, instanceId) -> client.terminateInstances(TerminateInstancesRequest.builder().instanceIds(instanceId).build())))
                                .build(),
                        simpleRegionalTable("key_pairs", KeyPairInfo.builder().sdkFields(), client -> client.describeKeyPairs().keyPairs()),
                        simpleRegionalTable("launch_templates", LaunchTemplate.builder().sdkFields(), client -> client.describeLaunchTemplatesPaginator().launchTemplates()),
                        simpleRegionalTable("nat_gateways", NatGateway.builder().sdkFields(), client -> client.describeNatGatewaysPaginator().natGateways()),
                        simpleRegionalTable("network_interfaces", NetworkInterface.builder().sdkFields(), client -> client.describeNetworkInterfacesPaginator().networkInterfaces()),
                        simpleRegionalTable("placement_groups", PlacementGroup.builder().sdkFields(), client -> client.describePlacementGroups().placementGroups()),
                        simpleRegionalTable("prefix_lists", PrefixList.builder().sdkFields(), client -> client.describePrefixListsPaginator().prefixLists()),
                        simpleRegionalTable("public_ipv4_pools", PublicIpv4Pool.builder().sdkFields(), client -> client.describePublicIpv4PoolsPaginator(DescribePublicIpv4PoolsRequest.builder().build()).publicIpv4Pools()),
                        // every region returns the same regions
                        simpleTable("ec2", "regions", Region.builder().sdkFields(), () -> ec2.describeRegions().regions()),
                        simpleRegionalTable("route_tables", RouteTable.builder().sdkFields(), client -> client.describeRouteTablesPaginator().routeTables()),
                        regionalTable("snapshots", SdkTableColumns.builder(Snapshot.builder().sdkFields()), AwsTables::snapshots, new SnapshotFilter())
                                .build(),
                        simpleRegionalTable("security_groups", SecurityGroup.builder().sdkFields(), client -> client.describeSecurityGroupsPaginator().securityGroups()),
                        simpleRegionalTable("subnets", Subnet.builder().sdkFields(), client -> client.describeSubnetsPaginator().subnets()),
                        simpleRegionalTable("tags", TagDescription.builder().sdkFields(), client -> client.describeTagsPaginator().tags()),
                        simpleRegionalTable("volumes", Volume.builder().sdkFields(), client -> client.describeVolumesPaginator().volumes()),
                        simpleRegionalTable("vpc_endpoints", VpcEndpoint.builder().sdkFields(), client -> client.describeVpcEndpointsPaginator().vpcEndpoints()),
                        simpleRegionalTable("vpc_peering_connections", VpcPeeringConnection.builder().sdkFields(), client -> client.describeVpcPeeringConnectionsPaginator().vpcPeeringConnections()),
                        simpleRegionalTable("vpcs", Vpc.builder().sdkFields(), client -> client.describeVpcsPaginator().vpcs()),
                        simpleRegionalTable("vpn_connections", VpnConnection.builder().sdkFields(), client -> client.describeVpnConnections().vpnConnections()),
                        simpleRegionalTable("vpn_gateways", VpnGateway.builder().sdkFields(), client -> client.describeVpnGateways().vpnGateways()),
                        AwsTable.builder(new SchemaTableName("s3", "buckets"), SdkTableColumns.of(Bucket.builder().sdkFields()))
                                .rowSource((_, _) -> sdkRows(s3.listBucketsPaginator(ListBucketsRequest.builder().build()).buckets()))
                                .inserter(new Inserter(CreateBucketRequest::builder, request -> createBucket(s3, (CreateBucketRequest) request), bucketSetters()))
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
        return AwsTable.builder(new SchemaTableName(schema, table), SdkTableColumns.of(sdkFields))
                .rowSource((_, _) -> sdkRows(rows.get()))
                .build();
    }

    private AwsTable simpleRegionalTable(String table, List<SdkField<?>> sdkFields, Function<Ec2Client, Iterable<? extends SdkPojo>> rows)
    {
        return regionalTable(table, SdkTableColumns.builder(sdkFields), (client, _, _) -> rows.apply(client))
                .build();
    }

    /**
     * Every regional table has a region column, which is a filter that divides the scans into a split for every region,
     * so the row source reads a single region using its client.
     */
    private AwsTable.Builder regionalTable(String table, SdkTableColumns.Builder columns, RegionalRowSource rowSource, FilterApplier... tableFilters)
    {
        return AwsTable.builder(
                        new SchemaTableName("ec2", table),
                        columns.prepend(new ColumnMetadata(REGION_COLUMN, VARCHAR), SdkRow::context).build())
                .rowSource(regionalRows(rowSource))
                .filterApplier(new CombinedFilter(ImmutableList.<FilterApplier>builder()
                        .add(regionFilter)
                        .add(tableFilters)
                        .build()));
    }

    private AwsTable.RowSource regionalRows(RegionalRowSource rowSource)
    {
        return (table, handle) -> {
            // the split manager divides scans by region, so there's a single region, unless the region filter wasn't used to create the splits
            String region = (String) table.requireFilterApplier().getFilter(table.columnHandle(REGION_COLUMN), handle.constraint(), ec2Clients.defaultRegion());
            Slice context = utf8Slice(region);
            Iterable<? extends SdkPojo> rows;
            try {
                rows = rowSource.rows(ec2Clients.client(region), table, handle);
            }
            catch (SdkException e) {
                throw AwsErrors.translateInRegion(e, region);
            }
            return Iterables.transform(translateErrors(rows, region), object -> new SdkRow(object, context));
        };
    }

    // INSERT has no row to read the region from, so resources are only created in the default region
    private Inserter regionalInserter(Inserter inserter)
    {
        String defaultRegion = ec2Clients.defaultRegion();
        Map<String, ColumnSetter> columnSetters = ImmutableMap.<String, ColumnSetter>builder()
                .putAll(inserter.columnSetters())
                .put(REGION_COLUMN, (_, _, block, position) -> {
                    String region = VARCHAR.getSlice(block, position).toStringUtf8();
                    if (!region.equals(defaultRegion)) {
                        throw new TrinoException(NOT_SUPPORTED, "Resources can only be created in the region set in aws.region, %s, not in %s".formatted(defaultRegion, region));
                    }
                })
                .buildOrThrow();
        return new Inserter(
                inserter.requestBuilder(),
                request -> {
                    try {
                        return inserter.executor().apply(request);
                    }
                    catch (SdkException e) {
                        throw AwsErrors.translateInRegion(e, defaultRegion);
                    }
                },
                columnSetters);
    }

    private Updater regionalUpdater(Set<String> columns, RegionalUpdater updater)
    {
        return new Updater(columns, (rowId, values) -> {
            RegionalRowId id = RegionalRowId.parse(rowId);
            try {
                updater.update(ec2Clients.client(id.region()), id.id(), values);
            }
            catch (SdkException e) {
                throw AwsErrors.translateInRegion(e, id.region());
            }
        });
    }

    private Deleter regionalDeleter(BiConsumer<Ec2Client, String> deleter)
    {
        return rowId -> {
            RegionalRowId id = RegionalRowId.parse(rowId);
            try {
                deleter.accept(ec2Clients.client(id.region()), id.id());
            }
            catch (SdkException e) {
                throw AwsErrors.translateInRegion(e, id.region());
            }
        };
    }

    private static Slice regionalRowId(SdkRow row, String id)
    {
        return utf8Slice(new RegionalRowId(((Slice) row.context()).toStringUtf8(), id).toString());
    }

    private static AwsTable bucketTable(String table, List<SdkField<?>> sdkFields, Function<String, Iterable<? extends SdkPojo>> rows)
    {
        return AwsTable.builder(
                        new SchemaTableName("s3", table),
                        SdkTableColumns.builder(sdkFields)
                                .prepend(new ColumnMetadata("bucket_name", VARCHAR), SdkRow::context)
                                .build())
                .rowSource((awsTable, handle) -> {
                    awsTable.requireFilterApplier().checkRequiredFilters(handle);
                    List<String> bucketNames = awsTable.requireFilterApplier().getFilterValues(awsTable.columnHandle("bucket_name"), handle.constraint());
                    return Iterables.concat(Lists.transform(
                            bucketNames,
                            bucketName -> {
                                Slice context = utf8Slice(bucketName);
                                return Iterables.transform(rows.apply(bucketName), object -> new SdkRow(object, context));
                            }));
                })
                .filterApplier(new BucketFilter())
                .build();
    }

    private static ColumnMetadata rowIdColumn()
    {
        return ColumnMetadata.builder().setName(ROW_ID).setType(VARCHAR).setHidden(true).build();
    }

    private static Iterable<Image> images(Ec2Client ec2, AwsTable table, AwsTableHandle handle)
    {
        FilterApplier filter = table.requireFilterApplier();
        List<String> imageIds = filter.getFilterValues(table.columnHandle("image_id"), handle.constraint());
        List<String> names = filter.getFilterValues(table.columnHandle("name"), handle.constraint());
        List<String> owners = filter.getFilterValues(table.columnHandle("owner_id"), handle.constraint());
        DescribeImagesRequest.Builder request = DescribeImagesRequest.builder();
        if (!names.isEmpty()) {
            request.filters(Filter.builder().name("name").values(names).build());
        }
        // image IDs alone limit the results, and can refer to public or shared images
        if (imageIds.isEmpty() || !owners.isEmpty()) {
            request.owners(ownersOrSelf(owners));
        }
        DescribeImagesRequest baseRequest = request.build();
        Iterable<Image> images = imageIds.isEmpty()
                ? ec2.describeImagesPaginator(baseRequest).images()
                : describeByIds(imageIds, MISSING_IMAGE_CODES, ids -> ec2.describeImagesPaginator(baseRequest.toBuilder().imageIds(ids).build()).images());
        return images;
    }

    private static Iterable<Snapshot> snapshots(Ec2Client ec2, AwsTable table, AwsTableHandle handle)
    {
        FilterApplier filter = table.requireFilterApplier();
        List<String> snapshotIds = filter.getFilterValues(table.columnHandle("snapshot_id"), handle.constraint());
        List<String> volumeIds = filter.getFilterValues(table.columnHandle("volume_id"), handle.constraint());
        List<String> owners = filter.getFilterValues(table.columnHandle("owner_id"), handle.constraint());
        DescribeSnapshotsRequest.Builder request = DescribeSnapshotsRequest.builder();
        if (!volumeIds.isEmpty()) {
            request.filters(Filter.builder().name("volume-id").values(volumeIds).build());
        }
        // snapshot IDs alone limit the results, and can refer to public or shared snapshots
        if (snapshotIds.isEmpty() || !owners.isEmpty()) {
            request.ownerIds(ownersOrSelf(owners));
        }
        DescribeSnapshotsRequest baseRequest = request.build();
        Iterable<Snapshot> snapshots = snapshotIds.isEmpty()
                ? ec2.describeSnapshotsPaginator(baseRequest).snapshots()
                : describeByIds(snapshotIds, MISSING_SNAPSHOT_CODES, ids -> ec2.describeSnapshotsPaginator(baseRequest.toBuilder().snapshotIds(ids).build()).snapshots());
        return snapshots;
    }

    // the ID request parameters are much faster than the ID filters, which make AWS check all public images or snapshots without an owner,
    // but AWS fails the whole request when any of the IDs doesn't exist, so then describe each ID separately, and skip the missing ones
    private static <T> Iterable<T> describeByIds(List<String> ids, Set<String> missingIdCodes, Function<List<String>, Iterable<T>> describe)
    {
        checkArgument(!ids.isEmpty(), "ids is empty");
        if (ids.size() == 1) {
            return emptyOnErrorCodes(describe.apply(ids), missingIdCodes);
        }
        return fallbackOnErrorCodes(
                describe.apply(ids),
                missingIdCodes,
                () -> Iterables.concat(Lists.transform(ids, id -> emptyOnErrorCodes(describe.apply(List.of(id)), missingIdCodes))));
    }

    // without an owner, AWS lists all public images or snapshots, which is slow and rarely useful
    private static List<String> ownersOrSelf(List<String> owners)
    {
        if (owners.isEmpty()) {
            return List.of(SELF_OWNER);
        }
        return owners;
    }

    private static Iterable<Instance> instances(Ec2Client ec2, AwsTable table, AwsTableHandle handle)
    {
        List<String> instanceIds = table.requireFilterApplier().getFilterValues(table.columnHandle("instance_id"), handle.constraint());
        DescribeInstancesRequest.Builder request = DescribeInstancesRequest.builder();
        // unlike the InstanceIds parameter, the filter returns no rows for missing IDs, instead of failing the whole request
        if (!instanceIds.isEmpty()) {
            request.filters(Filter.builder().name("instance-id").values(instanceIds).build());
        }
        Iterable<Reservation> reservations = ec2.describeInstancesPaginator(request.build()).reservations();
        return Iterables.concat(Iterables.transform(reservations, Reservation::instances));
    }

    private static Slice stateName(Instance instance)
    {
        if (instance.state() == null || instance.state().name() == null) {
            return null;
        }
        return utf8Slice(instance.state().nameAsString());
    }

    // tags are set using tag specifications, which also require the type of the created resource
    private static Map<String, ColumnSetter> tagsSetter(ResourceType resourceType)
    {
        return ImmutableMap.of("tags", (request, type, block, position) -> {
            TagSpecification.Builder tagSpecification = TagSpecification.builder().resourceType(resourceType);
            SdkField<?> tagsField = requestField(tagSpecification, "Tags");
            tagsField.set(tagSpecification, toSdkValue(tagsField, type, block, position));
            requestField(request, "TagSpecifications").set(request, List.of(tagSpecification.build()));
        });
    }

    private static SdkField<?> requestField(Object builder, String memberName)
    {
        return ((SdkPojo) builder).sdkFields().stream()
                .filter(field -> field.memberName().equals(memberName))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("%s does not have a %s field".formatted(builder.getClass().getName(), memberName)));
    }

    // the bucket name is the name column, but the bucket field in the request
    private static Map<String, ColumnSetter> bucketSetters()
    {
        return ImmutableMap.of(
                "name", (request, _, block, position) -> ((CreateBucketRequest.Builder) request).bucket(VARCHAR.getSlice(block, position).toStringUtf8()),
                "bucket_region", (request, _, block, position) -> ((CreateBucketRequest.Builder) request)
                        .createBucketConfiguration(configuration -> configuration.locationConstraint(VARCHAR.getSlice(block, position).toStringUtf8())));
    }

    private static List<String> createBucket(S3Client s3, CreateBucketRequest request)
    {
        s3.createBucket(withLocationConstraint(request, s3.serviceClientConfiguration().region().id()));
        return List.of(request.bucket());
    }

    // outside of us-east-1, S3 requires a location constraint matching the region of the endpoint
    @VisibleForTesting
    static CreateBucketRequest withLocationConstraint(CreateBucketRequest request, String region)
    {
        // us-east-1 is the default, and S3 rejects it as a location constraint
        if (request.createBucketConfiguration() != null || region.equals("us-east-1")) {
            return request;
        }
        return request.toBuilder()
                .createBucketConfiguration(configuration -> configuration.locationConstraint(region))
                .build();
    }

    // the instance type can only be changed when the instance is stopped, so it's changed before starting the instance
    private static void updateInstance(Ec2Client ec2, String rowId, Map<String, Object> values)
    {
        if (values.containsKey("instance_type")) {
            String desiredInstanceType = ((String) values.get("instance_type")).toLowerCase(Locale.US);
            ec2.modifyInstanceAttribute(
                    ModifyInstanceAttributeRequest
                            .builder()
                            .instanceType(AttributeValue.builder().value(desiredInstanceType).build())
                            .instanceId(rowId)
                            .build());
        }
        if (!values.containsKey(STATE_NAME)) {
            return;
        }
        String desiredState = (String) values.get(STATE_NAME);
        switch (desiredState) {
            case "running" -> ec2.startInstances(StartInstancesRequest.builder().instanceIds(rowId).build());
            case "stopped" -> ec2.stopInstances(StopInstancesRequest.builder().instanceIds(rowId).build());
            default -> throw new TrinoException(NOT_SUPPORTED, "Updates of ec2.instances.state column to %s are not supported".formatted(desiredState));
        }
    }

    private static Iterable<SdkRow> sdkRows(Iterable<? extends SdkPojo> objects)
    {
        return Iterables.transform(objects, SdkRow::new);
    }

    @FunctionalInterface
    private interface RegionalRowSource
    {
        Iterable<? extends SdkPojo> rows(Ec2Client ec2, AwsTable table, AwsTableHandle handle);
    }

    @FunctionalInterface
    private interface RegionalUpdater
    {
        void update(Ec2Client ec2, String id, Map<String, Object> values);
    }

    /**
     * Identifies a resource in a region, so it's changed or removed in the region it was read from.
     */
    @VisibleForTesting
    record RegionalRowId(String region, String id)
    {
        private static final String SEPARATOR = "/";

        RegionalRowId
        {
            requireNonNull(region, "region is null");
            requireNonNull(id, "id is null");
        }

        static RegionalRowId parse(String rowId)
        {
            int separator = rowId.indexOf(SEPARATOR);
            checkArgument(separator > 0, "Row ID without a region: %s", rowId);
            return new RegionalRowId(rowId.substring(0, separator), rowId.substring(separator + SEPARATOR.length()));
        }

        @Override
        public String toString()
        {
            return region + SEPARATOR + id;
        }
    }
}
