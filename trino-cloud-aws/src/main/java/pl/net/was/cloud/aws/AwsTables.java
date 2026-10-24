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
import com.google.common.collect.Streams;
import com.google.inject.Inject;
import io.airlift.slice.Slice;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.SchemaTableName;
import pl.net.was.cloud.aws.AwsTable.ColumnSetter;
import pl.net.was.cloud.aws.AwsTable.Deleter;
import pl.net.was.cloud.aws.AwsTable.Inserter;
import pl.net.was.cloud.aws.AwsTable.Updater;
import pl.net.was.cloud.aws.filters.ColumnFilter;
import software.amazon.awssdk.core.SdkField;
import software.amazon.awssdk.core.SdkPojo;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.AttributeValue;
import software.amazon.awssdk.services.ec2.model.AvailabilityZone;
import software.amazon.awssdk.services.ec2.model.CreateImageRequest;
import software.amazon.awssdk.services.ec2.model.DeregisterImageRequest;
import software.amazon.awssdk.services.ec2.model.DescribeAvailabilityZonesRequest;
import software.amazon.awssdk.services.ec2.model.DescribeImagesRequest;
import software.amazon.awssdk.services.ec2.model.DescribeInstanceTypesRequest;
import software.amazon.awssdk.services.ec2.model.DescribeInstancesRequest;
import software.amazon.awssdk.services.ec2.model.DescribeKeyPairsRequest;
import software.amazon.awssdk.services.ec2.model.DescribeLaunchTemplatesRequest;
import software.amazon.awssdk.services.ec2.model.DescribeNatGatewaysRequest;
import software.amazon.awssdk.services.ec2.model.DescribeNetworkInterfacesRequest;
import software.amazon.awssdk.services.ec2.model.DescribePlacementGroupsRequest;
import software.amazon.awssdk.services.ec2.model.DescribePrefixListsRequest;
import software.amazon.awssdk.services.ec2.model.DescribePublicIpv4PoolsRequest;
import software.amazon.awssdk.services.ec2.model.DescribeRegionsRequest;
import software.amazon.awssdk.services.ec2.model.DescribeRouteTablesRequest;
import software.amazon.awssdk.services.ec2.model.DescribeSecurityGroupsRequest;
import software.amazon.awssdk.services.ec2.model.DescribeSnapshotsRequest;
import software.amazon.awssdk.services.ec2.model.DescribeSubnetsRequest;
import software.amazon.awssdk.services.ec2.model.DescribeTagsRequest;
import software.amazon.awssdk.services.ec2.model.DescribeVolumesRequest;
import software.amazon.awssdk.services.ec2.model.DescribeVpcEndpointsRequest;
import software.amazon.awssdk.services.ec2.model.DescribeVpcPeeringConnectionsRequest;
import software.amazon.awssdk.services.ec2.model.DescribeVpcsRequest;
import software.amazon.awssdk.services.ec2.model.DescribeVpnConnectionsRequest;
import software.amazon.awssdk.services.ec2.model.DescribeVpnGatewaysRequest;
import software.amazon.awssdk.services.ec2.model.Ec2Request;
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
import software.amazon.awssdk.services.s3.model.S3Request;
import software.amazon.awssdk.utils.builder.SdkBuilder;

import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.TABLE_NOT_FOUND;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;
import static pl.net.was.cloud.aws.AwsTable.ROW_ID;
import static pl.net.was.cloud.aws.SdkValueConverter.toSdkValue;
import static pl.net.was.cloud.aws.filters.ColumnFilter.ec2Filter;
import static pl.net.was.cloud.aws.filters.ColumnFilter.partition;
import static pl.net.was.cloud.aws.filters.ColumnFilter.requestMember;

public class AwsTables
{
    private static final Set<String> MISSING_IMAGE_CODES = ImmutableSet.of("InvalidAMIID.NotFound", "InvalidAMIID.Malformed");
    private static final Set<String> MISSING_SNAPSHOT_CODES = ImmutableSet.of("InvalidSnapshot.NotFound", "InvalidSnapshotID.Malformed");
    private static final String SELF_OWNER = "self";
    private static final String REGION_COLUMN = "region";
    private static final String BUCKET_NAME_COLUMN = "bucket_name";
    // the state is a row with a code and a name, and UPDATE changes it by setting just the name
    private static final String STATE_NAME = "state_name";

    private final Ec2Clients ec2Clients;
    private final Map<SchemaTableName, AwsTable> tables;

    @Inject
    public AwsTables(Ec2Clients ec2Clients, S3Client s3)
    {
        this.ec2Clients = requireNonNull(ec2Clients, "ec2Clients is null");
        // resources are created in the default region, but changed and removed in the region of the row
        Ec2Client ec2 = ec2Clients.defaultClient();
        requireNonNull(s3, "s3 is null");
        this.tables = Stream.of(
                        simpleRegionalTable(
                                "availability_zones",
                                AvailabilityZone.builder().sdkFields(),
                                DescribeAvailabilityZonesRequest::builder,
                                (client, request) -> client.describeAvailabilityZones(request).availabilityZones()),
                        regionalTable(
                                "images",
                                SdkTableColumns.builder(Image.builder().sdkFields())
                                        // populate row_id - to support DELETE
                                        .prepend(rowIdColumn(), row -> regionalRowId(row, ((Image) row.object()).imageId()))
                                        // populate instance_id - to support INSERT
                                        .prependInputOnly("instance_id", VARCHAR),
                                DescribeImagesRequest::builder,
                                (client, request) -> client.describeImagesPaginator(request).images(),
                                ImmutableList.of(
                                        // the ImageIds member is much faster than the image-id filter, which makes AWS check all public images without an owner
                                        requestMember("image_id", "ImageIds").skipMissingValues(MISSING_IMAGE_CODES),
                                        // owners can be given as aliases, like self or amazon, but the owner_id column always holds account IDs,
                                        // and without an owner, AWS lists all public images, which is slow and rarely useful, unless the IDs limit the results
                                        requestMember("owner_id", "Owners").unenforced().defaultValues(() -> List.of(SELF_OWNER), "image_id"),
                                        ec2Filter("name", "name")))
                                .primaryKey("image_id")
                                .inserter(regionalInserter(new Inserter(CreateImageRequest::builder, request -> List.of(ec2.createImage((CreateImageRequest) request).imageId()), tagsSetter(ResourceType.IMAGE))))
                                .deleter(regionalDeleter((client, imageId) -> client.deregisterImage(DeregisterImageRequest.builder().imageId(imageId).build())))
                                .build(),
                        simpleRegionalTable(
                                "instance_types",
                                InstanceTypeInfo.builder().sdkFields(),
                                DescribeInstanceTypesRequest::builder,
                                (client, request) -> client.describeInstanceTypesPaginator(request).instanceTypes()),
                        regionalTable(
                                "instances",
                                SdkTableColumns.builder(Instance.builder().sdkFields())
                                        .prepend(rowIdColumn(), row -> regionalRowId(row, ((Instance) row.object()).instanceId()))
                                        .insertAfter("state", new ColumnMetadata(STATE_NAME, VARCHAR), row -> stateName((Instance) row.object())),
                                DescribeInstancesRequest::builder,
                                (client, request) -> Iterables.concat(Iterables.transform(client.describeInstancesPaginator(request).reservations(), Reservation::instances)),
                                // unlike the InstanceIds member, the filter returns no rows for missing IDs, instead of failing the whole request
                                ImmutableList.of(ec2Filter("instance_id", "instance-id")))
                                .primaryKey("instance_id")
                                .inserter(regionalInserter(new Inserter(
                                        () -> RunInstancesRequest.builder().minCount(1).maxCount(1),
                                        request -> ec2.runInstances((RunInstancesRequest) request).instances().stream().map(Instance::instanceId).toList(),
                                        tagsSetter(ResourceType.INSTANCE))))
                                .updater(regionalUpdater(ImmutableSet.of(STATE_NAME, "instance_type"), AwsTables::updateInstance))
                                .deleter(regionalDeleter((client, instanceId) -> client.terminateInstances(TerminateInstancesRequest.builder().instanceIds(instanceId).build())))
                                .build(),
                        simpleRegionalTable(
                                "key_pairs",
                                KeyPairInfo.builder().sdkFields(),
                                DescribeKeyPairsRequest::builder,
                                (client, request) -> client.describeKeyPairs(request).keyPairs()),
                        simpleRegionalTable(
                                "launch_templates",
                                LaunchTemplate.builder().sdkFields(),
                                DescribeLaunchTemplatesRequest::builder,
                                (client, request) -> client.describeLaunchTemplatesPaginator(request).launchTemplates()),
                        simpleRegionalTable(
                                "nat_gateways",
                                NatGateway.builder().sdkFields(),
                                DescribeNatGatewaysRequest::builder,
                                (client, request) -> client.describeNatGatewaysPaginator(request).natGateways()),
                        simpleRegionalTable(
                                "network_interfaces",
                                NetworkInterface.builder().sdkFields(),
                                DescribeNetworkInterfacesRequest::builder,
                                (client, request) -> client.describeNetworkInterfacesPaginator(request).networkInterfaces()),
                        simpleRegionalTable(
                                "placement_groups",
                                PlacementGroup.builder().sdkFields(),
                                DescribePlacementGroupsRequest::builder,
                                (client, request) -> client.describePlacementGroups(request).placementGroups()),
                        simpleRegionalTable(
                                "prefix_lists",
                                PrefixList.builder().sdkFields(),
                                DescribePrefixListsRequest::builder,
                                (client, request) -> client.describePrefixListsPaginator(request).prefixLists()),
                        simpleRegionalTable(
                                "public_ipv4_pools",
                                PublicIpv4Pool.builder().sdkFields(),
                                DescribePublicIpv4PoolsRequest::builder,
                                (client, request) -> client.describePublicIpv4PoolsPaginator(request).publicIpv4Pools()),
                        // every region returns the same regions
                        AwsTable.builder(new SchemaTableName("ec2", "regions"), SdkTableColumns.of(Region.builder().sdkFields()))
                                .rowSource(RequestRowSource.of(ec2, DescribeRegionsRequest::builder, (client, request) -> client.describeRegions(request).regions()))
                                .build(),
                        simpleRegionalTable(
                                "route_tables",
                                RouteTable.builder().sdkFields(),
                                DescribeRouteTablesRequest::builder,
                                (client, request) -> client.describeRouteTablesPaginator(request).routeTables()),
                        regionalTable(
                                "snapshots",
                                SdkTableColumns.builder(Snapshot.builder().sdkFields()),
                                DescribeSnapshotsRequest::builder,
                                (client, request) -> client.describeSnapshotsPaginator(request).snapshots(),
                                ImmutableList.of(
                                        // the SnapshotIds member is much faster than the snapshot-id filter, which makes AWS check all public snapshots without an owner
                                        requestMember("snapshot_id", "SnapshotIds").skipMissingValues(MISSING_SNAPSHOT_CODES),
                                        // like the owners of images
                                        requestMember("owner_id", "OwnerIds").unenforced().defaultValues(() -> List.of(SELF_OWNER), "snapshot_id"),
                                        ec2Filter("volume_id", "volume-id")))
                                .build(),
                        simpleRegionalTable(
                                "security_groups",
                                SecurityGroup.builder().sdkFields(),
                                DescribeSecurityGroupsRequest::builder,
                                (client, request) -> client.describeSecurityGroupsPaginator(request).securityGroups()),
                        simpleRegionalTable(
                                "subnets",
                                Subnet.builder().sdkFields(),
                                DescribeSubnetsRequest::builder,
                                (client, request) -> client.describeSubnetsPaginator(request).subnets()),
                        simpleRegionalTable(
                                "tags",
                                TagDescription.builder().sdkFields(),
                                DescribeTagsRequest::builder,
                                (client, request) -> client.describeTagsPaginator(request).tags()),
                        simpleRegionalTable(
                                "volumes",
                                Volume.builder().sdkFields(),
                                DescribeVolumesRequest::builder,
                                (client, request) -> client.describeVolumesPaginator(request).volumes()),
                        simpleRegionalTable(
                                "vpc_endpoints",
                                VpcEndpoint.builder().sdkFields(),
                                DescribeVpcEndpointsRequest::builder,
                                (client, request) -> client.describeVpcEndpointsPaginator(request).vpcEndpoints()),
                        simpleRegionalTable(
                                "vpc_peering_connections",
                                VpcPeeringConnection.builder().sdkFields(),
                                DescribeVpcPeeringConnectionsRequest::builder,
                                (client, request) -> client.describeVpcPeeringConnectionsPaginator(request).vpcPeeringConnections()),
                        simpleRegionalTable(
                                "vpcs",
                                Vpc.builder().sdkFields(),
                                DescribeVpcsRequest::builder,
                                (client, request) -> client.describeVpcsPaginator(request).vpcs()),
                        simpleRegionalTable(
                                "vpn_connections",
                                VpnConnection.builder().sdkFields(),
                                DescribeVpnConnectionsRequest::builder,
                                (client, request) -> client.describeVpnConnections(request).vpnConnections()),
                        simpleRegionalTable(
                                "vpn_gateways",
                                VpnGateway.builder().sdkFields(),
                                DescribeVpnGatewaysRequest::builder,
                                (client, request) -> client.describeVpnGateways(request).vpnGateways()),
                        AwsTable.builder(new SchemaTableName("s3", "buckets"), SdkTableColumns.of(Bucket.builder().sdkFields()))
                                .rowSource(RequestRowSource.of(s3, ListBucketsRequest::builder, (client, request) -> client.listBucketsPaginator(request).buckets()))
                                .inserter(new Inserter(CreateBucketRequest::builder, request -> createBucket(s3, (CreateBucketRequest) request), bucketSetters()))
                                .build(),
                        bucketTable(s3, "objects", S3Object.builder().sdkFields(), ListObjectsV2Request::builder, (client, request) -> client.listObjectsV2Paginator(request).contents()),
                        bucketTable(s3, "object_versions", ObjectVersion.builder().sdkFields(), ListObjectVersionsRequest::builder, (client, request) -> client.listObjectVersionsPaginator(request).versions()),
                        bucketTable(s3, "deleted_objects", DeleteMarkerEntry.builder().sdkFields(), ListObjectVersionsRequest::builder, (client, request) -> client.listObjectVersionsPaginator(request).deleteMarkers()))
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

    private <R extends Ec2Request> AwsTable simpleRegionalTable(
            String table,
            List<SdkField<?>> sdkFields,
            Supplier<? extends SdkBuilder<?, R>> request,
            BiFunction<Ec2Client, R, Iterable<? extends SdkPojo>> executor)
    {
        return regionalTable(table, SdkTableColumns.builder(sdkFields), request, executor, ImmutableList.of())
                .build();
    }

    /**
     * Every regional table has a region column, which is a partition filter that divides the scans into a split for every region,
     * so every split reads a single region using its client.
     */
    private <R extends Ec2Request> AwsTable.Builder regionalTable(
            String table,
            SdkTableColumns.Builder columns,
            Supplier<? extends SdkBuilder<?, R>> request,
            BiFunction<Ec2Client, R, Iterable<? extends SdkPojo>> executor,
            List<ColumnFilter> filters)
    {
        return AwsTable.builder(
                        new SchemaTableName("ec2", table),
                        columns.prepend(new ColumnMetadata(REGION_COLUMN, VARCHAR), row -> row.requestValue(REGION_COLUMN)).build())
                .rowSource(RequestRowSource.partitioned(ec2Clients::client, request, executor))
                .filters(ImmutableList.<ColumnFilter>builder()
                        // with aws.regions=*, the regions are listed using an AWS request, so only when splits are created
                        .add(partition(REGION_COLUMN).knownValues(ec2Clients::regions))
                        .addAll(filters)
                        .build());
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
        return utf8Slice(new RegionalRowId(((Slice) row.requestValue(REGION_COLUMN)).toStringUtf8(), id).toString());
    }

    /**
     * Every bucket is listed in a separate split, and scans without a filter on the bucket name list all buckets owned by the account.
     * The listed buckets don't restrict the filter, because a filter can choose buckets of other accounts that grant access to them.
     */
    private static <R extends S3Request> AwsTable bucketTable(
            S3Client s3,
            String table,
            List<SdkField<?>> sdkFields,
            Supplier<? extends SdkBuilder<?, R>> request,
            BiFunction<S3Client, R, Iterable<? extends SdkPojo>> executor)
    {
        return AwsTable.builder(
                        new SchemaTableName("s3", table),
                        SdkTableColumns.builder(sdkFields)
                                .prepend(new ColumnMetadata(BUCKET_NAME_COLUMN, VARCHAR), row -> row.requestValue(BUCKET_NAME_COLUMN))
                                .build())
                .rowSource(RequestRowSource.of(s3, request, executor))
                .filters(ImmutableList.of(requestMember(BUCKET_NAME_COLUMN, "Bucket").defaultValues(() -> bucketNames(s3))))
                .build();
    }

    // the split manager lists the buckets on the coordinator, outside of the row sources, which translate the errors of AWS requests
    private static List<String> bucketNames(S3Client s3)
    {
        try {
            return Streams.stream(s3.listBucketsPaginator(ListBucketsRequest.builder().build()).buckets())
                    .map(Bucket::name)
                    .collect(toImmutableList());
        }
        catch (SdkException e) {
            throw AwsErrors.translate(e);
        }
    }

    private static ColumnMetadata rowIdColumn()
    {
        return ColumnMetadata.builder().setName(ROW_ID).setType(VARCHAR).setHidden(true).build();
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
