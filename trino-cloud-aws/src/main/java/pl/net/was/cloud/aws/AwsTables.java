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
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;
import pl.net.was.cloud.aws.AwsTable.ColumnSetter;
import pl.net.was.cloud.aws.AwsTable.Deleter;
import pl.net.was.cloud.aws.AwsTable.Inserter;
import pl.net.was.cloud.aws.AwsTable.Updater;
import pl.net.was.cloud.aws.filters.BucketFilter;
import pl.net.was.cloud.aws.filters.CombinedFilter;
import pl.net.was.cloud.aws.filters.Ec2Filter;
import pl.net.was.cloud.aws.filters.Ec2FilterNames;
import pl.net.was.cloud.aws.filters.FilterApplier;
import pl.net.was.cloud.aws.filters.ImageFilter;
import pl.net.was.cloud.aws.filters.RegionFilter;
import pl.net.was.cloud.aws.filters.SnapshotFilter;
import software.amazon.awssdk.core.SdkField;
import software.amazon.awssdk.core.SdkPojo;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.core.traits.ListTrait;
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
import software.amazon.awssdk.utils.builder.SdkBuilder;

import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
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
import static pl.net.was.cloud.aws.filters.Ec2FilterNames.TAG_FILTER;
import static pl.net.was.cloud.aws.filters.Ec2FilterNames.ec2Filters;
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
                        simpleRegionalTable(
                                "availability_zones",
                                AvailabilityZone.builder().sdkFields(),
                                DescribeAvailabilityZonesRequest::builder,
                                (client, request) -> client.describeAvailabilityZones(request).availabilityZones(),
                                ec2Filters("group-long-name", "group-name", "opt-in-status", "parent-zone-id", "parent-zone-name", "region-name", "state", "zone-id", "zone-name", "zone-type")),
                        regionalTable(
                                "images",
                                SdkTableColumns.builder(Image.builder().sdkFields())
                                        // populate row_id - to support DELETE
                                        .prepend(rowIdColumn(), row -> regionalRowId(row, ((Image) row.object()).imageId()))
                                        // populate instance_id - to support INSERT
                                        .prepend(new ColumnMetadata("instance_id", VARCHAR), _ -> null),
                                ec2Filters(
                                        TAG_FILTER,
                                        "architecture",
                                        "boot-mode",
                                        "creation-date",
                                        "description",
                                        "ena-support",
                                        "free-tier-eligible",
                                        "hypervisor",
                                        "image-allowed",
                                        "image-type",
                                        "kernel-id",
                                        "public-ssm-parameter-name",
                                        "ramdisk-id",
                                        "root-device-name",
                                        "root-device-type",
                                        "source-image-id",
                                        "source-image-region",
                                        "source-instance-id",
                                        "sriov-net-support",
                                        "state",
                                        "virtualization-type"),
                                AwsTables::images,
                                new ImageFilter())
                                .primaryKey("image_id")
                                .inserter(regionalInserter(new Inserter(CreateImageRequest::builder, request -> List.of(ec2.createImage((CreateImageRequest) request).imageId()), tagsSetter(ResourceType.IMAGE))))
                                .deleter(regionalDeleter((client, imageId) -> client.deregisterImage(DeregisterImageRequest.builder().imageId(imageId).build())))
                                .build(),
                        simpleRegionalTable(
                                "instance_types",
                                InstanceTypeInfo.builder().sdkFields(),
                                DescribeInstanceTypesRequest::builder,
                                (client, request) -> client.describeInstanceTypesPaginator(request).instanceTypes(),
                                ec2Filters("auto-recovery-supported", "bare-metal", "burstable-performance-supported", "current-generation", "dedicated-hosts-supported", "free-tier-eligible", "hibernation-supported", "hypervisor", "instance-storage-supported", "instance-type", "nitro-enclaves-support", "nitro-tpm-support", "reboot-migration-support")),
                        regionalTable(
                                "instances",
                                SdkTableColumns.builder(Instance.builder().sdkFields())
                                        .prepend(rowIdColumn(), row -> regionalRowId(row, ((Instance) row.object()).instanceId()))
                                        .insertAfter("state", new ColumnMetadata(STATE_NAME, VARCHAR), row -> stateName((Instance) row.object())),
                                ec2Filters(
                                        TAG_FILTER,
                                        "architecture",
                                        "boot-mode",
                                        "capacity-reservation-id",
                                        "client-token",
                                        "current-instance-boot-mode",
                                        "ebs-optimized",
                                        "ena-support",
                                        "hypervisor",
                                        "image-id",
                                        "instance-id",
                                        "instance-lifecycle",
                                        "instance-type",
                                        "ipv6-address",
                                        "kernel-id",
                                        "key-name",
                                        "outpost-arn",
                                        "platform-details",
                                        "private-dns-name",
                                        "private-ip-address",
                                        "ramdisk-id",
                                        "root-device-name",
                                        "root-device-type",
                                        "source-dest-check",
                                        "spot-instance-request-id",
                                        "subnet-id",
                                        "tpm-support",
                                        "usage-operation",
                                        "virtualization-type",
                                        "vpc-id")
                                        .withColumnFilter(STATE_NAME, "instance-state-name"),
                                (client, filters, _, _) -> instances(client, filters))
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
                                (client, request) -> client.describeKeyPairs(request).keyPairs(),
                                ec2Filters(TAG_FILTER, "key-name", "key-pair-id")),
                        simpleRegionalTable(
                                "launch_templates",
                                LaunchTemplate.builder().sdkFields(),
                                DescribeLaunchTemplatesRequest::builder,
                                (client, request) -> client.describeLaunchTemplatesPaginator(request).launchTemplates(),
                                ec2Filters(TAG_FILTER, "launch-template-name")),
                        simpleRegionalTable(
                                "nat_gateways",
                                NatGateway.builder().sdkFields(),
                                DescribeNatGatewaysRequest::builder,
                                (client, request) -> client.describeNatGatewaysPaginator(request).natGateways(),
                                ec2Filters(TAG_FILTER, "nat-gateway-id", "state", "subnet-id", "vpc-id")),
                        simpleRegionalTable(
                                "network_interfaces",
                                NetworkInterface.builder().sdkFields(),
                                DescribeNetworkInterfacesRequest::builder,
                                (client, request) -> client.describeNetworkInterfacesPaginator(request).networkInterfaces(),
                                ec2Filters("availability-zone", "availability-zone-id", "description", "interface-type", "mac-address", "network-interface-id", "owner-id", "private-dns-name", "private-ip-address", "requester-id", "requester-managed", "source-dest-check", "status", "subnet-id", "vpc-id")
                                        .withColumnFilter("tag_set", TAG_FILTER)),
                        simpleRegionalTable(
                                "placement_groups",
                                PlacementGroup.builder().sdkFields(),
                                DescribePlacementGroupsRequest::builder,
                                (client, request) -> client.describePlacementGroups(request).placementGroups(),
                                ec2Filters(TAG_FILTER, "group-arn", "group-name", "spread-level", "state", "strategy")),
                        simpleRegionalTable(
                                "prefix_lists",
                                PrefixList.builder().sdkFields(),
                                DescribePrefixListsRequest::builder,
                                (client, request) -> client.describePrefixListsPaginator(request).prefixLists(),
                                ec2Filters("prefix-list-id", "prefix-list-name")),
                        simpleRegionalTable(
                                "public_ipv4_pools",
                                PublicIpv4Pool.builder().sdkFields(),
                                DescribePublicIpv4PoolsRequest::builder,
                                (client, request) -> client.describePublicIpv4PoolsPaginator(request).publicIpv4Pools(),
                                ec2Filters(TAG_FILTER)),
                        // every region returns the same regions
                        globalTable(
                                "regions",
                                Region.builder().sdkFields(),
                                DescribeRegionsRequest::builder,
                                request -> ec2.describeRegions(request).regions(),
                                ec2Filters("endpoint", "opt-in-status", "region-name")),
                        simpleRegionalTable(
                                "route_tables",
                                RouteTable.builder().sdkFields(),
                                DescribeRouteTablesRequest::builder,
                                (client, request) -> client.describeRouteTablesPaginator(request).routeTables(),
                                ec2Filters(TAG_FILTER, "owner-id", "route-table-id", "vpc-id")),
                        regionalTable(
                                "snapshots",
                                SdkTableColumns.builder(Snapshot.builder().sdkFields()),
                                ec2Filters(TAG_FILTER, "description", "encrypted", "storage-tier", "transfer-type", "volume-id", "volume-size")
                                        .withColumnFilter("state", "status"),
                                AwsTables::snapshots,
                                new SnapshotFilter())
                                .build(),
                        simpleRegionalTable(
                                "security_groups",
                                SecurityGroup.builder().sdkFields(),
                                DescribeSecurityGroupsRequest::builder,
                                (client, request) -> client.describeSecurityGroupsPaginator(request).securityGroups(),
                                ec2Filters(TAG_FILTER, "description", "group-id", "group-name", "owner-id", "vpc-id")),
                        simpleRegionalTable(
                                "subnets",
                                Subnet.builder().sdkFields(),
                                DescribeSubnetsRequest::builder,
                                (client, request) -> client.describeSubnetsPaginator(request).subnets(),
                                ec2Filters(TAG_FILTER, "availability-zone", "availability-zone-id", "available-ip-address-count", "cidr-block", "customer-owned-ipv4-pool", "default-for-az", "enable-dns64", "enable-lni-at-device-index", "ipv6-native", "map-customer-owned-ip-on-launch", "map-public-ip-on-launch", "outpost-arn", "owner-id", "state", "subnet-arn", "subnet-id", "vpc-id")),
                        simpleRegionalTable(
                                "tags",
                                TagDescription.builder().sdkFields(),
                                DescribeTagsRequest::builder,
                                (client, request) -> client.describeTagsPaginator(request).tags(),
                                ec2Filters("key", "resource-id", "resource-type", "value")),
                        simpleRegionalTable(
                                "volumes",
                                Volume.builder().sdkFields(),
                                DescribeVolumesRequest::builder,
                                (client, request) -> client.describeVolumesPaginator(request).volumes(),
                                ec2Filters(TAG_FILTER, "availability-zone", "availability-zone-id", "encrypted", "fast-restored", "multi-attach-enabled", "size", "snapshot-id", "volume-id", "volume-type")
                                        .withColumnFilter("state", "status")),
                        simpleRegionalTable(
                                "vpc_endpoints",
                                VpcEndpoint.builder().sdkFields(),
                                DescribeVpcEndpointsRequest::builder,
                                (client, request) -> client.describeVpcEndpointsPaginator(request).vpcEndpoints(),
                                ec2Filters(TAG_FILTER, "ip-address-type", "service-name", "service-region", "vpc-endpoint-id", "vpc-endpoint-type", "vpc-id")),
                        simpleRegionalTable(
                                "vpc_peering_connections",
                                VpcPeeringConnection.builder().sdkFields(),
                                DescribeVpcPeeringConnectionsRequest::builder,
                                (client, request) -> client.describeVpcPeeringConnectionsPaginator(request).vpcPeeringConnections(),
                                ec2Filters(TAG_FILTER, "vpc-peering-connection-id")),
                        simpleRegionalTable(
                                "vpcs",
                                Vpc.builder().sdkFields(),
                                DescribeVpcsRequest::builder,
                                (client, request) -> client.describeVpcsPaginator(request).vpcs(),
                                ec2Filters(TAG_FILTER, "dhcp-options-id", "is-default", "owner-id", "state", "vpc-id")),
                        simpleRegionalTable(
                                "vpn_connections",
                                VpnConnection.builder().sdkFields(),
                                DescribeVpnConnectionsRequest::builder,
                                (client, request) -> client.describeVpnConnections(request).vpnConnections(),
                                ec2Filters(TAG_FILTER, "customer-gateway-id", "state", "transit-gateway-id", "type", "vpn-connection-id", "vpn-gateway-id")),
                        simpleRegionalTable(
                                "vpn_gateways",
                                VpnGateway.builder().sdkFields(),
                                DescribeVpnGatewaysRequest::builder,
                                (client, request) -> client.describeVpnGateways(request).vpnGateways(),
                                ec2Filters(TAG_FILTER, "amazon-side-asn", "availability-zone", "state", "type", "vpn-gateway-id")),
                        AwsTable.builder(new SchemaTableName("s3", "buckets"), SdkTableColumns.of(Bucket.builder().sdkFields()))
                                .rowSource((_, _) -> sdkRows(s3.listBucketsPaginator(ListBucketsRequest.builder().build()).buckets()))
                                .inserter(new Inserter(CreateBucketRequest::builder, request -> createBucket(s3, (CreateBucketRequest) request), bucketSetters()))
                                .build(),
                        bucketTable("objects", S3Object.builder().sdkFields(), (bucketName, prefix) -> s3.listObjectsV2Paginator(ListObjectsV2Request.builder().bucket(bucketName).prefix(prefix.orElse(null)).build()).contents()),
                        bucketTable("object_versions", ObjectVersion.builder().sdkFields(), (bucketName, prefix) -> s3.listObjectVersionsPaginator(ListObjectVersionsRequest.builder().bucket(bucketName).prefix(prefix.orElse(null)).build()).versions()),
                        bucketTable("deleted_objects", DeleteMarkerEntry.builder().sdkFields(), (bucketName, prefix) -> s3.listObjectVersionsPaginator(ListObjectVersionsRequest.builder().bucket(bucketName).prefix(prefix.orElse(null)).build()).deleteMarkers()))
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

    /**
     * An EC2 table that's not regional, because every region returns the same rows.
     */
    private static <R extends Ec2Request> AwsTable globalTable(String table, List<SdkField<?>> sdkFields, Supplier<? extends SdkBuilder<?, R>> request, Function<R, Iterable<? extends SdkPojo>> rows, Ec2FilterNames filterNames)
    {
        SdkTableColumns columns = SdkTableColumns.of(sdkFields);
        Ec2Filter ec2Filter = new Ec2Filter(columns.columnMetadata(), filterNames);
        return AwsTable.builder(new SchemaTableName("ec2", table), columns)
                .rowSource((_, handle) -> sdkRows(rows.apply(withFilters(request.get(), ec2Filter.filters(handle.constraint())))))
                .filterApplier(ec2Filter)
                .build();
    }

    private <R extends Ec2Request> AwsTable simpleRegionalTable(String table, List<SdkField<?>> sdkFields, Supplier<? extends SdkBuilder<?, R>> request, BiFunction<Ec2Client, R, Iterable<? extends SdkPojo>> rows, Ec2FilterNames filterNames)
    {
        return regionalTable(table, SdkTableColumns.builder(sdkFields), filterNames, (client, filters, _, _) -> rows.apply(client, withFilters(request.get(), filters)))
                .build();
    }

    /**
     * Every regional table has a region column, which is a filter that divides the scans into a split for every region,
     * so the row source reads a single region using its client.
     * Predicates on columns matching the filter names of the describe operation are pushed down for every table, and given to the row source as EC2 filters.
     */
    private AwsTable.Builder regionalTable(String table, SdkTableColumns.Builder columns, Ec2FilterNames filterNames, RegionalRowSource rowSource, FilterApplier... tableFilters)
    {
        SdkTableColumns tableColumns = columns.prepend(new ColumnMetadata(REGION_COLUMN, VARCHAR), SdkRow::context).build();
        Ec2Filter ec2Filter = new Ec2Filter(tableColumns.columnMetadata(), filterNames);
        return AwsTable.builder(new SchemaTableName("ec2", table), tableColumns)
                .rowSource(regionalRows(rowSource, ec2Filter))
                .filterApplier(new CombinedFilter(ImmutableList.<FilterApplier>builder()
                        .add(regionFilter)
                        .add(ec2Filter)
                        .add(tableFilters)
                        .build()));
    }

    private AwsTable.RowSource regionalRows(RegionalRowSource rowSource, Ec2Filter ec2Filter)
    {
        return (table, handle) -> {
            // the split manager divides scans by region, so there's a single region, unless the region filter wasn't used to create the splits
            String region = (String) table.requireFilterApplier().getFilter(table.columnHandle(REGION_COLUMN), handle.constraint(), ec2Clients.defaultRegion());
            Slice context = utf8Slice(region);
            Iterable<? extends SdkPojo> rows;
            try {
                rows = rowSource.rows(ec2Clients.client(region), ec2Filter.filters(handle.constraint()), table, handle);
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

    /**
     * The rows are listed by the bucket name and the prefix of the keys, if the filter on keys has one.
     */
    private static AwsTable bucketTable(String table, List<SdkField<?>> sdkFields, BiFunction<String, Optional<String>, Iterable<? extends SdkPojo>> rows)
    {
        return AwsTable.builder(
                        new SchemaTableName("s3", table),
                        SdkTableColumns.builder(sdkFields)
                                .prepend(new ColumnMetadata("bucket_name", VARCHAR), SdkRow::context)
                                .build())
                .rowSource((awsTable, handle) -> {
                    awsTable.requireFilterApplier().checkRequiredFilters(handle);
                    List<String> bucketNames = awsTable.requireFilterApplier().getFilterValues(awsTable.columnHandle("bucket_name"), handle.constraint());
                    Optional<String> prefix = awsTable.requireFilterApplier().getFilterPrefix(awsTable.columnHandle("key"), handle.constraint());
                    return Iterables.concat(Lists.transform(
                            bucketNames,
                            bucketName -> {
                                Slice context = utf8Slice(bucketName);
                                return Iterables.transform(rows.apply(bucketName, prefix), object -> new SdkRow(object, context));
                            }));
                })
                .filterApplier(new BucketFilter())
                .build();
    }

    // the list of filters is named Filters in most describe requests, but Filter in some, so it's found by the type of its elements
    @VisibleForTesting
    static <R extends Ec2Request> R withFilters(SdkBuilder<?, R> request, List<Filter> filters)
    {
        if (filters.isEmpty()) {
            return request.build();
        }
        SdkField<?> filtersField = ((SdkPojo) request).sdkFields().stream()
                .filter(field -> field.getOptionalTrait(ListTrait.class)
                        .map(list -> list.memberFieldInfo().constructor() != null && list.memberFieldInfo().constructor().get() instanceof Filter.Builder)
                        .orElse(false))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("%s does not have a list of filters".formatted(request.getClass().getName())));
        filtersField.set(request, filters);
        return request.build();
    }

    private static ColumnMetadata rowIdColumn()
    {
        return ColumnMetadata.builder().setName(ROW_ID).setType(VARCHAR).setHidden(true).build();
    }

    private static Iterable<Image> images(Ec2Client ec2, List<Filter> filters, AwsTable table, AwsTableHandle handle)
    {
        FilterApplier filter = table.requireFilterApplier();
        List<String> imageIds = filter.getFilterValues(table.columnHandle("image_id"), handle.constraint());
        List<String> names = imageNames(filter, table.columnHandle("name"), handle.constraint());
        List<String> owners = filter.getFilterValues(table.columnHandle("owner_id"), handle.constraint());
        ImmutableList.Builder<Filter> imageFilters = ImmutableList.<Filter>builder().addAll(filters);
        if (!names.isEmpty()) {
            imageFilters.add(Filter.builder().name("name").values(names).build());
        }
        List<Filter> requestFilters = imageFilters.build();
        DescribeImagesRequest.Builder request = DescribeImagesRequest.builder();
        if (!requestFilters.isEmpty()) {
            request.filters(requestFilters);
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

    // a range of names with a common prefix, like the one of name LIKE 'prefix%', is filtered using the prefix followed by a wildcard
    private static List<String> imageNames(FilterApplier filter, AwsColumnHandle column, TupleDomain<ColumnHandle> constraint)
    {
        List<String> names = filter.getFilterValues(column, constraint);
        if (!names.isEmpty()) {
            return names;
        }
        return filter.getFilterPrefix(column, constraint)
                .map(prefix -> List.of(prefix + "*"))
                .orElse(List.of());
    }

    private static Iterable<Snapshot> snapshots(Ec2Client ec2, List<Filter> filters, AwsTable table, AwsTableHandle handle)
    {
        FilterApplier filter = table.requireFilterApplier();
        List<String> snapshotIds = filter.getFilterValues(table.columnHandle("snapshot_id"), handle.constraint());
        List<String> owners = filter.getFilterValues(table.columnHandle("owner_id"), handle.constraint());
        DescribeSnapshotsRequest.Builder request = DescribeSnapshotsRequest.builder();
        if (!filters.isEmpty()) {
            request.filters(filters);
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

    // unlike the InstanceIds parameter, the instance-id filter returns no rows for missing IDs, instead of failing the whole request
    private static Iterable<Instance> instances(Ec2Client ec2, List<Filter> filters)
    {
        Iterable<Reservation> reservations = ec2.describeInstancesPaginator(withFilters(DescribeInstancesRequest.builder(), filters)).reservations();
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
        Iterable<? extends SdkPojo> rows(Ec2Client ec2, List<Filter> filters, AwsTable table, AwsTableHandle handle);
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
