trino-cloud-aws
=================

The AWS connector allows querying and managing [Amazon Web
Services](https://aws.amazon.com/) resources, like EC2 instances or S3 buckets,
using SQL. Every table maps to an AWS API operation that lists resources, and
every row is one resource returned by that operation.

The connector can also create, modify and remove some resources using `INSERT`,
`UPDATE` and `DELETE` statements. Writes are disabled by default, see
[SQL support](#sql-support).

# Quick Start

To run a Docker container with this connector, set the appropriate environmental variables, and run the following,
replacing `<version>` with the [latest release](https://github.com/nineinchnick/trino-cloud/releases), without the `v` prefix:
```bash
docker run \
  -d \
  --name trino-cloud \
  -e AWS_ACCESS_KEY_ID \
  -e AWS_SECRET_ACCESS_KEY \
  -e AWS_SESSION_TOKEN \
  -e AWS_REGION \
  -p 8080:8080 \
  nineinchnick/trino-cloud:<version>
```

Then use your favourite SQL client to connect to Trino running at http://localhost:8080,
or use the Trino CLI included in the image:
```bash
docker exec -it trino-cloud trino --catalog aws --schema ec2
```

> Warning: the Docker image doesn't configure any authentication. Anyone who can reach port 8080
> can run queries using the AWS credentials of the container. Only publish the port on trusted networks,
> like `-p 127.0.0.1:8080:8080`, and use credentials with read-only permissions,
> see [IAM permissions](#iam-permissions).

## Using an AWS CLI profile

To use a profile from the AWS CLI configuration, mount the `~/.aws` directory and set `AWS_PROFILE`:
```bash
docker run \
  -d \
  --name trino-cloud \
  -v ~/.aws:/home/trino/.aws:ro \
  -e AWS_PROFILE=my-profile \
  -e AWS_REGION \
  -p 8080:8080 \
  nineinchnick/trino-cloud:<version>
```

For SSO profiles, run `aws sso login --profile my-profile` first, so the cached token is valid.

# Requirements

To connect to AWS, you need:

- The Trino version the connector was built for. Each release of the connector
  is built for one Trino version, set in the `dep.trino.version` property of the
  `pom.xml` file of the release tag, and the plugin might not load in other
  versions.
- Network access from the Trino coordinator and workers to the AWS EC2 and S3
  endpoints of the configured regions, or to the endpoint set in the
  `aws.endpoint` property.
- An AWS account, and credentials of an IAM identity with permissions to call
  the AWS API operations of the queried tables, see [IAM permissions](#iam-permissions).

## IAM permissions

With the default `aws.read-only=true`, the connector only needs permissions to
list resources. Use an IAM policy like the following one, to make sure the
connector can't change anything, even if writes are enabled by mistake:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "DescribeEc2",
      "Effect": "Allow",
      "Action": [
        "ec2:DescribeAvailabilityZones",
        "ec2:DescribeImages",
        "ec2:DescribeInstanceTypes",
        "ec2:DescribeInstances",
        "ec2:DescribeKeyPairs",
        "ec2:DescribeLaunchTemplates",
        "ec2:DescribeNatGateways",
        "ec2:DescribeNetworkInterfaces",
        "ec2:DescribePlacementGroups",
        "ec2:DescribePrefixLists",
        "ec2:DescribePublicIpv4Pools",
        "ec2:DescribeRegions",
        "ec2:DescribeRouteTables",
        "ec2:DescribeSecurityGroups",
        "ec2:DescribeSnapshots",
        "ec2:DescribeSubnets",
        "ec2:DescribeTags",
        "ec2:DescribeVolumes",
        "ec2:DescribeVpcEndpoints",
        "ec2:DescribeVpcPeeringConnections",
        "ec2:DescribeVpcs",
        "ec2:DescribeVpnConnections",
        "ec2:DescribeVpnGateways"
      ],
      "Resource": "*"
    },
    {
      "Sid": "ListS3",
      "Effect": "Allow",
      "Action": [
        "s3:ListAllMyBuckets",
        "s3:ListBucket",
        "s3:ListBucketVersions"
      ],
      "Resource": "*"
    }
  ]
}
```

To limit the buckets that can be listed, replace the `Resource` of the `ListS3`
statement with bucket ARNs, like `arn:aws:s3:::my-bucket`, and move
`s3:ListAllMyBuckets` to a statement with `"Resource": "*"`.

To use the statements described in [SQL support](#sql-support) with `aws.read-only=false`,
the IAM identity also needs the permissions to call the following AWS API
operations:

IAM actions required for writes:

| Statement                   | IAM actions                                                                                                                                  |
|-----------------------------|----------------------------------------------------------------------------------------------------------------------------------------------|
| `INSERT INTO ec2.images`    | `ec2:CreateImage`, and `ec2:CreateTags` to set the `tags` column                                                                             |
| `DELETE FROM ec2.images`    | `ec2:DeregisterImage`                                                                                                                        |
| `INSERT INTO ec2.instances` | `ec2:RunInstances`, `ec2:CreateTags` to set the `tags` column, and `iam:PassRole` to launch instances with an instance profile               |
| `UPDATE ec2.instances`      | `ec2:StartInstances` and `ec2:StopInstances` to set the `state_name` column, `ec2:ModifyInstanceAttribute` to set the `instance_type` column |
| `DELETE FROM ec2.instances` | `ec2:TerminateInstances`                                                                                                                     |
| `INSERT INTO s3.buckets`    | `s3:CreateBucket`                                                                                                                            |

Use a separate catalog with its own credentials for writes, and limit the
resources these actions can be applied to with IAM policy conditions.

# Configuration

To configure the AWS connector, create a catalog properties file in
`etc/catalog` named, for example, `example.properties`, to mount the AWS
connector as the `example` catalog. Create the file with the following
contents, replacing the region as appropriate:

```text
connector.name=aws
aws.region=eu-central-1
```

If `aws.region` is not set, the connector uses the [AWS SDK default region
provider
chain](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/region-selection.html),
which reads the `AWS_REGION` environment variable, the `aws.region` Java system
property, or the region of the AWS profile. If no region can be resolved, or the
region name is unknown, creating the catalog fails.

## Multiple regions

By default, the tables of regional EC2 resources only list resources in the
region set in `aws.region`. To query several regions from one catalog, list them
in `aws.regions`:

```text
connector.name=aws
aws.region=eu-central-1
aws.regions=eu-central-1,eu-west-1,us-east-1
```

Set `aws.regions=*` to query all regions enabled for the account. The connector
lists the enabled regions using `ec2:DescribeRegions` in the region set in
`aws.region` when they are first needed, and doesn't list them again until the
catalog is restarted.

Regional tables have a `region` column, see [Tables](#tables). The
`ec2.regions` table and the tables in the `s3` schema always use the region set
in `aws.region`. `INSERT` statements create resources in the region set in
`aws.region`, while `UPDATE` and `DELETE` statements change and remove resources
in the region they were read from.

## Credentials

Without `aws.access-key`, `aws.secret-key` or `aws.profile`, the connector reads
credentials using the [AWS SDK default credentials provider
chain](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/credentials-chain.html).
It checks, in order: Java system properties, environment variables like
`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` and `AWS_SESSION_TOKEN`, web
identity tokens, the shared `~/.aws/credentials` and `~/.aws/config` files, and
container or EC2 instance roles.

When using an SSO profile, run `aws sso login --profile my-profile` before
starting Trino, so the cached token is valid.

Each catalog can use different credentials, so one Trino cluster can query
multiple AWS accounts at the same time. For example, create a
`production.properties` catalog file that uses a profile:

```text
connector.name=aws
aws.region=eu-central-1
aws.profile=production
```

And a `staging.properties` catalog file that assumes a role in another account:

```text
connector.name=aws
aws.region=eu-west-1
aws.iam-role=arn:aws:iam::123456789012:role/trino-readonly
aws.external-id=${ENV:STAGING_EXTERNAL_ID}
```

Avoid putting secret keys directly in catalog files. Use environment variables
instead, like `aws.secret-key=${ENV:STAGING_SECRET_KEY}`. Trino masks the value
of `aws.secret-key` when it logs the configuration.

# Configuration properties

The following configuration properties are available:

AWS configuration properties:

| Property name           | Description                                                                                                                                                                                                                       | Default                                               |
|-------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------|
| `aws.region`            | AWS region of the EC2 and S3 endpoints, like `eu-central-1`. Unknown region names are rejected when the catalog is created. Replaces the deprecated `region` property.                                                            | Region from the AWS SDK default region provider chain |
| `aws.regions`           | Comma-separated list of AWS regions queried by the regional EC2 tables, or `*` for all regions enabled for the account. Unknown region names are rejected when the catalog is created. See [Multiple regions](#multiple-regions). | The region set in `aws.region`                        |
| `aws.endpoint`          | Endpoint URL that overrides the AWS service endpoints, for example `http://localhost:4566` for an emulator. Enables S3 path-style access.                                                                                         |                                                       |
| `aws.access-key`        | AWS access key ID. Requires `aws.secret-key`.                                                                                                                                                                                     |                                                       |
| `aws.secret-key`        | AWS secret access key. Requires `aws.access-key`.                                                                                                                                                                                 |                                                       |
| `aws.profile`           | Name of a profile from the AWS shared `~/.aws/config` and `~/.aws/credentials` files. Can't be used with `aws.access-key` and `aws.secret-key`.                                                                                   |                                                       |
| `aws.iam-role`          | ARN of an IAM role to assume using STS, with the credentials from the other properties.                                                                                                                                           |                                                       |
| `aws.external-id`       | External ID required by the trust policy of the role set in `aws.iam-role`. Requires `aws.iam-role`.                                                                                                                              |                                                       |
| `aws.role-session-name` | Session name used when assuming the role set in `aws.iam-role`.                                                                                                                                                                   | `trino-cloud`                                         |
| `aws.max-connections`   | Maximum number of concurrent connections to each AWS service.                                                                                                                                                                     | `50`                                                  |
| `aws.retry-mode`        | AWS SDK retry mode used when requests fail or are throttled: `STANDARD`, `LEGACY` or `ADAPTIVE`.                                                                                                                                  | `ADAPTIVE`                                            |
| `aws.max-error-retries` | Maximum number of retries of a failed AWS request.                                                                                                                                                                                | `10`                                                  |
| `aws.read-only`         | Reject `INSERT`, `UPDATE` and `DELETE` statements, which create, modify and remove AWS resources. See [SQL support](#sql-support).                                                                                                | `true`                                                |
| `aws.max-inserted-rows` | Maximum number of rows a single `INSERT` statement can insert, to limit the number of resources it creates. The limit applies to each writer task.                                                                                | `10`                                                  |

# Type mapping

The columns of every table are derived from the fields of the AWS SDK for Java
model of the listed resources. The connector maps the SDK types to Trino types
according to the following table:

AWS SDK type to Trino type mapping:

| AWS SDK type                                                  | Trino type                    |
|---------------------------------------------------------------|-------------------------------|
| `String`                                                      | `VARCHAR`                     |
| `Byte`                                                        | `TINYINT`                     |
| `Short`                                                       | `SMALLINT`                    |
| `Integer`                                                     | `INTEGER`                     |
| `Long`                                                        | `BIGINT`                      |
| `Float`                                                       | `REAL`                        |
| `Double`                                                      | `DOUBLE`                      |
| `Boolean`                                                     | `BOOLEAN`                     |
| `Instant`                                                     | `TIMESTAMP(3) WITH TIME ZONE` |
| `SdkBytes`                                                    | `VARBINARY`                   |
| `BigDecimal`                                                  | `VARCHAR`                     |
| `Document`                                                    | `VARCHAR` with JSON text      |
| Nested object                                                 | `ROW(...)`                    |
| `List`                                                        | `ARRAY(...)`                  |
| `Map`                                                         | `MAP(VARCHAR, ...)`           |
| `List` of objects with a `Key` and a `Value` field, like tags | `MAP(VARCHAR, VARCHAR)`       |

No other types are supported.

Timestamps are in UTC. Values that aren't set in the AWS API response are
`NULL`. Objects nested more than 8 levels deep are mapped to `VARCHAR`, with the
text representation of the SDK object.

Column and nested field names are the SDK member names in `snake_case`, like
`instance_id` for `InstanceId`. Acronyms stay whole, like `size_in_gb` for
`SizeInGB`, and so do a few mixed-case words, like `vcpu_info` for `VCpuInfo`,
`etag` for `ETag`, `size_in_mib` for `SizeInMiB` and `baseline_throughput_in_mbps`
for `BaselineThroughputInMBps`.

Nested objects are rows, so their fields can be accessed using the dereference
operator, and tags are maps, indexed by the tag key:

```sql
SELECT instance_id, state.name, placement.availability_zone, tags['Name']
FROM example.ec2.instances
WHERE launch_time > now() - INTERVAL '1' DAY;
```

To find resources without a tag, use `element_at`, because the subscript
operator fails for missing keys:

```sql
SELECT instance_id
FROM example.ec2.instances
WHERE element_at(tags, 'Owner') IS NULL;
```

Some tables have columns that differ from the SDK model:

- The `state_name` column of `ec2.instances` holds the same value as
  `state.name`, like `running`, so the state can be changed by updating a
  single value.
- The `instance_id` column of `ec2.images` is always `NULL` when reading. It
  sets the instance to create the image from when inserting.
- The `bucket_name` column of the `s3.objects`, `s3.object_versions` and
  `s3.deleted_objects` tables is the name of the listed bucket, taken from the
  required filter.

# Tables

The connector provides the `ec2` and `s3` schemas. The tables are not created by
users, and every catalog has the same set of tables.

The AWS API operations that list resources only support a few filters. The
connector pushes down equality and `IN` predicates on the columns listed in the
**Pushed-down filters** column of the following tables, so AWS returns fewer
rows. Predicates that allow `NULL` values are not pushed down. Trino applies all
other predicates after reading every row returned by the AWS API operation.

The `ec2` tables push down predicates on the columns matching the filters of
their EC2 describe operation, like `vpc_id` as the `vpc-id` filter, and the
`state_name` column of `ec2.instances` as the `instance-state-name` filter.
Equality and `IN` predicates on a tag value, listed like `tags[...]`, as in
`tags['Name'] = 'web'` or `element_at(tags, 'Name') IN ('web', 'db')`, are
pushed down as the `tag:Name` filter. EC2 filter values treat `*` and `?` as
wildcards, so Trino still applies these predicates to the returned rows.

Tables with a **Required filters** column value fail with the
`AWS_MISSING_PREDICATE` error code when the query doesn't have an equality or
`IN` predicate on that column, because the AWS API can't list the resources
without it.

When a pushed-down `IN` predicate lists more values than a single AWS request
accepts, the scan is divided into splits that run in parallel on multiple
workers: one split per bucket for the `s3` object tables, and one split per batch
of 100 values of a pushed-down `ec2` filter, like instance, image or snapshot
IDs. Regional tables are also read in a separate split for every region, and for
every batch of values in each region.

## `ec2` schema

The tables in the `ec2` schema, except `ec2.regions`, list resources in the
regions set in `aws.regions`, or in the region set in `aws.region`. They have a
`region` column with the region of every resource. Equality and `IN` predicates
on the `region` column are pushed down to choose the regions to read, and
regions that are not configured return no rows:

```sql
SELECT region, instance_id, state_name
FROM example.ec2.instances
WHERE region IN ('eu-central-1', 'eu-west-1');
```

The `region` column is not listed as a pushed-down filter in the following
table, but it's pushed down for every table except `ec2.regions`.

| Table                         | Description                            | Required filters | Pushed-down filters                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          | Supported writes             |
|-------------------------------|----------------------------------------|------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------------------|
| `ec2.availability_zones`      | Availability Zones and Local Zones     |                  | `group_long_name`, `group_name`, `opt_in_status`, `parent_zone_id`, `parent_zone_name`, `region_name`, `state`, `zone_id`, `zone_name`, `zone_type`                                                                                                                                                                                                                                                                                                                                                                                                          |                              |
| `ec2.images`                  | Amazon Machine Images (AMIs)           |                  | `architecture`, `boot_mode`, `creation_date`, `description`, `ena_support`, `free_tier_eligible`, `hypervisor`, `image_allowed`, `image_id`, `image_type`, `kernel_id`, `name`, `owner_id`, `public_ssm_parameter_name`, `ramdisk_id`, `root_device_name`, `root_device_type`, `source_image_id`, `source_image_region`, `source_instance_id`, `sriov_net_support`, `state`, `tags[...]`, `virtualization_type`                                                                                                                                              | `INSERT`, `DELETE`           |
| `ec2.instance_types`          | Instance types available in the region |                  | `auto_recovery_supported`, `bare_metal`, `burstable_performance_supported`, `current_generation`, `dedicated_hosts_supported`, `free_tier_eligible`, `hibernation_supported`, `hypervisor`, `instance_storage_supported`, `instance_type`, `nitro_enclaves_support`, `nitro_tpm_support`, `reboot_migration_support`                                                                                                                                                                                                                                         |                              |
| `ec2.instances`               | EC2 instances                          |                  | `architecture`, `boot_mode`, `capacity_reservation_id`, `client_token`, `current_instance_boot_mode`, `ebs_optimized`, `ena_support`, `hypervisor`, `image_id`, `instance_id`, `instance_lifecycle`, `instance_type`, `ipv6_address`, `kernel_id`, `key_name`, `outpost_arn`, `platform_details`, `private_dns_name`, `private_ip_address`, `ramdisk_id`, `root_device_name`, `root_device_type`, `source_dest_check`, `spot_instance_request_id`, `state_name`, `subnet_id`, `tags[...]`, `tpm_support`, `usage_operation`, `virtualization_type`, `vpc_id` | `INSERT`, `UPDATE`, `DELETE` |
| `ec2.key_pairs`               | Key pairs                              |                  | `key_name`, `key_pair_id`, `tags[...]`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |                              |
| `ec2.launch_templates`        | Launch templates                       |                  | `launch_template_name`, `tags[...]`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |                              |
| `ec2.nat_gateways`            | NAT gateways                           |                  | `nat_gateway_id`, `state`, `subnet_id`, `tags[...]`, `vpc_id`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |                              |
| `ec2.network_interfaces`      | Network interfaces                     |                  | `availability_zone`, `availability_zone_id`, `description`, `interface_type`, `mac_address`, `network_interface_id`, `owner_id`, `private_dns_name`, `private_ip_address`, `requester_id`, `requester_managed`, `source_dest_check`, `status`, `subnet_id`, `tag_set[...]`, `vpc_id`                                                                                                                                                                                                                                                                         |                              |
| `ec2.placement_groups`        | Placement groups                       |                  | `group_arn`, `group_name`, `spread_level`, `state`, `strategy`, `tags[...]`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |                              |
| `ec2.prefix_lists`            | Prefix lists of AWS services           |                  | `prefix_list_id`, `prefix_list_name`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |                              |
| `ec2.public_ipv4_pools`       | Public IPv4 address pools              |                  | `tags[...]`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |                              |
| `ec2.regions`                 | Regions enabled for the account        |                  | `endpoint`, `opt_in_status`, `region_name`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |                              |
| `ec2.route_tables`            | Route tables                           |                  | `owner_id`, `route_table_id`, `tags[...]`, `vpc_id`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |                              |
| `ec2.security_groups`         | Security groups                        |                  | `description`, `group_id`, `group_name`, `owner_id`, `tags[...]`, `vpc_id`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |                              |
| `ec2.snapshots`               | EBS snapshots                          |                  | `description`, `encrypted`, `owner_id`, `snapshot_id`, `state`, `storage_tier`, `tags[...]`, `transfer_type`, `volume_id`, `volume_size`                                                                                                                                                                                                                                                                                                                                                                                                                     |                              |
| `ec2.subnets`                 | Subnets                                |                  | `availability_zone`, `availability_zone_id`, `available_ip_address_count`, `cidr_block`, `customer_owned_ipv4_pool`, `default_for_az`, `enable_dns64`, `enable_lni_at_device_index`, `ipv6_native`, `map_customer_owned_ip_on_launch`, `map_public_ip_on_launch`, `outpost_arn`, `owner_id`, `state`, `subnet_arn`, `subnet_id`, `tags[...]`, `vpc_id`                                                                                                                                                                                                       |                              |
| `ec2.tags`                    | Tags of all EC2 resources              |                  | `key`, `resource_id`, `resource_type`, `value`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |                              |
| `ec2.volumes`                 | EBS volumes                            |                  | `availability_zone`, `availability_zone_id`, `encrypted`, `fast_restored`, `multi_attach_enabled`, `size`, `snapshot_id`, `state`, `tags[...]`, `volume_id`, `volume_type`                                                                                                                                                                                                                                                                                                                                                                                   |                              |
| `ec2.vpc_endpoints`           | VPC endpoints                          |                  | `ip_address_type`, `service_name`, `service_region`, `tags[...]`, `vpc_endpoint_id`, `vpc_endpoint_type`, `vpc_id`                                                                                                                                                                                                                                                                                                                                                                                                                                           |                              |
| `ec2.vpc_peering_connections` | VPC peering connections                |                  | `tags[...]`, `vpc_peering_connection_id`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |                              |
| `ec2.vpcs`                    | VPCs                                   |                  | `dhcp_options_id`, `is_default`, `owner_id`, `state`, `tags[...]`, `vpc_id`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |                              |
| `ec2.vpn_connections`         | Site-to-Site VPN connections           |                  | `customer_gateway_id`, `state`, `tags[...]`, `transit_gateway_id`, `type`, `vpn_connection_id`, `vpn_gateway_id`                                                                                                                                                                                                                                                                                                                                                                                                                                             |                              |
| `ec2.vpn_gateways`            | Virtual private gateways               |                  | `amazon_side_asn`, `availability_zone`, `state`, `tags[...]`, `type`, `vpn_gateway_id`                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |                              |

Without a filter on `owner_id`, the `ec2.images` and `ec2.snapshots` tables only
list images and snapshots owned by the account of the catalog credentials,
because listing all public images or snapshots is slow and rarely useful. A
filter on `image_id` or `snapshot_id` without a filter on `owner_id` lists the
images or snapshots with these IDs, including public and shared ones, and skips
IDs that don't exist. To list public images, filter on the owner account ID:

```sql
SELECT image_id, name, creation_date
FROM example.ec2.images
WHERE owner_id = '137112412989' AND name LIKE 'al2023-ami-2023%';
```

A `LIKE` predicate on the `name` column of `ec2.images` is pushed down as the
constant prefix of its pattern followed by the `*` wildcard, like
`al2023-ami-2023*` in the previous example. Trino applies the rest of the
pattern.

## `s3` schema

The tables in the `s3` schema list buckets in all regions, and access each
bucket in its own region.

| Table                | Description                                     | Required filters | Pushed-down filters  | Supported writes |
|----------------------|-------------------------------------------------|------------------|----------------------|------------------|
| `s3.buckets`         | Buckets owned by the account                    |                  |                      | `INSERT`         |
| `s3.objects`         | Current versions of objects in a bucket         | `bucket_name`    | `bucket_name`, `key` |                  |
| `s3.object_versions` | All versions of objects in a bucket             | `bucket_name`    | `bucket_name`, `key` |                  |
| `s3.deleted_objects` | Delete markers of objects in a versioned bucket | `bucket_name`    | `bucket_name`, `key` |                  |

For example, to list the largest objects in a bucket:

```sql
SELECT key, size, last_modified
FROM example.s3.objects
WHERE bucket_name = 'my-bucket'
ORDER BY size DESC
LIMIT 10;
```

Predicates on the `key` column that only match keys starting with the same
prefix, like `key LIKE 'logs/2024/%'`, `starts_with(key, 'logs/')`, or
`key >= 'logs/2024' AND key < 'logs/2025'`, are pushed down as the prefix of the
listed keys. Equality and `IN` predicates are pushed down as the longest prefix
of all their values. Trino applies the whole predicate to the listed keys, so
in `key LIKE 'logs/%.json'`, only the `logs/` prefix limits the listed keys.

# SQL support

The connector provides read access to all tables, and write access to a few of
them. In addition to the [globally available](https://trino.io/docs/current/language/sql-support.html#sql-globally-available) and [read
operation](https://trino.io/docs/current/language/sql-support.html#sql-read-operations) statements, the connector supports the
following features, when `aws.read-only` is set to `false`:

- [INSERT](https://trino.io/docs/current/sql/insert.html), see also [INSERT](#insert)
- [UPDATE](https://trino.io/docs/current/sql/update.html), see also [UPDATE](#update)
- [DELETE](https://trino.io/docs/current/sql/delete.html), see also [DELETE](#delete)

Writes are disabled by default, so the connector can't create, modify or remove
any AWS resources. Statements that write fail with the `NOT_SUPPORTED` error
code. To enable writes, set `aws.read-only=false` in the catalog properties
file. Every user who can access the catalog can then write, acting as the AWS
identity of the connector, so use a separate catalog with its own credentials
for writes, and limit who can access it with Trino [access
control](https://trino.io/docs/current/security/built-in-system-access-control.html). See [IAM permissions](#iam-permissions)
for the IAM actions required for writes.

## Non-transactional writes

Writes aren't transactional. Every row is a separate AWS API call, which can't
be rolled back. If a statement fails, the resources created, modified or removed
for earlier rows stay that way. The coordinator logs the IDs of the resources
created by each `INSERT`, and workers log the resources created by an `INSERT`
that failed.

Writes don't support [fault-tolerant execution](https://trino.io/docs/current/admin/fault-tolerant-execution.html),
and fail when the `retry-policy` property is set to `QUERY` or `TASK`.

## INSERT

The following tables support `INSERT`:

- `ec2.images`, which creates an image from the instance set in the
  `instance_id` column.
- `ec2.instances`, which launches one instance for every row.
- `s3.buckets`, which creates a bucket.

Images and instances are created in the region set in `aws.region`. Inserting a
different value in the `region` column fails.

A single `INSERT` statement can insert up to `aws.max-inserted-rows` rows, 10
by default, and fails with the `AWS_TOO_MANY_INSERTED_ROWS` error code when it
inserts more. When launching instances, the connector sets a client token
derived from the query and the row, so if a request is sent again for the same
row, it doesn't launch another instance.

Columns are set in the fields of the AWS API request with the same name, so only
some columns can be inserted, and inserting a column that doesn't have a
matching request field fails. The values are converted back to the types of the
request fields, so rows are set as nested objects. The `tags` column sets the
tags of the created instance or image. For example:

```sql
INSERT INTO example.ec2.instances (image_id, instance_type, key_name, ebs_optimized, tags)
VALUES ('ami-05f7491af5eef733a', 't2.micro', 'default', false, MAP(ARRAY['Name'], ARRAY['web']));
```

To create an image from an instance:

```sql
INSERT INTO example.ec2.images (instance_id, name)
VALUES ('i-04a7cf7ca232cd251', 'web-backup');
```

When inserting into `s3.buckets`, the bucket is created in the region of the
catalog, unless the `bucket_region` column is set:

```sql
INSERT INTO example.s3.buckets (name, bucket_region)
VALUES ('my-bucket', 'eu-central-1');
```

## UPDATE

Only the `ec2.instances` table supports `UPDATE`, and only of the `state_name`
and `instance_type` columns. Setting a column to `NULL` is not supported. `UPDATE`
requires a filter on the `instance_id` column, so a single statement can't
change every instance at once.

To stop or start an instance, set its state name to `stopped` or `running`:

```sql
UPDATE example.ec2.instances
SET state_name = 'stopped'
WHERE instance_id = 'i-04a7cf7ca232cd251';
```

To change the instance type, stop the instance first, because AWS only allows it
for stopped instances:

```sql
UPDATE example.ec2.instances
SET instance_type = 't3.small'
WHERE instance_id = 'i-04a7cf7ca232cd251';
```

When updating both columns in one statement, the connector changes the instance
type before starting the instance.

## DELETE

The following tables support `DELETE`:

- `ec2.images`, which deregisters the image. The snapshots of the image are not
  deleted.
- `ec2.instances`, which terminates the instance.

`DELETE` requires a filter on the `image_id` or `instance_id` column, so a
single statement can't remove every resource at once. Statements without it
fail with the `AWS_MISSING_PREDICATE` error code.

```sql
DELETE FROM example.ec2.instances
WHERE instance_id = 'i-04a7cf7ca232cd251';
```

The row isn't immediately removed from the table if the instance is running, as
it stays in the `shutting-down` state for a while.

# Error codes

The connector reports the following error codes, besides the standard Trino
error codes:

AWS connector error codes:

| Error code                   | Type         | Description                                                                                                                           |
|------------------------------|--------------|---------------------------------------------------------------------------------------------------------------------------------------|
| `AWS_API_ERROR`              | `EXTERNAL`   | An AWS API request failed, for example because of missing IAM permissions.                                                            |
| `AWS_THROTTLED`              | `EXTERNAL`   | AWS throttled the requests, and the retries set by `aws.retry-mode` and `aws.max-error-retries` were exhausted.                       |
| `AWS_MISSING_PREDICATE`      | `USER_ERROR` | The query doesn't have a filter required by the table, or an `UPDATE` or `DELETE` statement doesn't filter on the primary key column. |
| `AWS_TOO_MANY_INSERTED_ROWS` | `USER_ERROR` | An `INSERT` statement inserts more rows than `aws.max-inserted-rows`.                                                                 |

# Limitations

- `INSERT` statements create resources only in the region set in `aws.region`,
  even when the catalog queries several regions set in `aws.regions`.
- An error in one region, like failed authentication in a region that's not
  enabled for the account, fails the whole query. The error message names the
  region.
- Scans that are not divided by buckets, IDs or regions, as described in
  [Tables](#tables), run in a single split on one worker, because AWS returns
  the pages of a list operation one after another.
- The connector doesn't provide table or column statistics, so the cost-based
  optimizer can't estimate the size of the tables.
- Only the filters listed in [Tables](#tables) are pushed down. `LIMIT` and other
  predicates are applied by Trino after listing all matching resources.
- Every query lists the resources again, there is no caching.

# Build

Run all the unit test classes.
```
./mvnw test
```

Creates a deployable plugin directory and ZIP file in `trino-cloud-aws/target`
```
./mvnw clean package
```

# Deploy

An example command to run the Trino server with the aws plugin and catalog enabled,
using the plugin built from the current checkout.
Replace `<version>` with the project version from [pom.xml](../pom.xml), like `0.84-SNAPSHOT`,
and `<trino-version>` with the `dep.trino.version` property from the same file, like `483`.
The plugin only loads in the Trino version it was built for.

```bash
src=$(git rev-parse --show-toplevel)
docker run \
  -v $src/trino-cloud-aws/target/trino-cloud-aws-<version>:/usr/lib/trino/plugin/aws \
  -v $src/catalog/aws.properties:/etc/trino/catalog/aws.properties \
  -e AWS_ACCESS_KEY_ID \
  -e AWS_SECRET_ACCESS_KEY \
  -e AWS_SESSION_TOKEN \
  -e AWS_REGION \
  -p 8080:8080 \
  --name trino \
  -d \
  trinodb/trino:<trino-version>
```

Connect to that server using:
```bash
docker exec -it trino trino --catalog aws --schema ec2
```

# Adding new tables

All tables are defined in [AwsTables](src/main/java/pl/net/was/cloud/aws/AwsTables.java),
one [AwsTable](src/main/java/pl/net/was/cloud/aws/AwsTable.java) definition per table.

To add a new table that can be read from:

1. If the table lists a single AWS SDK model, add a `simpleTable(schema, name, Model.builder().sdkFields(), () -> client.describeX().items())` entry.
   Columns are derived from the SDK model fields, see [Type mapping](#type-mapping).
   If a name is split in the wrong place, or two fields would end up with the same column name, which fails creating the catalog,
   add the mixed-case word to `WORDS` in [SdkColumns](src/main/java/pl/net/was/cloud/aws/SdkColumns.java).
2. Otherwise, use `AwsTable.builder(name, SdkTableColumns.builder(sdkFields)...build())` and set a `rowSource`,
   which returns the SDK model objects for a table handle.
   To add columns that aren't in the SDK model, use `prepend` or `insertAfter` on the `SdkTableColumns` builder,
   with a function that computes the value from the SDK object. See the `ec2.instances` definition, which adds `row_id` and `state_name`.
3. Add the table to the [Tables](#tables) section of this README.
   `TestAwsDocumentation` fails if a table is missing from it.
4. Profit!

> Note: if the corresponding API method requires predicates that don't map to any field in the response,
> add an extra column, set a `filterApplier` that supports it, and pass the value from the predicate as the context of the rows,
> so the column can be computed with `SdkRow::context`.
> See the S3 `bucketTable` definitions.

## INSERT support

Set an `inserter` on the table definition, with a supplier of the AWS SDK request builder and a function that executes the request.
Table columns are mapped to the request fields with the same name, and their values are converted to the types of the request fields.
Inserting a column that doesn't have a matching field in the request fails.
For columns that map to request fields with a different name or type, like `tags`, add a column setter to the inserter.

> Note: if the corresponding API method requires values for fields that don't map to any column,
> add extra columns that always return `NULL`, like the `instance_id` column of `ec2.images`.

## UPDATE and DELETE support

1. Prepend a hidden `row_id` column with a unique identifier of the resource.
   See the `ec2.instances` definition.
1. Set the `primaryKey` column, which `UPDATE` and `DELETE` statements must filter on, so the filter applier must support it.
1. Set an `updater` and/or a `deleter` on the table definition. They receive the row ID, and the updater also receives the values of the updated columns.
   The updater lists the columns it supports, and updating other columns is rejected when planning the query.

Updates might perform multiple different API calls, depending on which columns are being set.
