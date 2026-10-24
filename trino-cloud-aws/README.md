trino-cloud-aws
=================

This is a Trino connector to access AWS resources using SQL.

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
> see [Read-only IAM policy](#read-only-iam-policy).

# Usage

Not all resources are mapped yet, here's a list of the available schemas and tables:
* `ec2.availability_zones`
* `ec2.images`
* `ec2.instance_types`
* `ec2.instances`
* `ec2.key_pairs`
* `ec2.launch_templates`
* `ec2.nat_gateways`
* `ec2.network_interfaces`
* `ec2.placement_groups`
* `ec2.prefix_lists`
* `ec2.public_ipv4_pools`
* `ec2.regions`
* `ec2.route_tables`
* `ec2.security_groups`
* `ec2.snapshots`
* `ec2.subnets`
* `ec2.tags`
* `ec2.volumes`
* `ec2.vpc_endpoints`
* `ec2.vpc_peering_connections`
* `ec2.vpcs`
* `ec2.vpn_connections`
* `ec2.vpn_gateways`
* `s3.buckets`
* `s3.deleted_objects`
* `s3.objects`
* `s3.object_versions`

Some tables can only be queried with a filter in the `WHERE` clause:
* `s3.objects`, `s3.object_versions` and `s3.deleted_objects` require `bucket_name = '...'`
* `ec2.images` requires a filter on `image_id`, `name` or `owner_id`,
  see [#16](https://github.com/nineinchnick/trino-cloud/issues/16)

For example:
```sql
SELECT key, size FROM s3.objects WHERE bucket_name = 'my-bucket';
```

## Type mapping

Columns are derived from the fields of the AWS SDK model of each table.
Column and nested field names are the SDK member names in `snake_case`, like `instance_id` for `InstanceId`.
Acronyms stay whole, like `size_in_gb` for `SizeInGB`, and so do a few mixed-case words,
like `vcpu_info` for `VCpuInfo`, `etag` for `ETag` and `size_in_mib` for `SizeInMiB`.

| AWS SDK type                                        | Trino type                    |
|-----------------------------------------------------|-------------------------------|
| `String`                                            | `varchar`                     |
| `Byte`                                              | `tinyint`                     |
| `Short`                                             | `smallint`                    |
| `Integer`                                           | `integer`                     |
| `Long`                                              | `bigint`                      |
| `Float`                                             | `real`                        |
| `Double`                                            | `double`                      |
| `Boolean`                                           | `boolean`                     |
| `Instant`                                           | `timestamp(3) with time zone` |
| `SdkBytes`                                          | `varbinary`                   |
| `BigDecimal`                                        | `varchar`                     |
| `Document`                                          | `varchar` with JSON text      |
| Nested object                                       | `row(...)`                    |
| `List`                                              | `array(...)`                  |
| `Map`                                               | `map(varchar, ...)`           |
| `List` of objects with a Key and a Value, like tags | `map(varchar, varchar)`       |

Timestamps are in UTC. Values that aren't set in the AWS API response are `NULL`.
Objects nested more than 8 levels deep are mapped to `varchar`, with the text representation of the SDK object.
Nested objects are rows, so their fields can be accessed using the dereference operator,
and tags are maps, indexed by the tag key:
```sql
SELECT instance_id, state.name, placement.availability_zone, tags['Name']
FROM ec2.instances
WHERE launch_time > now() - INTERVAL '1' DAY;
```

To find instances without a tag, use `element_at`, because the subscript operator fails for missing keys:
```sql
SELECT instance_id FROM ec2.instances WHERE element_at(tags, 'Owner') IS NULL;
```

## Writes

Writes are disabled by default, so the connector can't create, modify or remove any AWS resources.
To enable `INSERT`, `UPDATE` and `DELETE`, set `aws.read-only=false` in the catalog properties file.
Every user who can access the catalog can then write, acting as the AWS identity of the connector,
so use a separate catalog with its own credentials for writes, and limit who can access it
with Trino [access control](https://trino.io/docs/current/security/built-in-system-access-control.html).

Writes aren't transactional. Every row is a separate AWS API call, which can't be rolled back.
If a statement fails, the resources created, modified or removed for earlier rows stay that way.
The coordinator logs the IDs of the resources created by each `INSERT`,
and workers log the resources created by an `INSERT` that failed.

A single `INSERT` can insert up to `aws.max-inserted-rows` rows, 10 by default.
When launching instances, the connector sets a client token derived from the query and the row,
so if a request is sent again for the same row, it doesn't launch another instance.

The following tables support inserting into:
* `ec2.images`, which creates an image from the instance set in the `instance_id` column
* `ec2.instances`, which launches an instance
* `s3.buckets`, which creates a bucket

Columns are set in the fields of the AWS API request with the same name, so only some columns can be inserted,
and inserting a column that doesn't have a matching request field fails.
The values are converted back to the types of the request fields, so rows are set as nested objects.
The `tags` column sets the tags of the created instance or image.
For example:
```sql
INSERT INTO ec2.instances (image_id, instance_type, key_name, ebs_optimized, tags)
VALUES ('ami-05f7491af5eef733a', 't2.micro', 'default', false, MAP(ARRAY['Name'], ARRAY['web']));
```

When inserting into `s3.buckets`, the bucket is created in the region of the catalog, unless the `bucket_region` column is set:
```sql
INSERT INTO s3.buckets (name, bucket_region) VALUES ('my-bucket', 'eu-central-1');
```

The `ec2.instances` table also has limited `UPDATE` and `DELETE` support.
`UPDATE` and `DELETE` require a filter on the primary key column, like `WHERE instance_id = '...'`,
so a single statement can't change or remove every resource at once.

Only the `state` and `instance_type` columns can be updated.
To stop or start an instance, set its state name to `stopped` or `running`:
```sql
UPDATE ec2.instances SET state = ROW(NULL, 'stopped') WHERE instance_id = 'i-04a7cf7ca232cd251';
```

To change the instance type, stop the instance first, because AWS only allows it for stopped instances:
```sql
UPDATE ec2.instances SET instance_type = 't3.small' WHERE instance_id = 'i-04a7cf7ca232cd251';
```

To terminate it, delete the row:
```sql
DELETE FROM ec2.instances WHERE instance_id = 'i-04a7cf7ca232cd251';
```

> Note that the row won't be immediately deleted if the instance is running,
> as it'll be in the `shutting-down` state for a while.

# Configuration

The following configuration options are recognized by this connector:

| Property name           | Description                                                                                                                                     | Default                               |
|-------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------|---------------------------------------|
| `aws.region`            | AWS region of the EC2 and S3 endpoints, like `eu-central-1`. Unknown region names are rejected when the catalog is created. Replaces `region`.  | Region from the AWS SDK default chain |
| `aws.endpoint`          | Endpoint URL that overrides the AWS service endpoints, for example `http://localhost:4566` for an emulator. Enables S3 path-style access.       |                                       |
| `aws.access-key`        | AWS access key ID. Requires `aws.secret-key`.                                                                                                   |                                       |
| `aws.secret-key`        | AWS secret access key. Requires `aws.access-key`.                                                                                               |                                       |
| `aws.profile`           | Name of a profile from the AWS shared `~/.aws/config` and `~/.aws/credentials` files. Can't be used with `aws.access-key` and `aws.secret-key`. |                                       |
| `aws.iam-role`          | ARN of an IAM role to assume using STS, with the credentials from the other properties.                                                         |                                       |
| `aws.external-id`       | External ID required by the trust policy of the role set in `aws.iam-role`. Requires `aws.iam-role`.                                            |                                       |
| `aws.role-session-name` | Session name used when assuming the role set in `aws.iam-role`.                                                                                 | `trino-cloud`                         |
| `aws.max-connections`   | Maximum number of concurrent connections to each AWS service.                                                                                   | `50`                                  |
| `aws.retry-mode`        | AWS SDK retry mode used when requests fail or are throttled: `STANDARD`, `LEGACY` or `ADAPTIVE`.                                                | `ADAPTIVE`                            |
| `aws.max-error-retries` | Maximum number of retries of a failed AWS request.                                                                                              | `10`                                  |
| `aws.read-only`         | Reject `INSERT`, `UPDATE` and `DELETE`, which create, modify and remove AWS resources. See [Writes](#writes).                                   | `true`                                |
| `aws.max-inserted-rows` | Maximum number of rows a single `INSERT` can insert, to limit the number of resources it creates. The limit applies to each writer task.        | `10`                                  |

If `aws.region` is not set, the connector uses the
[AWS SDK default region provider chain](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/region-selection.html),
which reads the `AWS_REGION` environment variable, the `aws.region` Java system property, or the region of the AWS profile.
If no region can be resolved, creating the catalog fails.

## Credentials

Without `aws.access-key`, `aws.secret-key` or `aws.profile`, the connector reads credentials using the
[AWS SDK default credentials provider chain](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/credentials-chain.html).
It checks, in order: Java system properties, environment variables like `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`
and `AWS_SESSION_TOKEN`, web identity tokens, the shared `~/.aws/credentials` and `~/.aws/config` files,
and container or EC2 instance roles.

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

Each catalog can use different credentials, so one Trino server can query multiple AWS accounts at the same time.
For example, create a `production.properties` catalog file that uses a profile:
```
connector.name=aws
aws.region=eu-central-1
aws.profile=production
```

And a `staging.properties` catalog file that assumes a role in another account:
```
connector.name=aws
aws.region=eu-west-1
aws.iam-role=arn:aws:iam::123456789012:role/trino-readonly
aws.external-id=${ENV:STAGING_EXTERNAL_ID}
```

Avoid putting secret keys directly in catalog files. Use environment variables instead, like
`aws.secret-key=${ENV:STAGING_SECRET_KEY}`. Trino masks the value of `aws.secret-key` when it logs the configuration.

## Read-only IAM policy

With the default `aws.read-only=true`, the connector only needs permissions to list resources.
Use an IAM policy like the following one, to make sure the connector can't change anything,
even if writes are enabled by mistake:

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

To limit the buckets that can be listed, replace the `Resource` of the `ListS3` statement
with bucket ARNs, like `arn:aws:s3:::my-bucket`, and move `s3:ListAllMyBuckets` to a statement with `"Resource": "*"`.

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
2. Otherwise, use `AwsTable.builder(name, columns)` and set a `rowSource`, which generates rows for a table handle.
   Use `SdkColumns.columns(extraColumns, sdkFields)` to add columns that aren't in the SDK model.
3. Profit!

> Note: if the corresponding API method requires predicates that don't map to any field in the response,
> add an extra column, set a `filterApplier` that supports it, and populate the column from the predicate in the row source.
> See the S3 `bucketTable` definitions.

## INSERT support

Set an `inserter` on the table definition, with a supplier of the AWS SDK request builder and a function that executes the request.
Table columns are mapped to the request fields with the same name, and their values are converted to the types of the request fields.
Inserting a column that doesn't have a matching field in the request fails.
For columns that map to request fields with a different name or type, like `tags`, add a column setter to the inserter.

> Note: if the corresponding API method requires values for fields that don't map to any column,
> add extra columns, and populate them with nulls in the row source.

## UPDATE and DELETE support

1. Add a hidden `row_id` column to the table columns, and populate it with a unique identifier in the row source.
   See the `ec2.instances` definition.
1. Set the `primaryKey` column, which `UPDATE` and `DELETE` statements must filter on, so the filter applier must support it.
1. Set an `updater` and/or a `deleter` on the table definition. They receive the row ID, and the updater also receives the values of the updated columns.
   The updater lists the columns it supports, and updating other columns is rejected when planning the query.

Updates might perform multiple different API calls, depending on which columns are being set.
