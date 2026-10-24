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

The following tables support inserting into:
* `ec2.images`
* `ec2.instances`

Only `VARCHAR` columns can be set for now, see [#285](https://github.com/nineinchnick/trino-cloud/issues/285).
For example:
```sql
INSERT INTO ec2.instances (image_id, instance_type, key_name) VALUES ('ami-05f7491af5eef733a', 't2.micro', 'default');
```

The `ec2.instances` table also has limited `UPDATE` and `DELETE` support.

> Warning: `UPDATE` and `DELETE` are currently broken and fail for every row,
> see [#283](https://github.com/nineinchnick/trino-cloud/issues/283).
> The examples below show how they're meant to work after it's fixed.

To stop an instance, update the instance row:
```sql
UPDATE ec2.instances SET state = MAP(ARRAY['Name'], ARRAY['stopped']) WHERE instance_id = 'i-04a7cf7ca232cd251';
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

To add a new table that can be read from:

1. Define columns for the new table in the constructor of [AwsMetadata](src/main/java/pl/net/was/cloud/aws/AwsMetadata.java).
2. Define a function to generate rows in [AwsRecordSetProvider.rowGetters](src/main/java/pl/net/was/cloud/aws/AwsRecordSetProvider.java).
3. Profit!

> Note: if the corresponding API method requires predicates that don't map to any field in the response,
> add an extra column, which value will be populated from the predicate in the WHERE clause.

## INSERT support

To add INSERT support for any table, append new entries to the `fields` and `inserters` properties in the
[AwsPageSink](src/main/java/pl/net/was/cloud/aws/AwsPageSink.java) class.
By default, all table columns are optimistically mapped to an AWS SDK request fields.
Some columns might not have matching fields in a request.

> Note: if the corresponding API method requires values for fields that don't map to any column,
> add extra ones, and populate them with nulls in the record set provider.

## UPDATE and DELETE support

1. Add a hidden `row_id` column in `AwsMetadata.columns`. The column should be populated with a unique identifier value in `AwsRecordSetProvider.rowGetters`.
1. Define the primary key in `AwsMetadata.primaryKeys`.
1. Add an entry to the `updaters` and/or `deleters` maps in [AwsMergeSink](src/main/java/pl/net/was/cloud/aws/AwsMergeSink.java).

Updates might perform multiple different API calls, depending on which columns are being set.
