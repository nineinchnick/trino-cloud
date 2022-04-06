trino-cloud-aws
=================

This is a Trino connector to access AWS resources using SQL.

# Quick Start

To run a Docker container with this connector, set the appropriate environmental variables, and run the following:
```bash
docker run \
  -d \
  --name trino-cloud \
  -e AWS_ACCESS_KEY_ID \
  -e AWS_SECRET_ACCESS_KEY \
  -e AWS_REGION \
  -p 8080:8080 \
  nineinchnick/trino-cloud:0.15
```

Then use your favourite SQL client to connect to Trino running at http://localhost:8080

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

The following tables support inserting into:
* `ec2.images`
* `ec2.instances`, with limited `UPDATE` and `DELETE` support
* `s3.buckets`

For example:
```sql
INSERT INTO instances (image_id, instance_type, key_name) VALUES ('ami-05f7491af5eef733a', 't2.micro', 'default')
```

To stop an instance, update the instance row:
```sql
UPDATE instances SET state = MAP(ARRAY['Name'], ARRAY['stopped']) WHERE instance_id = 'i-04a7cf7ca232cd251';
```

To terminate it, delete the row:
```sql
DELETE FROM instances WHERE instance_id = 'i-04a7cf7ca232cd251';
```

> Note that the row won't be immediately deleted if the instance is running,
> as it'll be in the `shutting-down` state for a while.

# Configuration

The following configuration options are recognized by this connector:

* `region`

# Build

Run all the unit test classes.
```
mvn test
```

Creates a deployable jar file
```
mvn clean package
```

# Deploy

An example command to run the Trino server with the aws plugin and catalog enabled:

```bash
src=$(git rev-parse --show-toplevel)
docker run \
  -v $src/trino-cloud-aws/target/trino-cloud-aws-0.15-SNAPSHOT:/usr/lib/trino/plugin/aws \
  -e AWS_ACCESS_KEY_ID \
  -e AWS_SECRET_ACCESS_KEY \
  -e AWS_REGION \
  -p 8080:8080 \
  --name trino \
  -d \
  trinodb/trino:375
```

Connect to that server using:
```bash
docker run -it --rm --link trino trinodb/trino:375 trino --server trino:8080 --catalog aws --schema default
```

# Adding new tables

To add a new table that can be read from:

1. Define columns for the new table in the constructor of [AwsMetadata](src/main/java/pl/net/was/cloud/aws/AwsMetadata.java).
2. Define a function to generate rows in [AwsRecordSetProvider.rowGetters](src/main/java/pl/net/was/cloud/aws/AwsRecordSetProvider.java).
3. Profit!

> Note: if the corresponding API method requires predicates that don't map to any field in the response,
> add an extra column, which value will be populated from the predicate in the WHERE clause.

## INSERT support

To add INSERT support for any table, append new entries to the `fields` and `writers` properties in `AwsPageSink` class.
By default, all table columns are optimistically mapped to an AWS SDK request fields.
Some columns might not have matching fields in a request.

> Note: if the corresponding API method requires values for fields that don't map to any column,
> add extra ones, and populate them with nulls in the record set provider.

## UPDATE and DELETE support

1. Add a hidden `row_id` column in `AwsMetadata.columns`. The column should be populated with a unique identifier value in `AwsRecordSetProvider.rowGetters`.
1. Define the primary key in `AwsMetadata.primaryKeys`.
1. Add an entry to the `updaters` and/or `deleters` maps in `AwsPageSourceProvider` maps.

Updates might perform multiple different API calls, depending on which columns are being set.
