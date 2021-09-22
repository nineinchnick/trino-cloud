trino-cloud-aws
=================

This is a Trino connector to access AWS resources using SQL.

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
* `ec2.instances`
* `s3.buckets`

For example:
```sql
INSERT INTO instances (image_id, instance_type, key_name) VALUES ('ami-05f7491af5eef733a', 't2.micro', 'default')
```

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
  -v $src/trino-cloud-aws/target/trino-cloud-aws-0.1-SNAPSHOT:/usr/lib/trino/plugin/aws \
  -v $src/catalog:/etc/trino/catalog \
  -e AWS_ACCESS_KEY_ID \
  -e AWS_SECRET_ACCESS_KEY \
  -e AWS_REGION \
  -p 8080:8080 \
  --name trino \
  -d \
  trinodb/trino:361
```

Connect to that server using:
```bash
docker run -it --rm --link trino trinodb/trino:361 trino --server trino:8080 --catalog aws --schema default
```

# Adding new tables

To add a new table:

1. Define columns for the new table in the constructor of [AwsMetadata](src/main/java/pl/net/was/cloud/aws/AwsMetadata.java).
1. Define a function to generate rows in [AwsRecordSetProvider.rowGetters](src/main/java/pl/net/was/cloud/aws/AwsRecordSetProvider.java).
1. Profit!
