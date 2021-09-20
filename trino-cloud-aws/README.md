trino-cloud-aws
=================

This is a Trino connector to access AWS resources using SQL.

Not all resources are mapped yet, here's a list of the available tables:
* `ec2_instances`
* `s3_buckets`

# Configuration

The following configuration options are recognized by this connector:

* `access_key_id` - required
* `secret_access_key` - required
* `default_region` - optional, if not set, queries without a predicate for the `region` column will fail

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
  -p 8080:8080 \
  --name trino \
  -d \
  trinodb/trino:361
```

Connect to that server using:
```bash
docker run -it --rm --link trino trinodb/trino:361 trino --server trino:8080 --catalog aws --schema default
```
