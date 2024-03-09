trino-cloud
===========

[Trino](http://trino.io/) connectors for managing cloud resources, like AWS EC2 instances or S3 buckets.
Please keep in mind that this is not production ready and it was created for tests.

# Quick Start

To run a Docker container with one of the connectors, make sure to have the following
environmental variables set, same as for
[CLI access](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-envvars.html):
* `AWS_ACCESS_KEY_ID`
* `AWS_SECRET_ACCESS_KEY`
* `AWS_REGION`

```bash
docker run \
  --tmpfs /etc/trino/catalog \
  -v $(pwd)/catalog/aws.properties:/etc/trino/catalog/aws.properties \
  -e AWS_ACCESS_KEY_ID \
  -e AWS_SECRET_ACCESS_KEY \
  -e AWS_REGION \
  -p 8080:8080 \
  --name trino-cloud-aws \
  nineinchnick/trino-cloud:0.52
```

Then use your favourite SQL client to connect to Trino running at http://localhost:8080

# Usage

Download one of the ZIP packages, unzip it and copy the `trino-cloud-aws-0.52` directory to the plugin directory on every node in your Trino cluster.
Create a `aws.properties` file in your Trino catalog directory and configure the credentials for a specific AWS account.
To manage multiple accounts at the same time, create separate config files for multiple catalogs.

```
connector.name=aws
region=${ENV:AWS_REGION}
```

After reloading Trino, you should be able to connect to the `aws` catalog.

# Development

For more information, see the README files in connector directories:
* [trino-cloud-aws](trino-cloud-aws/README.md)
