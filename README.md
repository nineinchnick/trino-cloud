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
* `AWS_SESSION_TOKEN`, only for temporary credentials, like SSO or an assumed role
* `AWS_REGION`

Replace `<version>` with the [latest release](https://github.com/nineinchnick/trino-cloud/releases), without the `v` prefix:

```bash
docker run \
  -e AWS_ACCESS_KEY_ID \
  -e AWS_SECRET_ACCESS_KEY \
  -e AWS_SESSION_TOKEN \
  -e AWS_REGION \
  -p 8080:8080 \
  --name trino-cloud-aws \
  nineinchnick/trino-cloud:<version>
```

The image already includes the `aws` catalog, configured by [catalog/aws.properties](catalog/aws.properties).
See [Credentials](trino-cloud-aws/README.md#credentials) for other ways to pass credentials, like AWS CLI profiles.

Then use your favourite SQL client to connect to Trino running at http://localhost:8080

# Usage

Download the ZIP package from the [latest release](https://github.com/nineinchnick/trino-cloud/releases),
unzip it and copy the `trino-cloud-aws-<version>` directory to the plugin directory on every node in your Trino cluster.
Each release is built for one Trino version, and the plugin might not load in other versions.
Check the `dep.trino.version` property in the [pom.xml](pom.xml) of the release tag.
Create a `aws.properties` file in your Trino catalog directory:

```
connector.name=aws
aws.region=${ENV:AWS_REGION}
```

By default, the connector reads credentials using the AWS SDK default credentials provider chain.
Each catalog can set its own credentials, so you can use multiple AWS accounts at the same time.
See the [connector configuration](trino-cloud-aws/README.md#configuration) for all properties.

After restarting Trino, you should be able to connect to the `aws` catalog.

# Development

For more information, see the README files in connector directories:
* [trino-cloud-aws](trino-cloud-aws/README.md)
