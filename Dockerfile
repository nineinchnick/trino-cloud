ARG TRINO_VERSION
FROM nineinchnick/trino-core:$TRINO_VERSION

ARG VERSION

ADD trino-cloud-aws/target/trino-cloud-aws-$VERSION/ /usr/lib/trino/plugin/aws/
ADD catalog/aws.properties /etc/trino/catalog/aws.properties
