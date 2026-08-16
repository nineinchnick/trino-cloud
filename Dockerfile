ARG TRINO_VERSION
FROM trinodb/trino-core:$TRINO_VERSION AS plugin

ARG VERSION

COPY trino-cloud-aws/target/trino-cloud-aws-$VERSION.zip /tmp/trino-cloud-aws.zip
RUN mkdir /tmp/trino-cloud-aws && \
    cd /tmp/trino-cloud-aws && \
    jar --extract --file /tmp/trino-cloud-aws.zip

FROM trinodb/trino-core:$TRINO_VERSION

ARG VERSION

COPY --chown=trino:trino --from=plugin /tmp/trino-cloud-aws/trino-cloud-aws-$VERSION/ /usr/lib/trino/plugin/aws/
COPY --chown=trino:trino catalog/aws.properties /etc/trino/catalog/aws.properties
