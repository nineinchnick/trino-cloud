FROM trinodb/trino:361

ARG VERSION

RUN rm -rf /usr/lib/trino/plugin/{accumulo,bigquery,cassandra,druid,example-http,google-sheets,iceberg,kafka,kudu,ml,mysql,password-authenticators,phoenix5,postgresql,raptor-legacy,redshift,session-property-managers,teradata-functions,tpcds,atop,blackhole,clickhouse,elasticsearch,geospatial,hive-hadoop2,kinesis,local-file,memsql,mongodb,oracle,phoenix,pinot,prometheus,redis,resource-group-managers,sqlserver,thrift,tpch}

ADD trino-cloud-aws/target/trino-cloud-aws-$VERSION/ /usr/lib/trino/plugin/aws/
