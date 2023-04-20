ARG BASE_IMAGE

FROM $BASE_IMAGE

ENV JAVA_TOOL_OPTIONS '-Dfile.encoding=UTF8'

# jvm exporter
RUN mkdir /kafka/etc
COPY src/main/resources/monitoring/jmx_prometheus_javaagent-0.17.0.jar /kafka/etc/jmx_prometheus_javaagent.jar
COPY target/classes/monitoring/config.yml /kafka/etc/config.yml

# logging
COPY target/classes/logging/log4j.properties /kafka/config/log4j.properties

# connector
COPY target/connector-package/acdc-connect-connector-sink-jdbc /kafka/connect/acdc-connect-connector-sink-jdbc
COPY target/connector-package/acdc-connect-connector-source-tidb /kafka/connect/acdc-connect-connector-source-tidb
COPY target/connector-package/acdc-connect-connector-sink-hdfs /kafka/connect/acdc-connect-connector-sink-hdfs
COPY target/connector-package/acdc-connect-connector-sink-kafka /kafka/connect/acdc-connect-connector-sink-kafka

# smt
COPY target/connector-package/acdc-connect-plugins-converter-schemaless-json /kafka/connect/acdc-connect-plugins-converter-schemaless-json
COPY target/connector-package/acdc-connect-transforms-format /kafka/connect/acdc-connect-transforms-format
COPY target/connector-package/acdc-connect-transforms-value-mapper /kafka/connect/acdc-connect-transforms-value-mapper
COPY target/connector-package/acdc-internal-connect-plugins-converter-xdf /kafka/connect/acdc-internal-connect-plugins-converter-xdf
