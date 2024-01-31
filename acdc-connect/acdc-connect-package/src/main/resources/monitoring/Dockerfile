FROM debezium/connect:1.9.5.Final

RUN mkdir /kafka/etc

COPY jmx_prometheus_javaagent-0.17.0.jar /kafka/etc/jmx_prometheus_javaagent.jar
COPY config.yml /kafka/etc/config.yml