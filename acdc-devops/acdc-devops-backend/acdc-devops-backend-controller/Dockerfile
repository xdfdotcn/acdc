# 基础镜像使用java
FROM openjdk:8u332-jdk

# 将jar包添加到容器中并更名为app.jar
ADD target/acdc-devops-backend-controller-1.0.0-Alpha-SNAPSHOT.jar app.jar

# JVM参数
ENV JAVA_OPTS="${JAVA_OPTS} -server -Xmx4096m -Xms4096m"
ENV JAVA_OPTS="${JAVA_OPTS} -XX:+HeapDumpOnOutOfMemoryError -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintHeapAtGC"
ENV JAVA_OPTS="${JAVA_OPTS} -XX:HeapDumpPath=/logs -Xloggc:/logs/gc.log"

# 运行jar包
RUN bash -c 'touch /app.jar'
ENTRYPOINT exec java ${JAVA_OPTS} -jar /app.jar
