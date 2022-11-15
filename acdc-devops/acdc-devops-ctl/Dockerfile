FROM openjdk:8u332-jdk

ADD target/acdc-devops-ctl-1.0.0-Alpha-SNAPSHOT.jar /acdc-ctl/lib/acdc-ctl.jar
ADD target/classes/*.sh /acdc-ctl/

RUN echo y| apt-get update
RUN echo y| apt-get install telnet
RUN echo y| apt-get install net-tools

RUN chmod 777 -R /acdc-ctl

WORKDIR /acdc-ctl