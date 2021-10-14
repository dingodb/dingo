FROM ubuntu:18.04

ENV DEBIAN_FRONTEND=noninteractive
SHELL ["/bin/bash", "-c"]

RUN apt-get update && apt-get install -y openjdk-8-jdk vim unzip

COPY ./dingo-cli/build/distributions/dingo.zip /opt

RUN unzip /opt/dingo.zip -d /opt/dingo && mkdir /opt/dingo/log && mkdir /opt/dingo/data && chmod +x /opt/dingo/bin/*

ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64/

WORKDIR /opt/dingo

ENTRYPOINT [ "/opt/dingo/bin/start.sh" ]
