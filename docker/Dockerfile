FROM ubuntu:18.04

ENV DEBIAN_FRONTEND=noninteractive
SHELL ["/bin/bash", "-c"]

RUN apt-get update && apt-get install -y openjdk-8-jdk vim unzip netcat net-tools && unset http_proxy https_proxy HTTP_PROXY HTTPS_PROXY

COPY ./dingo.zip /opt

RUN unzip /opt/dingo.zip -d /opt/ && mkdir /opt/dingo/log && mkdir /opt/dingo/coordinator && mkdir /opt/dingo/executor && mkdir /opt/dingo/executor/meta && mkdir /opt/dingo/executor/raftDb && mkdir /opt/dingo/executor/raftLog && chmod +x /opt/dingo/bin/*

ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64/

WORKDIR /opt/dingo

ENTRYPOINT [ "/opt/dingo/bin/start.sh" ]
