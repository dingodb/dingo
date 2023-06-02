FROM ubuntu:18.04

ENV TZ=Asia/Shanghai \
    DEBIAN_FRONTEND=noninteractive
SHELL ["/bin/bash", "-c"]

RUN apt-get update  \
    && apt-get install -y openjdk-8-jdk vim unzip netcat net-tools tzdata curl \
    && unset http_proxy https_proxy HTTP_PROXY HTTPS_PROXY \
    && ln -fs /usr/share/zoneinfo/${TZ} /etc/localtime \
    && echo ${TZ} > /etc/timezone \
    && dpkg-reconfigure --frontend noninteractive tzdata \
    && rm -rf /var/lib/apt/lists/*

COPY ./dingo.zip /opt

RUN unzip /opt/dingo.zip -d /opt/ && mkdir /opt/dingo/log && mkdir /opt/dingo/executor && chmod +x /opt/dingo/bin/*

ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64/

WORKDIR /opt/dingo

ENTRYPOINT [ "/opt/dingo/bin/start.sh" ]
