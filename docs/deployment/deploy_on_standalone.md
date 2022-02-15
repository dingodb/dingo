# Standalone mode

We want your experience getting started with DingoDB to be both low effort and high reward. Here you'll find a collection of quick start guides that contain starter distributions of the DingoDB platform.

## Running Dingo Using Github docker registry

> Prerequisites
> 1. Install Docker
> 2. Create a personal access token. Steps about creating an account can refer to the following link personal access token [PAT](https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/creating-a-personal-access-token).
> 3. Grant authentication to [github container registry](https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-container-registry). 


```shell
curl -sSL https://raw.githubusercontent.com/dingodb/dingo/main/docker/docker-compose.yml?token=ABUTRTNQUWN3TGXJ5SE3MTLBM7NT6 > docker-compose.yml
docker-compose up -d
docker exec -it `docker ps | grep executor | awk '{print $1}'` /bin/bash
./bin/sqlline.sh
```

## Running Dingo using Docker hub registry


- Download docker-compose file

```shell
curl -sSL https://raw.githubusercontent.com/dingodb/dingo/main/docker/docker-compose.yml?token=ABUTRTNQUWN3TGXJ5SE3MTLBM7NT6 > docker-compose.yml
```

- Change docker image registry

```shell
sed -i 's/ghcr.io\/dingodb\/dingo-ubuntu:main/dingodatabase\/dingodb:0.1.0/g' docker-compose.yml
```

- Start Dingo cluster

```
docker-compose up -d
docker exec -it `docker ps | grep executor | awk '{print $1}'` /bin/bash
./bin/sqlline.sh
```

## Running Dingo in local

> Prerequisites
> 1. Install JDK8 or higher.
> 2. Install Apache zookeeper  or Confluent zookeeper

### 1. Start zookeeper

Step about starting zookeeper is ignore here.

### 2. Build Dingodb from source

1. Build source

```shell
git clone git@github.com:dingodb/dingo.git dingo.git
cd dingo.git
./gradlew build
```
2. Copy `dingo.zip` to target directory

```
  cp dingo-cli/build/distributions/dingo.zip /opt/dingo
```

3. replace zookeeper address in dingo conf directory

```
cd /opt/ && unzip dingo.zip -d dingo && cd dingo && chmod +x bin/* && mkdir log
sed -i 's/zookeeper:2181/You-Zookeeper-Cluster-IP/g' conf/config.yaml
sed -i 's/executor/You-Host-IP/g' conf/config.yaml
./bin/setup.sh cluster
```

### 2. Start Coordinator

```shell
./bin/start-coordinator.sh
```
### 3. Start Executor
```shell
./bin/start-executor.sh
```