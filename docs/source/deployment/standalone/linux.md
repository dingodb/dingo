# Single Node Rapid Deployment of Docker Containers

First of all, after successfully deploying and installing Docker in a Linux environment, in order to perform a single-node rapid deployment of Docker containers, you need to follow the steps below to perform the deployment process:
## 1. Prerequisites
 Ensure that your Linux system has successfully installed the Docker container engine and that the Docker service is running normally.

## 2. Environment preparation
### Environment description
Configure the git docker docker-compose environment in advance;
```shell
# Note: Check your docker-compose version against the following relationships, and modify the docker-compose.yml version field of the configuration file downloaded in 2.1 as appropriate
 docker-compose.yml version| Docker Engine version| Docker Compose version
  2.4；                       17.12.0+;              1.21.0+
  2.3；                       17.06.0+;              1.16.0+
  2.2；                       1.13.0+;               1.13.0+
  2.1；                       1.12.0+;               1.9.0+
  2.0；                       1.10.0+ ;              1.6.0+
```
### Pull-out Container
```shell
# Pull dingo-store
docker pull dingodatabase/dingo-store:latest
# Pull dingo
docker pull dingodatabase/dingo:latest
```
## 3. Starting containers
Here we need to start containers based on docker-compose, so let's first go pull: docker-compose.
### Pull the corresponding docker-compose configuration

```shell
# Method 1 
curl https://github.com/dingodb/dingo-deploy/blob/main/container/docker-compose/docker-compose.single.yml -o docker-compose.yml
# Method 2
git clone https://github.com/dingodb/dingo-deploy.git
cd  dingo-deploy/container/docker-compose
```
**Configuration file description:**
* docker-compose.yml for single-node multi-service, where the full startup contains 3 coordinator, 3 store, 3 index, 1 executor, 1 web (proxy);
* docker-compose.single.yml is a single node, single service, where the full startup contains 1 coordinator, 1 store, 1 index, 1 executor, 1 web (proxy); * coordinator, store, index use host network, executor, web (proxy) use net network.
* The coordinator, store, and index use the host network, and the executor and web(proxy) use the net network.
### Start the corresponding container
```shell
# Change to your own ip address, do not use 127.0.0.1 (web(proxy) and executor use net network, 127.0.0.1 does not point to host).
# docker-compose.yml is in the current directory and has the same name, you can not use the -f parameter.
DINGO_HOST_IP=x.x.x.x docker-compose -f ./docker-compose.yml up -d
```
## 4. Viewing cluster status
### Check container status
Checks if all containers in the yml configuration file are in the Up state.
```shell
docker ps
```
### View process status
- View coordinator status
```Shell
# Enter the container
docker exec -it coordinator1 bash
cd build/bin/

[root@coordinator1 bin]#  ./dingodb_client --method=GetCoordinatorMap
```

- Check the Store status
```shell
# Enter the container
docker exec -it coordinator1 bash
cd build/bin/

[root@coordinator1 bin]# ./dingodb_client --method=GetStoreMap
```

* Checking the status of the Executor. To check the status of executor, just check the log and make sure mysql init is successful.
```shell
docker logs executor
```

* Check the status of the Proxy. Web(Proxy) can be viewed through swagger, you need to replace the IP in the URL with your own IP address.

```
http://localhost:13000/swagger-ui/index.html
```