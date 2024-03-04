# Deploy In Single Node Using Docker

For the convenience of developers, the deployment of DingoDB can also choose docker-compse mode.


## Deploy Architecture View

There are many types of roles in the cluster mode of DingoDB, such as coordinator, executor, dingo-store, and the cluster mode requires at least three machines. The architecture view can refer [dingo-deploy-architecture](https://dingodb.readthedocs.io/en/latest/deployment/deploy_on_cluster_by_ansible.html).

## Directory Structure Planning

| Directory Types                                                                | Path                     |
|--------------------------------------------------------------------------------|:-------------------------|
| Program installation path                                                      | /opt                     |
| dingo-store script path                                                        | /opt/dingo-store/scripts |
| dingo-store deployment path <br/>(including separate data and log directories) | /opt/dingo-store/dist    |
| executor's script path                                                         | /opt/dingo/bin           |
| executor's log path                                                            | /opt/dingo/log           |

## User group planning

| Deployment Method | User name | Gorup name |
|-------------------|-----------|------------|
| docker deploy     | root      | root       |

## Component list and port usage
| Component | Version | Port                                                                                             | Remarks                                         |
|-----------|---------|--------------------------------------------------------------------------------------------------|-------------------------------------------------|
| DingoDB   | 0.7.0   | raft:22101-22103/20101-20103     <br/>server:22001-22003/20001-20003     <br/>executor:3307/8765 | Real-time analytics database                    |

## Installation Notes

### 1.Environment Setup
#### (1) Environment Description
Currently, docker-compose startup is only supported on Linux environments. Windows and macOS do not support the host network mode of Docker.

You need to configure the git, Docker, and docker-compose environments in advance.
```text
 # Note: Check your docker-compose version and correspondingly modify the 'version' field in the downloaded configuration file 'docker-compose.yml' as follows:
  docker-compose.yml version| Docker Engine version| Docker Compose version
  2.4；                       17.12.0+;              1.21.0+
  2.3；                       17.06.0+;              1.16.0+
  2.2；                       1.13.0+;               1.13.0+
  2.1；                       1.12.0+;               1.9.0+
  2.0；                       1.10.0+ ;              1.6.0+
```

### 1.2 Pull container

```shell
docker pull dingodatabase/dingo-store:latest
docker pull dingodatabase/dingo:latest
```
### 2. Starting a container based on Docker Compose

#### (1) Pull the corresponding Docker Compose configuration

```shell
# Method one
curl https://raw.githubusercontent.com/dingodb/dingo/develop/docker/docker-compose.yml -o docker-compose.yml
 
# Method two
git clone https://github.com/dingodb/dingo.git
cd dingo/docker
```
#### (2) Configuration file description
The docker-compose file includes 3 coordinators, 3 stores, 3 indexes, 1 executor, and 1 proxy. You can modify the file to start only a subset of these nodes or add more nodes based on your actual needs.

The coordinators, stores, and indexes use the host network mode, while the executor and proxy use the bridge network mode.

#### (3) Start the corresponding containers
```shell
#  Replace with your own IP address (do not use 127.0.0.1 as proxy and executor use the bridge network and 127.0.0.1 does not point to the host)
# If the docker-compose.yml file is in the current directory and has the same name, you do not need to use the -f parameter.
DINGO_HOST_IP=x.x.x.x docker-compose -f ./docker-compose.yml up -d
 
# Stop and remove the containers.
docker-compose -f ./docker-compose.yml down
```

### 3. View the status of cluster

#### (1) Check the status of the containers
To check if all 11 containers specified in the docker-compose.yml configuration file are in an "Up" state.
```shell
[root@dingo221 ~]# docker ps
CONTAINER ID   IMAGE                                      COMMAND                  CREATED        STATUS        PORTS                                                                                      NAMES
0bde306f4c2e   dingodatabase/dingo-store:latest           "/opt/dingo-store/sc…"   45 hours ago   Up 45 hours                                                                                              store3
fa0d18e2d0fe   dingodatabase/dingo-store:latest           "/opt/dingo-store/sc…"   45 hours ago   Up 45 hours                                                                                              store2
45ce67bc688a   dingodatabase/dingo-store:latest           "/opt/dingo-store/sc…"   45 hours ago   Up 45 hours                                                                                              index3
3631bfea894c   dingodatabase/dingo-store:latest           "/opt/dingo-store/sc…"   45 hours ago   Up 45 hours                                                                                              index2
92cf6df0e87d   dingodatabase/dingo-store:latest           "/opt/dingo-store/sc…"   45 hours ago   Up 45 hours                                                                                              store1
9775c834ee2b   dingodatabase/dingo-store:latest           "/opt/dingo-store/sc…"   45 hours ago   Up 45 hours                                                                                              index1
3439f679c211   dingodatabase/dingo-store:latest           "/opt/dingo-store/sc…"   45 hours ago   Up 45 hours                                                                                              coordinator1
c927d17f848f   dingodatabase/dingo:latest                 "/opt/dingo/bin/star…"   45 hours ago   Up 45 hours   0.0.0.0:9999->9999/tcp, :::9999->9999/tcp, 0.0.0.0:13000->13000/tcp, :::13000->13000/tcp   proxy
72381a6822ef   dingodatabase/dingo-store:latest           "/opt/dingo-store/sc…"   45 hours ago   Up 45 hours                                                                                              coordinator2
1581e4b35d14   dingodatabase/dingo:latest                 "/opt/dingo/bin/star…"   45 hours ago   Up 45 hours   0.0.0.0:3307->3307/tcp, :::3307->3307/tcp, 0.0.0.0:8765->8765/tcp, :::8765->8765/tcp       executor
18f8f13b7fab   dingodatabase/dingo-store:latest           "/opt/dingo-store/sc…"   45 hours ago   Up 45 hours                                                                                              coordinator3
```
#### (2) View the state of dingo-store(coordinator/store/index)
In the new version, all the dingodb_client_coordinator instances have been merged into dingodb_client.
```shell
# To enter a container
docker exec -it coordinator1 bash
cd build/bin/
 
# View the state of coordinator
[root@coordinator1 bin]#  ./dingodb_client --method=GetCoordinatorMap
E20230928 11:06:15.231629   186 dingodb_client.cc:804] [main] coordinator url is empty, try to use file://./coor_list
I20230928 11:06:15.232175   186 client_interation.cc:42] [Init] Init channel 172.30.14.221:22001
I20230928 11:06:15.244812   186 client_interation.cc:42] [Init] Init channel 172.30.14.221:22002
I20230928 11:06:15.244900   186 client_interation.cc:42] [Init] Init channel 172.30.14.221:22003
I20230928 11:06:15.245754   197 naming_service_thread.cpp:203] brpc::policy::FileNamingService("./coor_list"): added 3
I20230928 11:06:15.246028   186 coordinator_interaction.cc:65] [InitByNameService] Init channel by service_name file://./coor_list service_type=0
I20230928 11:06:15.246181   186 coordinator_interaction.cc:65] [InitByNameService] Init channel by service_name file://./coor_list service_type=1
I20230928 11:06:15.246259   186 coordinator_interaction.cc:65] [InitByNameService] Init channel by service_name file://./coor_list service_type=3
I20230928 11:06:15.256951   186 coordinator_interaction.h:217] [SendRequestByService] name_service_channel_ connect with meta server finished. response errcode: 0, leader_addr: 172.30.14.221:22003
I20230928 11:06:15.257054   186 coordinator_client_function_coor.cc:514] [SendGetCoordinatorMap] SendRequest status=OK
I20230928 11:06:15.257094   186 coordinator_client_function_coor.cc:515] [SendGetCoordinatorMap] leader_location {
  host: "172.30.14.221"
  port: 22002
}
auto_increment_leader_location {
  host: "172.30.14.221"
  port: 22002
}
 
 
#  View the state of store
[root@coordinator1 bin]# ./dingodb_client --method=GetStoreMap
E20230928 11:07:00.918895   198 dingodb_client.cc:804] [main] coordinator url is empty, try to use file://./coor_list
I20230928 11:07:00.919196   198 client_interation.cc:42] [Init] Init channel 172.30.14.221:22001
I20230928 11:07:00.930696   198 client_interation.cc:42] [Init] Init channel 172.30.14.221:22002
I20230928 11:07:00.930780   198 client_interation.cc:42] [Init] Init channel 172.30.14.221:22003
I20230928 11:07:00.931160   206 naming_service_thread.cpp:203] brpc::policy::FileNamingService("./coor_list"): added 3
I20230928 11:07:00.931306   198 coordinator_interaction.cc:65] [InitByNameService] Init channel by service_name file://./coor_list service_type=0
I20230928 11:07:00.931385   198 coordinator_interaction.cc:65] [InitByNameService] Init channel by service_name file://./coor_list service_type=1
I20230928 11:07:00.931493   198 coordinator_interaction.cc:65] [InitByNameService] Init channel by service_name file://./coor_list service_type=3
I20230928 11:07:00.937242   198 coordinator_interaction.h:217] [SendRequestByService] name_service_channel_ connect with meta server finished. response errcode: 0, leader_addr: 172.30.14.221:22002
I20230928 11:07:00.937340   198 coordinator_client_function_coor.cc:463] [SendGetStoreMap] SendRequest status=OK
I20230928 11:07:00.937357   198 coordinator_client_function_coor.cc:465] [SendGetStoreMap] epoch: 1006
storemap {
  epoch: 1006
  stores {
    id: 1101
    state: STORE_NORMAL
    server_location {
      host: "172.30.14.221"
      port: 21001
    }
    raft_location {
      host: "172.30.14.221"
      port: 21101
    }
    keyring: "TO_BE_CONTINUED"
    create_timestamp: 1695707860131
    last_seen_timestamp: 1695870413206
    store_type: NODE_TYPE_INDEX
  }
  stores {
    id: 1102
    state: STORE_NORMAL
    server_location {
      host: "172.30.14.221"
      port: 21002
    }
    raft_location {
      host: "172.30.14.221"
      port: 21102
    }
    keyring: "TO_BE_CONTINUED"
    create_timestamp: 1695707860201
    last_seen_timestamp: 1695870413492
    store_type: NODE_TYPE_INDEX
  }
  stores {
    id: 1103
    state: STORE_NORMAL
    server_location {
      host: "172.30.14.221"
      port: 21003
    }
    raft_location {
      host: "172.30.14.221"
      port: 21103
    }
    keyring: "TO_BE_CONTINUED"
    create_timestamp: 1695707860173
    last_seen_timestamp: 1695870413492
    store_type: NODE_TYPE_INDEX
  }
  stores {
    id: 1001
    state: STORE_NORMAL
    server_location {
      host: "172.30.14.221"
      port: 20001
    }
    raft_location {
      host: "172.30.14.221"
      port: 20101
    }
    keyring: "TO_BE_CONTINUED"
    create_timestamp: 1695707860182
    last_seen_timestamp: 1695870413491
  }
  stores {
    id: 1002
    state: STORE_NORMAL
    server_location {
      host: "172.30.14.221"
      port: 20002
    }
    raft_location {
      host: "172.30.14.221"
      port: 20102
    }
    keyring: "TO_BE_CONTINUED"
    create_timestamp: 1695707860184
    last_seen_timestamp: 1695870413490
  }
  stores {
    id: 1003
    state: STORE_NORMAL
    server_location {
      host: "172.30.14.221"
      port: 20003
    }
    raft_location {
      host: "172.30.14.221"
      port: 20103
    }
    keyring: "TO_BE_CONTINUED"
    create_timestamp: 1695707870144
    last_seen_timestamp: 1695870413205
  }
}
store_id=1101 type=NODE_TYPE_INDEX addr={host: "172.30.14.221" port: 21001} state=STORE_NORMAL in_state=STORE_IN create_timestamp=1695707860131 last_seen_timestamp=1695870413206
store_id=1102 type=NODE_TYPE_INDEX addr={host: "172.30.14.221" port: 21002} state=STORE_NORMAL in_state=STORE_IN create_timestamp=1695707860201 last_seen_timestamp=1695870413492
store_id=1103 type=NODE_TYPE_INDEX addr={host: "172.30.14.221" port: 21003} state=STORE_NORMAL in_state=STORE_IN create_timestamp=1695707860173 last_seen_timestamp=1695870413492
store_id=1001 type=NODE_TYPE_STORE addr={host: "172.30.14.221" port: 20001} state=STORE_NORMAL in_state=STORE_IN create_timestamp=1695707860182 last_seen_timestamp=1695870413491
store_id=1002 type=NODE_TYPE_STORE addr={host: "172.30.14.221" port: 20002} state=STORE_NORMAL in_state=STORE_IN create_timestamp=1695707860184 last_seen_timestamp=1695870413490
store_id=1003 type=NODE_TYPE_STORE addr={host: "172.30.14.221" port: 20003} state=STORE_NORMAL in_state=STORE_IN create_timestamp=1695707870144 last_seen_timestamp=1695870413205
DINGODB_HAVE_STORE_AVAILABLE, store_count=3
DINGODB_HAVE_INDEX_AVAILABLE, index_count=3
 
 
# Other opeartor
./dingodb_client --method=CreateTable --name=test1  # Create table
./dingodb_client --method=GetTable --id=60067     # View the table
```
#### (3) View the state of dingo(executor/proxy)
```shell
# To view the executor and ensure that the MySQL init process was successful.
[root@dingo221 ~]# docker logs executor
DINGODB_STORE_NORMAL = 0, wait 1 second
DINGO_MYSQL_COORDINATORS: 172.30.14.221:22001
ERRCODE 0
DINGODB_STORE_NORMAL: 0 DINGODB_SCHEMA_MYSQL 0
DINGODB_STORE_NORMAL = 0, wait 1 second
DINGO_MYSQL_COORDINATORS: 172.30.14.221:22001
ERRCODE 1
LEADER_HOST:  LEADER_PORT:
DINGODB_STORE_NORMAL = 0, wait 1 second
DINGO_MYSQL_COORDINATORS: 172.30.14.221:22001
ERRCODE 1
LEADER_HOST: 172.30.14.221 LEADER_PORT: 22002
DINGODB_STORE_NORMAL = 0, wait 1 second
DINGO_MYSQL_COORDINATORS: 172.30.14.221:22001
ERRCODE 1
LEADER_HOST: 172.30.14.221 LEADER_PORT: 22002
DINGODB_STORE_NORMAL = 0, wait 1 second
DINGO_MYSQL_COORDINATORS: 172.30.14.221:22001
ERRCODE 1
LEADER_HOST: 172.30.14.221 LEADER_PORT: 22002
DINGODB_STORE_NORMAL = 0, wait 1 second
DINGO_MYSQL_COORDINATORS: 172.30.14.221:22001
ERRCODE 1
LEADER_HOST: 172.30.14.221 LEADER_PORT: 22002
DINGODB_STORE_NORMAL = 0, wait 1 second
DINGO_MYSQL_COORDINATORS: 172.30.14.221:22001
ERRCODE 1
LEADER_HOST: 172.30.14.221 LEADER_PORT: 22002
DINGODB_STORE_NORMAL = 0, wait 1 second
DINGO_MYSQL_COORDINATORS: 172.30.14.221:22001
ERRCODE 1
LEADER_HOST: 172.30.14.221 LEADER_PORT: 22002
DINGODB_STORE_NORMAL = 0, wait 1 second
DINGO_MYSQL_COORDINATORS: 172.30.14.221:22001
ERRCODE 1
LEADER_HOST: 172.30.14.221 LEADER_PORT: 22002
DINGODB_STORE_NORMAL = 0, wait 1 second
DINGO_MYSQL_COORDINATORS: 172.30.14.221:22001
ERRCODE 1
LEADER_HOST: 172.30.14.221 LEADER_PORT: 22002
DINGODB_STORE_NORMAL = 0, wait 1 second
DINGO_MYSQL_COORDINATORS: 172.30.14.221:22001
ERRCODE 1
LEADER_HOST: 172.30.14.221 LEADER_PORT: 22002
DINGODB_STORE_NORMAL >= 3, start to initialize MySQL
init meta store success
[main] WARN io.dingodb.sdk.service.connector.ServiceConnector - Exec class io.dingodb.sdk.service.meta.MetaServiceClient$$Lambda$133/1293226111 failed, code: [EREGION_UNAVAILABLE], message: Not enough stores for create region, will retry...
[main] WARN io.dingodb.sdk.service.connector.ServiceConnector - Exec class io.dingodb.sdk.service.meta.MetaServiceClient$$Lambda$133/1293226111 failed, code: [EREGION_UNAVAILABLE], message: Not enough stores for create region, will retry...
[main] WARN io.dingodb.sdk.service.connector.ServiceConnector - Exec class io.dingodb.sdk.service.meta.MetaServiceClient$$Lambda$133/1293226111 failed, code: [EREGION_UNAVAILABLE], message: Not enough stores for create region, will retry...
[main] WARN io.dingodb.sdk.service.connector.ServiceConnector - Exec class io.dingodb.sdk.service.meta.MetaServiceClient$$Lambda$133/1293226111 failed, code: [EREGION_UNAVAILABLE], message: Not enough stores for create region, will retry...
init user success
init db privilege success
init table privilege success
init global variables success
init information_schema.COLUMNS success
init information_schema.PARTITIONS success
init information_schema.EVENTS success
init information_schema.TRIGGERS success
init information_schema.STATISTICS success
init information_schema.ROUTINES success
init information_schema.KEY_COLUMN_USAGE success
init information_schema.SCHEMATA success
init information_schema.TABLES success
init mysql.ANALYZE_TASK success
init mysql.CM_SKETCH success
init mysql.TABLE_STATS success
init mysql.TABLE_BUCKETS success
Java mysql init succes
 
 
#  The address to access the proxy swagger,replace <your_ip_address> with your actual IP address.
http://<your_ip_address>:13000/swagger-ui/index.html
```
