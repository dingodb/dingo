# Deploy In Single Node Using Docker

For the convenience of developers, the deployment of DingoDB can also choose docker-compse mode.


## Deploy Architecture View

There are many types of roles in the cluster mode of DingoDB, such as coordinator, executor, driver-proxy, and the cluster mode requires at least three machines. The architecture view can refer [dingo-deploy-architecture](https://dingodb.readthedocs.io/en/latest/deployment/deploy_on_cluster.html).

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
| DingoDB   | 0.6.0   | raft:22101-22103/20101-20103     <br/>server:22001-22003/20001-20003     <br/>executor:3307/8765 | Real-time analytics database                    |

## Installation Notes

### 1. Pull container

```shell
docker pull dingodatabase/dingo-store:latest
```
### 2. Starting a container based on Docker Compose

#### (1) Pull the corresponding Docker Compose configuration

```shell
# Mode one
curl https://raw.githubusercontent.com/dingodb/dingo-store/main/docker/docker-compose.yml -o docker-compose.yml
 
# Mode two
git clone https://github.com/dingodb/dingo-store.git
cd dingo-store/docker
```
#### (2) Deploy dingo-store and executor
 * Pull the corresponding docker-compose configuration.
```shell
# Mode One
curl https://raw.githubusercontent.com/dingodb/dingo-deploy/main/container/docker-compose/docker-compose.yml -o docker-compose.yml
 
# Mode Two
git clone https://github.com/dingodb/dingo-deploy.git
cd  dingo-deploy/container/docker-compose
```

#### (3) Start the corresponding container.

```shell
#  Change to your IP address.
DINGO_HOST_IP=127.0.0.1 docker-compose -f ./docker-compose.yml up -d
 
#Note: Check your docker-compose version, corresponding to the following relationship, modify docker-compose.yml version appropriately
  docker-compose.yml version| Docker Engine version| Docker Compose version
  2.4；                       17.12.0+;              1.21.0+
  2.3；                       17.06.0+;              1.16.0+
  2.2；                       1.13.0+;               1.13.0+
  2.1；                       1.12.0+;               1.9.0+
  2.0；                       1.10.0+ ;              1.6.0+
```
### 3. View the status of cluster

```shell
# View the status of container
docker ps
"""
CONTAINER ID   IMAGE                              COMMAND                  CREATED         STATUS         PORTS                                       NAMES
800b7b04411f   dingodatabase/dingo-store:latest   "/opt/dingo-store/sc…"   5 seconds ago   Up 4 seconds                                               store3
b2beb3c01af6   dingodatabase/dingo-store:latest   "/opt/dingo-store/sc…"   5 seconds ago   Up 4 seconds                                               store1
4e88fdfdacb5   dingodatabase/dingo-store:latest   "/opt/dingo-store/sc…"   5 seconds ago   Up 4 seconds                                               store2
f1e6b8249fb1   dingodatabase/dingo-store:latest   "/opt/dingo-store/sc…"   5 seconds ago   Up 5 seconds                                               coordinator2
fd5a4bb985ce   dingodatabase/dingo-store:latest   "/opt/dingo-store/sc…"   5 seconds ago   Up 5 seconds                                               coordinator3
70e29cbfed78   dingodatabase/dingo-store:latest   "/opt/dingo-store/sc…"   5 seconds ago   Up 5 seconds                                               coordinator1
"""
 
# Enter the container
docker exec -it coordinator1 bash
 
# View the coordinator status （ Replace the IP address of your host machine with this one）.
cd build/bin
./dingodb_client_coordinator --method=GetCoordinatorMap --req_num=1 --coordinator-addr=172.26.0.2:22001  
I0406 10:01:54.634954   178 coordinator_client_function_coor.cc:72] leader_location: 172.26.0.2:22001
I0406 10:01:54.635211   171 coordinator_client_function_coor.cc:213] Received response get_coordinator_map=0 request_attachment=0 response_attachment=0 latency=189
I0406 10:01:54.635224   171 coordinator_client_function_coor.cc:217] leader_location 
{
  host: "172.26.0.2"
  port: 22001
}
coordinator_locations {
  host: "172.26.0.2"
  port: 22001
}
coordinator_locations {
  host: "172.26.0.3"
  port: 22001
}
coordinator_locations {
  host: "172.26.0.4"
  port: 22001
}
"""
 
# View the status of store
./dingodb_client_coordinator --method=GetStoreMap --req_num=1 --coordinator-addr=172.26.0.2:22001
"""
WARNING: Logging before InitGoogleLogging() is written to STDERR
I0406 10:02:00.826020   185 coordinator_client_function_coor.cc:72] leader_location: 172.26.0.2:22001
I0406 10:02:00.826290   184 coordinator_client_function_coor.cc:171] Received response get_store_map=1 request_attachment=0 response_attachment=0 latency=200
I0406 10:02:00.826304   184 coordinator_client_function_coor.cc:175] epoch: 1003
storemap {
  epoch: 1003
  stores {
    id: 1001
    server_location {
      host: "172.26.0.7"
      port: 20001
    }
    raft_location {
      host: "172.26.0.7"
      port: 20101
    }
    keyring: "TO_BE_CONTINUED"
  }
  stores {
    id: 1002
    server_location {
      host: "172.26.0.6"
      port: 20001
    }
    raft_location {
      host: "172.26.0.6"
      port: 20101
    }
    keyring: "TO_BE_CONTINUED"
  }
  stores {
    id: 1003
    server_location {
      host: "172.26.0.5"
      port: 20001
    }
    raft_location {
      host: "172.26.0.5"
      port: 20101
    }
    keyring: "TO_BE_CONTINUED"
  }
}
"""
 
# Cseate table
./dingodb_client_coordinator --method=CreateTable --req_num=1 --coordinator-addr=172.26.0.2:22001 --id=1001

"""
entity_type: ENTITY_TYPE_TABLE
  parent_entity_id: 2
  entity_id: 1001
}
"""
 
# View table
./dingodb_client_coordinator --method=GetTable --req_num=1 --coordinator-addr=172.26.0.2:22001 --id=1001

"""
WARNING: Logging before InitGoogleLogging() is written to STDERR
I0406 10:17:14.174944   212 coordinator_client_function_coor.cc:72] leader_location: 172.26.0.2:22001
I0406 10:17:14.176126   210 coordinator_client_function_meta.cc:149] Received response table_id=1001 request_attachment=0 response_attachment=0 latency=229
I0406 10:17:14.176141   210 coordinator_client_function_meta.cc:153] table_definition_with_id {
  table_id {
    entity_type: ENTITY_TYPE_TABLE
    parent_entity_id: 2
    entity_id: 1001
  }
  table_definition {
    name: "t_test1"
    columns {
      name: "test_columen_0"
      sql_type: SQL_TYPE_INTEGER
      element_type: ELEM_TYPE_BYTES
      precision: 100
      indexOfKey: 7
      default_val: "0"
    }
    columns {
      name: "test_columen_1"
      sql_type: SQL_TYPE_INTEGER
      element_type: ELEM_TYPE_BYTES
      precision: 100
      indexOfKey: 7
      default_val: "0"
    }
    columns {
      name: "test_columen_2"
      sql_type: SQL_TYPE_INTEGER
      element_type: ELEM_TYPE_BYTES
      precision: 100
      indexOfKey: 7
      default_val: "0"
    }
    version: 1
    table_partition {
      columns: "test_part_column"
      range_partition {
        ranges {
          start_key: "100"
        }
        ranges {
          start_key: "200"
        }
        ranges {
          start_key: "300"
        }
      }
    }
    properties {
      key: "test property"
      value: "test_property_value"
    }
  }
}
"""
 
# Other
./dingodb_client_coordinator --method=GetTables --req_num=1 --coordinator-addr=172.26.0.2:22001 #  查询tables
./dingodb_client_coordinator --method=GetTableRange --coordinator-addr=192.168.1.201:32002 --id=1002  # 获取表分区的leader
```
