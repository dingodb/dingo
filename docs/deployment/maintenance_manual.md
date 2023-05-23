# Maintenance manual

**Explanation of Notation**

- ${xxx} represents the configuration item set during installation.

  For example, if the installer_root_path is set to /home/dingo-store during installation, then ${installer_root_path} in this document represents /home/dingo-store.
 
## 1. Directory Structure Planning

### 1.1 Ansible deploy

| Directory Types                                                                | Path                                       |
|--------------------------------------------------------------------------------|:-------------------------------------------|
| Program installation path                                                      | ${installer_root_path}                     |
| Data path                                                                      | ${installer_root_path}/data                |
| Log path                                                                       | ${installer_root_path}/log                 |
| dingo-store script path                                                        | ${installer_root_path}/dingo-store/scripts |
| dingo-store deployment path <br/>(including separate data and log directories) | ${installer_root_path}/dingo-store/dist    |

### 1.2 Docker-compose deploy

| Directory Types                                                                | Path                     |
|--------------------------------------------------------------------------------|:-------------------------|
| Program installation path                                                      | /opt                     |
| dingo-store script path                                                        | /opt/dingo-store/scripts |
| dingo-store deployment path <br/>(including separate data and log directories) | /opt/dingo-store/dist    |
| executor's script path                                                         | /opt/dingo/bin           |
| executor's log path                                                            | /opt/dingo/log           |

## 2. User group planning

| Deployment Method | User name | Gorup name |
|-------------------|-----------|------------|
| ansible deploy    | dingodb   | dingodb    |
| docker deploy     | root      | root       |



## 3. Component list and port usage

### 3.1 Component list

#### 3.1.1 Ansible deploy

| Component        | Version | Port                                                               | Remarks                                                             |
|------------------|---------|--------------------------------------------------------------------|---------------------------------------------------------------------|
| DingoDB          | 0.6.0   | raft:22101/20101  <br/>server:22001/20001  <br/>executor:3307/8765 | Real-time analytics database                                        |
| Prometheus       | 2.14.0  | 19115                                                              | Real-time analytics databaseTime-series database, metric monitoring |
| node_exporter    | 0.18.1  | 19100                                                              | Prometheus component, monitoring node information                   |
| process_exporter | 0.7.10  | 19090                                                              | Prometheus component, monitoring process information                |
| grafana          | 8.3.3   | 3000                                                               | Data visualization platform                                         |

#### 3.1.2 Docker-compose部署

| Component | Version | Port                                                                                             | Remarks                                         |
|-----------|---------|--------------------------------------------------------------------------------------------------|-------------------------------------------------|
| DingoDB   | 0.6.0   | raft:22101-22103/20101-20103     <br/>server:22001-22003/20001-20003     <br/>executor:3307/8765 | Real-time analytics database                    |

## 4.Service Start and Stop

### 4.1 Ansible deploy

#### 4.1.2 Introduction DingoControl

DingoControl comes with some commonly used commands, mainly for multi-node management operations. For example, if you want to stop the store service in dingodb on all store nodes with one click, you can execute:

```shell
./DingoControl stop store
```
the command called is:
```shell
ansible store -m shell -a "${INSTALLER_PATH}/dingo-store/scripts/start-store.sh  stop" --become --become-user ${USER}
```

More examples can be found in the following instructions. If you need to manually execute commands, refer to the corresponding function call commands.

```shell
# Prerequisite: group_vars/all/_shared.yml and hosts have been configured.
# Grant permissions to DingoControl
chmod +x DingoControl
 
# DiongControl command explanation
Options:
  --help        display this help and exit
  deploy        deploy the application 
  show          show all process for user
  stop          stop  server
  clean         clean server
  start         start server
  cleanstart    stop clean deploy  and start server
  install       install playbook
 
# install
  # Installation options: playbook installs the default configuration, dingodb installs dingo (executor) and dingo-store, monitor installs all monitoring tools, and the rest are single-step installations.
  # example: ./DingoControl install playbook
  playbook    playbook.yml
  system      install system
  jdk         install jdk
  dingo       install dingo
  dingo-store install dingo-store
  dingodb     install dingo and dingo-store
  prometheus  install prometheus
  grafana     install grafana
  node        install node_exporter
  process     install process_exporter
  monitor     install prometheus grafana node_exporter process_exporter
 
 
 
# deploy
  # The deployment module includes the deployment of coordinator and store, and generates the directory structure before startup.
  # example: ./DingoControl deploy all
  all               stop/clean/deploy store/coordinator
  store             stop/clean/deploy store
  coordinator       stop/clean/deploy coordinator  
 
# show
  # example: ./DingoControl show process
  process          show all process # Display all processes of the deployed user.
  file             show all file not user # Check if there are directories with non-deployed user permissions under the deployment directory.
 
# stop
  # Stop the process, you can stop a single process, or stop all
  # ./DingoControl stop store
  all               stop all process
  store             stop store
  coordinator       stop coordinator
  executor          stop executor
  node-exporter     stop node-exporter
  process-exporter  stop process-exporter
  prometheus        stop prometheus
  grafana           stop grafana
 
# clean
  # Clean up files, automatically stop processes, only "all" to clean up all files and delete the directory, coordinator and store to clear deployment files and data, monitoring module to clean up systemctl service files.
  # example: ./DingoControl clean store
  all               stop all server and clean all file, if want del completely
  dingo-store       stop/clean store/coordinator and clean dingo-store, deprecated
  store             stop/clean store
  coordinator       stop/clean coordinator
  node-exporter     stop/clean node-exporter
  process-exporter  stop/clean process-exporter
  prometheus        stop/clean prometheus
  grafana           stop/clean grafana
 
 
# start
  # Start the process, automatically execute mysql_init.sh to initialize tables when starting the store.
  # example: ./DingoControl start store
  all               start all process
  store             start store
  coordinator       start coordinator
  executor          start executor
  node-exporter     start node-exporter
  process-exporter  start process-exporter
  prometheus        start prometheus
  grafana           start grafana
 
 
# cleanstart
  # Clear the data, re-deployment, and startup the coordinator, store, and executor.
  # example: ./DingoControl cleanstart all
  all   stop-clean-deploy store/coordinator and restart executor
```

### 4.2 Docker-compose delpoy

Currently, the deployment and startup of docker-compose are combined, and every time it is started, the original data is not preserved.

```
# To start a container and change its IP address to the IP address of the host machine.
DINGO_HOST_IP=127.0.0.1 docker-compose -f ./docker-compose.yml up -d

# To stop and remove a container
DINGO_HOST_IP=127.0.0.1 docker-compose -f ./docker-compose.yml down
```

## 5.  dingo-storecluster status check and operation

Enter the directory configured in installer_root_path.

#### 5.1 Check the status of the coordinator

```shell
cd dingo-store/build/bin
./dingodb_client_coordinator --method=GetCoordinatorMap  

# Output result
WARNING: Logging before InitGoogleLogging() is written to STDERR
E20230517 11:29:53.836163 1828522 coordinator_client.cc:320] [main] coordinator url is empty, try to use file://./coor_list
I20230517 11:29:53.850924 1828526 naming_service_thread.cpp:203] brpc::policy::FileNamingService("./coor_list"): added 3
I20230517 11:29:53.851150 1828522 coordinator_interaction.cc:64] [InitByNameService] Init channel by service_name file://./coor_list service_type=0
I20230517 11:29:53.851306 1828522 coordinator_interaction.cc:64] [InitByNameService] Init channel by service_name file://./coor_list service_type=1
I20230517 11:29:53.882277 1828529 coordinator_interaction.h:208] [SendRequestByService] name_service_channel_ connect with meta server finished. response errcode: 0, leader_addr: 172.20.3.201:22001
I20230517 11:29:53.882387 1828529 coordinator_client_function_coor.cc:496] [SendGetCoordinatorMap] SendRequest status=OK
I20230517 11:29:53.882423 1828529 coordinator_client_function_coor.cc:497] [SendGetCoordinatorMap] leader_location {
  host: "172.20.3.202"
  port: 22001
}
auto_increment_leader_location {
  host: "172.20.3.202"
  port: 22001
}
```

#### 5.2 Check the status of the store

```shell
./dingodb_client_coordinator --method=GetStoreMap

# Output result
WARNING: Logging before InitGoogleLogging() is written to STDERR
E20230517 11:30:12.944561 1828534 coordinator_client.cc:320] [main] coordinator url is empty, try to use file://./coor_list
I20230517 11:30:12.958177 1828539 naming_service_thread.cpp:203] brpc::policy::FileNamingService("./coor_list"): added 3
I20230517 11:30:12.958379 1828534 coordinator_interaction.cc:64] [InitByNameService] Init channel by service_name file://./coor_list service_type=0
I20230517 11:30:12.958513 1828534 coordinator_interaction.cc:64] [InitByNameService] Init channel by service_name file://./coor_list service_type=1
I20230517 11:30:12.968495 1828539 coordinator_interaction.h:208] [SendRequestByService] name_service_channel_ connect with meta server finished. response errcode: 0, leader_addr: 172.20.3.202:22001
I20230517 11:30:12.968590 1828539 coordinator_client_function_coor.cc:457] [SendGetStoreMap] SendRequest status=OK
I20230517 11:30:12.968619 1828539 coordinator_client_function_coor.cc:459] [SendGetStoreMap] epoch: 1003
storemap {
  epoch: 1003
  stores {
    id: 1201
    state: STORE_NORMAL
    server_location {
      host: "172.20.3.202"
      port: 20001
    }
    raft_location {
      host: "172.20.3.202"
      port: 20101
    }
    keyring: "TO_BE_CONTINUED"
    create_timestamp: 1683768684801
    last_seen_timestamp: 1684294210333
  }
  stores {
    id: 1101
    state: STORE_NORMAL
    server_location {
      host: "172.20.3.200"
      port: 20001
    }
    raft_location {
      host: "172.20.3.200"
      port: 20101
    }
    keyring: "TO_BE_CONTINUED"
    create_timestamp: 1683768687632
    last_seen_timestamp: 1684294210202
  }
  stores {
    id: 1001
    state: STORE_NORMAL
    server_location {
      host: "172.20.3.201"
      port: 20001
    }
    raft_location {
      host: "172.20.3.201"
      port: 20101
    }
    keyring: "TO_BE_CONTINUED"
    create_timestamp: 1683768692877
    last_seen_timestamp: 1684294207689
  }
}
I20230517 11:30:12.970433 1828539 coordinator_client_function_coor.cc:467] [SendGetStoreMap] store_id=1201 state=1 in_state=0 create_timestamp=1683768684801 last_seen_timestamp=1684294210333
I20230517 11:30:12.970482 1828539 coordinator_client_function_coor.cc:467] [SendGetStoreMap] store_id=1101 state=1 in_state=0 create_timestamp=1683768687632 last_seen_timestamp=1684294210202
I20230517 11:30:12.970510 1828539 coordinator_client_function_coor.cc:467] [SendGetStoreMap] store_id=1001 state=1 in_state=0 create_timestamp=1683768692877 last_seen_timestamp=1684294207689
I20230517 11:30:12.970541 1828539 coordinator_client_function_coor.cc:474] [SendGetStoreMap] DINGODB_HAVE_STORE_AVAILABLE, store_count=3
```

#### 5.3 Create table

```shell
# The parameter --name specifies the name of the table. The returned value includes entity_id: 66006, which is the ID number of the table.
./dingodb_client_coordinator --method=CreateTable  --name=test1

# Output result
WARNING: Logging before InitGoogleLogging() is written to STDERR
E20230517 11:34:04.153780 1828620 coordinator_client.cc:320] [main] coordinator url is empty, try to use file://./coor_list
I20230517 11:34:04.167536 1828630 naming_service_thread.cpp:203] brpc::policy::FileNamingService("./coor_list"): added 3
I20230517 11:34:04.167704 1828620 coordinator_interaction.cc:64] [InitByNameService] Init channel by service_name file://./coor_list service_type=0
I20230517 11:34:04.167815 1828620 coordinator_interaction.cc:64] [InitByNameService] Init channel by service_name file://./coor_list service_type=1
W20230517 11:34:04.174784 1828630 coordinator_interaction.h:199] [SendRequestByService] name_service_channel_ connect with meta server success by service name, connected to: 172.20.3.201:22001 found new leader: 172.20.3.202:22001
I20230517 11:34:04.202661 1828628 coordinator_client_function_meta.cc:314] [SendCreateTable] SendRequest status=OK
I20230517 11:34:04.202737 1828628 coordinator_client_function_meta.cc:315] [SendCreateTable] table_id {
  entity_type: ENTITY_TYPE_TABLE
  parent_entity_id: 2
  entity_id: 66006
}

```

#### 5.4 Select/Delete table

```shell
# Retrieve information about all tables, including their IDs. Specific information about a table can be obtained by querying that table using its ID
./dingodb_client_coordinator --method=GetTables


# To retrieve information about a specific table, including its TableDefinition and TableRange, use the parameter --id=66006, where 66006 is the ID number of the table
./dingodb_client_coordinator --method=GetTable  --id=66006 
./dingodb_client_coordinator --method=GetTableRange  --id=66006
./dingodb_client_coordinator --method=GetTableByName  --name=test1

# Delete table
./dingodb_client_coordinator --method=DropTable  --name=test1
```

#### 5.5 Newly added in iteration 2: AddPeer, RemovePeer, SplitRegion, and MergeRegion

```shell
1.AddPeer：The parameter --id specifies the store_id to which you want to expand, while --region-id specifies the region number you want to expand.
./dingodb_client_coordinator --method=AddPeerRegion --id=33004 --region-id=77001
After execution, you can confirm the Raft status of the region by using the QueryRegion method to view the region information.
./dingodb_client_coordinator --method=QueryRegion --id=77001
 
2.RemovePeer: The parameter --id specifies the store_id to be removed from the region, while --region-id specifies the region number to be scaled down.
./dingodb_client_coordinator --method=RemovePeerRegion --id=33002 --region-id=77001
 
3.SplitRegion：
To perform the SplitRegion action, use the parameter --split_from_id to specify the region number to be split.
./dingodb_client_coordinator --method=SplitRegion --split_from_id=77001

# Output result
WARNING: Logging before InitGoogleLogging() is written to STDERR
E20230507 23:17:49.362282 76278 coordinator_client.cc:317] [main] coordinator url is empty, try to use file://./coor_list
I20230507 23:17:49.364794 76284 naming_service_thread.cpp:203] brpc::policy::FileNamingService("./coor_list"): added 4
I20230507 23:17:49.364826 76278 coordinator_interaction.cc:65] [InitByNameService] Init channel by service_name file://./coor_list service_type=0
I20230507 23:17:49.364861 76278 coordinator_interaction.cc:65] [InitByNameService] Init channel by service_name file://./coor_list service_type=1
E20230507 23:17:49.364909 76290 coordinator_client_function_coor.cc:902] [SendSplitRegion] split_key is empty, will auto generate from the mid between start_key and end_key
W20230507 23:17:49.366366 76286 coordinator_interaction.h:199] [SendRequestByService] name_service_channel_ connect with meta server success by service name, connected to: 192.168.1.203:32002 found new leader: 192.168.1.203:32003
I20230507 23:17:49.366703 76284 coordinator_client_function_coor.cc:909] [SendSplitRegion] SendRequest status=OK
I20230507 23:17:49.366714 76284 coordinator_client_function_coor.cc:910] [SendSplitRegion] region {
  id: 77001
  epoch: 1
  state: REGION_NORMAL
  raft_status: REGION_RAFT_HEALTHY
  replica_status: REPLICA_NORMAL
  leader_store_id: 33001
  definition {
    id: 77001
    epoch: 1
    name: "2_1683472470_part_0_77001"
    peers {
      store_id: 33001
      server_location {
        host: "192.168.1.203"
        port: 30001
      }
      raft_location {
        host: "192.168.1.203"
        port: 30101
      }
    }
    peers {
      store_id: 33002
      server_location {
        host: "192.168.1.203"
        port: 30002
      }
      raft_location {
        host: "192.168.1.203"
        port: 30102
      }
    }
    peers {
      store_id: 33003
      id: 77001
      epoch: 1
      name: "2_1683472470_part_0_77001"
      peers {
        store_id: 33001
        server_location {
          host: "192.168.1.203"
          port: 30001
        }
        raft_location {
          host: "192.168.1.203"
          port: 30101
        }
      }
      peers {
        store_id: 33002
        server_location {
          host: "192.168.1.203"
          port: 30002
        }
        raft_location {
          host: "192.168.1.203"
          port: 30102
        }
      }
      peers {
        store_id: 33003
        server_location {
          host: "192.168.1.203"
          port: 30003
        }
        raft_location {
          host: "192.168.1.203"
          port: 30103
        }
      }
      range {
        start_key: "0"
        end_key: "100"
      }
      schema_id: 2
      table_id: 66001
    }
  }
  create_timestamp: 1683472470654
  last_update_timestamp: 1683472662912
}
I20230507 23:17:49.367532 76284 coordinator_client_function_coor.cc:926] [SendSplitRegion]  start_key = 30
I20230507 23:17:49.367545 76284 coordinator_client_function_coor.cc:927] [SendSplitRegion]  end_key   = 313030
I20230507 23:17:49.367553 76284 coordinator_client_function_coor.cc:933] [SendSplitRegion]  diff      = 013030
I20230507 23:17:49.367559 76284 coordinator_client_function_coor.cc:934] [SendSplitRegion]  half_diff = 009818
I20230507 23:17:49.367565 76284 coordinator_client_function_coor.cc:935] [SendSplitRegion]  mid       = 00309818
I20230507 23:17:49.367571 76284 coordinator_client_function_coor.cc:938] [SendSplitRegion]  mid real  = 309818
I20230507 23:17:49.367584 76284 coordinator_client_function_coor.cc:954] [SendSplitRegion] split from region 77001 to region 0 with watershed key [309818] will be sent
I20230507 23:17:49.369107 76285 coordinator_client_function_coor.cc:959] [SendSplitRegion] SendRequest status=OK
I20230507 23:17:49.369127 76285 coordinator_client_function_coor.cc:960] [SendSplitRegion]
I20230507 23:17:49.369138 76285 coordinator_client_function_coor.cc:970] [SendSplitRegion] split from region 77001 to region 0 with watershed key [309818] success
 
After execution, you can confirm the result of the split by using the QueryRegion method to view the region information.
./dingodb_client_coordinator --method=QueryRegion --id=77001
      range {
        start_key: "0"
        end_key: "0\230\030"
      }
 
./dingodb_client_coordinator --method=QueryRegion --id=77004
      range {
        start_key: "0\230\030"
        end_key: "100"
      }
 
By observing the range information, you can confirm that the split for range 1001 has been completed.
 
```

#### 5.6 The store node triggers a snapshot

##### 5.6.1 Manually trigger a snapshot

```
bin/dingodb_client_store --method=Snapshot --addrs=The corresponding business port for the store (non-Raft port) is --region_id=1007 (corresponding to the ID of the region)
```

##### 5.6.2 Automatically trigger a snapshot

```
dingo@inspur201 ➜  dist git:(main) ✗ cat store1/conf/store.yaml
cluster:
  name: dingodb
  instance_id: 1001
  coordinators: 192.168.1.201:22001,192.168.1.201:22002,192.168.1.201:22003
  keyring: TO_BE_CONTINUED
server:
  host: 192.168.1.201
  port: 20001
  heartbeatInterval: 10000 # ms
  metricsCollectInterval: 300000 # ms
raft:
  host: 192.168.1.201
  port: 20101
  path: /home/dingo/huzx/dingo-store/dist/store1/data/store/raft
  electionTimeout: 1000 # ms
  snapshotInterval: 60 这里标识Snapshot出发的时间间隔

```

##### 5.6.3 View Snapshot

###### 5.6.3.1 Viewing method 1 (using the browser function provided in Brpc).

Access the browser address： http://IP:20101/raft_stat

```
2_XYZ_part_0_1007
peer_id: 192.168.1.201:20103:0
state: LEADER
readonly: 0
term: 2
conf_index: 4115622
peers: 192.168.1.201:20101:0 192.168.1.201:20102:0 192.168.1.201:20103:0
changing_conf: NO stage: STAGE_NONE
election_timer: timeout(1000ms) STOPPED
vote_timer: timeout(2000ms) STOPPED
stepdown_timer: timeout(1000ms) SCHEDULING(in 484ms)
snapshot_timer: timeout(600000ms) SCHEDULING(in 282096ms)
storage: [2036336, 5216082]
disk_index: 5216082
known_applied_index: 5216081
last_log_id: (index=5216082,term=2)
state_machine: Idle
last_committed_index: 5216081
pending_index: 5216082
pending_queue_size: 1
last_snapshot_index: 4115622
last_snapshot_term: 2
snapshot_status: IDLE
replicator_14353780703353@192.168.1.201:20101:0: next_index=5216083 flying_append_entries_size=1 appending [5216082, 5216082] hc=15168 ac=5215677 ic=0
replicator_27565100105857@192.168.1.201:20102:0: next_index=5216083 flying_append_entries_size=1 appending [5216082, 5216082] hc=15168 ac=5215659 ic=0
```

###### 5.6.3.1 Viewing method 2

```
View the directory corresponding to the disk，./store1/data/store/raft/1001/snapshot/snapshot_xxx.
Note: Only one snapshot operation can be performed at a time in a store, and there is only one snapshot directory.
```



