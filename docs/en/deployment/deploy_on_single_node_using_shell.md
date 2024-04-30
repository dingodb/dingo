# Deploy On Single Node Using Shell

## Deploy Architecture View

There are many types of roles in the cluster mode of DingoDB, such as coordinator, executor, dingo-store, and the cluster mode requires at least three machines. The architecture view can refer [dingo-deploy-architecture](https://dingodb.readthedocs.io/en/latest/deployment/deploy_on_cluster_by_ansible.html).


## Deploy a 3-node cluster on a single machine
```shell
cd dingodb_test/scripts
vim deploy_server.sh
#Modify SERVER_HOST and RAFT_HOST to the IP address of the server
#Example：
# SERVER_HOST=127.0.0.1
# RAFT_HOST=127.0.0.1
sh clean_start_cluster.sh
```

## Command-line tool preparation
After deployment, first confirm whether the coor_list file exists in the build/bin directory. The latest deployment script will automatically generate this file.

If it does not exist in build/bin, you can find the coor_list file in the conf directory of the coordinator and store, and copy the file to the directory where dingodb_client_coordinator is located.

If you are connecting to a cluster deployed with an old deployment script, there may not be a coor_list file. You can manually create a coor_list file with the following content format:
```shell
127.0.0.1:22001
127.0.0.1:22002
127.0.0.1:22003
```
With this file, you can proceed with the subsequent operations.

The command-line parameters for dingodb_client_coordinator are:
```shell
./dingodb_client_coordinator --coor_url=file://./coor_list --method=GetStoreMap
```
You can also use the --url parameter abbreviation instead of --coor_url.

If the coor_list file is located in the current directory, you can omit the --coor_url parameter, as follows:
```shell
./dingodb_client_coordinator --method=GetStoreMap
```

## View cluster status
```shell
cd dingodb_test/build/bin

# 1.Check which node is the leader of the coordinator
./dingodb_client_coordinator --method=GetCoordinatorMap
#Confirm the leader location based on the output, and send subsequent requests to the leader location
"""
leader_location {
  host: "127.0.0.1"
  port: 22003
}
"""
#leader_location {
#  host: "127.0.0.1"
#  port: 22003
#}

# 2.Check the status of all stores to confirm whether they are sending heartbeats normally. If you can see information about the stores, it means that the system is working properly and you can proceed with further testing.
./dingodb_client_coordinator --method=GetStoreMap
"""
storemap {
   epoch: 103
   stores {
     id: 1001
     server_location {
       host: "127.0.0.1"
       port: 20001
     }
     raft_location {
       host: "127.0.0.1"
       port: 20101
     }
   }
   stores {
     id: 1002
     server_location {
       host: "127.0.0.1"
       port: 20002
     }
     raft_location {
       host: "127.0.0.1"
       port: 20102
     }
   }
   stores {
     id: 1003
     server_location {
       host: "127.0.0.1"
       port: 20003
     }
     raft_location {
       host: "127.0.0.1"
       port: 20103
     }
   }
 }
"""
```
