# Region Operation

## SplitRegion
To perform the SplitRegion action, use the parameter --split_from_id to specify the region number to be split.
```shell
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
