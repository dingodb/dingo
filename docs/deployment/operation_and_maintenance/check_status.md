# Check status

## Check the status of the coordinator
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

## Check the status of the store
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

