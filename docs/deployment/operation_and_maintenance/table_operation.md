# Table operation

## Create table
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

## Select table
```shell
# Retrieve information about all tables, including their IDs. Specific information about a table can be obtained by querying that table using its ID
./dingodb_client_coordinator --method=GetTables

# To retrieve information about a specific table, including its TableDefinition and TableRange, use the parameter --id=66006, where 66006 is the ID number of the table
./dingodb_client_coordinator --method=GetTable  --id=66006 
./dingodb_client_coordinator --method=GetTableRange  --id=66006
./dingodb_client_coordinator --method=GetTableByName  --name=test1
```

## Delete table
```shell
./dingodb_client_coordinator --method=DropTable  --name=test1
```
