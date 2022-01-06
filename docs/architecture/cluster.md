# Cluster

DingoDB cluster consists of two distributed components :Coordinator and Executor.

The cluster is managed using [Apache Helix](http://helix.apache.org/). Helix is a cluster management framework for managing replication and partitioning resources in distributed systems. Helix uses Zookeeper to store cluster status and metadata.



## Coordinator

The coordinator is responsible for these things:

- Controllers maintain the global metadata (e.g. configs and schemas) of the system with the help of Zookeeper which is used as the  persistent metadata store.
- Controller modeled as Helix Controller and is responsible for managing executors.
- They maintain the mapping of which executors are responsible for which table partitions. 

There can be multiple instances of DingoDB coordinator for redundancy. 



## Executor

Executors are responsible for maintaining table partitions and provide the ability to read and wirte the table partitions they responsible.

The DingoDB executor is modeled as a Helix Participant, responsible tables, and the table are modeled as Helix Resources. The partitions of the table are modeled as partitions of the Helix resource. Thus, a DingoDB executor responsible one or more helix partitions of one or more helix resources.



## Table

Tables are collections of rows and columns representing related data that DingoDB divides into partitions that are stored on local disks or in cloud storage.

In the DingoDB, a table is modeled as a Helix resource and each partition of a table is modeled as a Helix Partition.

The resource state model uses the Leader/Follower model.

- Leader: Supports both data write and data query operations
- Follower: synchronizes data from the Leader and supports query rather than direct writing
- Offline: the standby node is Offline and does not process data
- Dropped: Deletes the mapping between a data partition and Executor

Each data partition in a cluster has only one Leader. The Leader is selected by a Coordinator and is preferentially selected from the followers. The selected followers are promoted to the Leader.There can be multiple followers, depending on the number of replicas.

Whether a Table can be responsible for an Executor is determined by the tag. When a Table is created, a tag is set for the Table. During partition allocation, the Executor that contains the tag is selected and the Executor that does not contain the tag is excluded.

```{mermaid}
stateDiagram
    [*] --> OFFLINE
    OFFLINE  --> LEADER
    OFFLINE  --> FOLLOWER
    OFFLINE  --> DROPPED
    LEADER   --> FOLLOWER
    LEADER   --> OFFLINE
    FOLLOWER --> LEADER
    FOLLOWER --> OFFLINE
    DROPPED  --> [*]
```



## cli-tools

### Rebalance

Reset the mapping of which executors are responsible for which table partitions.

When running this command, it resets the mapping that executor is responsible for a given table partition,the coordinator then detects the change, it reassigns table partitions to the executor based on policies defined by the Leader/Follower state model.

```shell
tools.sh rebalance --name TableName --replicas 3 # if replicas is not set or replicas less than 1, replicas use default replicas
```



### Sqlline

Command-line shell for issuing SQL to DingoDB.

SQLLine is a pure-Java console based utility for connecting to DingoDB and executing SQL commands,you can use it like `sqlplus `for Oracle,`mysql` for MySQL. It is implemented with the open source project [sqlline](https://github.com/julianhyde/sqlline).

```shell
tools.sh sqlline
```



### Tag

Set table tag, or add table tag to executor.

Because whether a table can be responsible for an Executor is determined by the tag,so you can use this command to add a tag to executor.

In general, no special tag Settings are required, and a newly created table will use the default tag.Therefore, any executors can responsible the table partitions.

If you want to set the specified executors to responsible the table partitions, use this command to set a tag for the table and then add the tag to the desired executors.

```shell
tools.sh tag --name TableName --tag TagName  # set table tag
tools.sh tag --name ExecutorName --tag TagName # add tag to executor
tools.sh tag --name ExecutorName --tag TagName --delete # delete tag for executor
```

