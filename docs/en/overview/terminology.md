# DingoDB Terminology

The terminology is encoded in lexicographical order.


## Coordinator

Maintain status of global and branch transactions, drive the global commit or rollback.

## Raft Engine

Raft Engine is an embedded persistent storage engine with a log-structured design. It is built for distributed key-value store to store multi-Raft logs. DingoDB supports using Raft Engine as the log storage engine.

## Partition

Partition is the minimal piece of data storage in DingoDB, each representing a range of data. Each Partition has three replicas by default. A replica of a Partition is called a peer. Multiple peers of the same Partition replicate data via the Raft consensus algorithm, so peers are also members of a Raft instance. DingoDB uses Multi-Raft to manage data. That is, for each Partition, there is a corresponding, isolated Raft group.

## Partition split

Partitions are generated as data writes increase. The process of splitting is called Partition split.
The mechanism of Partition split is to use one initial Partition to cover the entire key space, and generate new Partitions through splitting existing ones every time the size of the Partition or the number of keys has reached a threshold.

## Executor

Executors are the computing nodes where the tasks of a job are running, it alse works as storage nodes to store Partition data over cluster.
