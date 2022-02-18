# Release Notes v0.2.0 

* Architecture
    1. Refactor DingoDB architecture abandon Zookeeper, Kafka and Helix.
    1. Using raft as the consensus protocol to make agreement among multiple nodes on membership selection and data replication.
    1. Region is proposed as the unit of data replication, it can be scheduled, split, managed by `coordinator`. 
    1. Rocksdb is replaced by distributed key-value.

* Distributed Storage
    1. Support region to replicate between nodes.
    1. Support region to split based on policies such as key counts or region size.
    1. Support Region to perform periodic snapshot. 

* SQL
    1. Support multiple aggregation functions, such as min,max,avg, etc.
    2. Support `insert into ... select`.
    
* Client Tools
    1. thin jdbc driver
