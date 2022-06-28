# What is DingoDB

DingoDB is a real-time Hybrid Serving & Analytical Processing (HSAP) Database. It can execute high-frequency query and
upsert, interactive analysis, multi-dimensional analysis in extremely low latency.

![](../images/dingo_stack.png)

## Key Features

1. Compliant with MySQL-Compatible  
   Based on the popular [Apache Calcite](https://calcite.apache.org/) SQL engine, DingoDB can parse, optimize and
   execute standard SQL statement, and is capable to run part of TPC-H and TPC-DS (See [TPC](http://www.tpc.org/))
   queries. DingoDB is also compliant with JDBC and can be seamlessly integrated with web services, BI tools, etc.
2. Support high frequency write operation  
   By using the log structured key-value storage [RocksDB](https://rocksdb.org/), DingoDB support high frequency write
   operation like INSERT, UPDATE, DELETE.
3. Support point query and multi-dimensional analysis simultaneously  
   DingoDB own capability to execute high concurrent point query, upsert and fast multi-dimensional analysis in low latency.
4. Easily integrated with streaming data and other DBMS's  
   By providing dedicated APIs for popular streaming data processing engine,
   e.g. [Apache Flink](https://flink.apache.org/), DingoDB can easily accept data from them, and support more analysis
   working or web serving that is not applicable to be done in stream. DingoDB can also access databases of many types,
   using pluggable connectors for each of them.
5. A distributed architecture with flexible and elastic scalability  
   DingoDB stores and processes data in a distributed manner with strong cluster and resource management functionality,
   which make it easy to expand the capacity.
6. Supports high availability with automatic failover when a minority of replicas fail; transparent to applications.
