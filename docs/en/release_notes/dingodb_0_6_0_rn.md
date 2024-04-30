# Release Notes v0.6.0

## 1. Architecture Layer

### 1.1 Storage-Compute Separation
  * Compute Engine (Executor): It receives SQL queries based on the MySQL protocol and DingoDB's proprietary protocol. It performs SQL parsing, logical plan generation, and execution plan generation, and interfaces with the underlying Store for storage operations.
  * Distributed Storage Engine (Store): It is an efficient distributed storage engine based on C++. The storage layer is divided into metadata storage and data storage. The design of the storage layer allows flexible extension with multiple storage engines such as RocksDB, memory, xdp-rocks, etc.
  * Support for Compute Pushdown Operations: To efficiently leverage the benefits of aggregation and filtering operations and improve computational efficiency, the storage layer supports the logical implementation of compute pushdown. It supports operations such as filter, count, sum, min, max, etc.

### 1.2 Raft Upgrade
  * Provides a leader election mechanism that supports multi-node elections.
  * Provides log replication to ensure system reliability and prevent data loss effectively.
  * Offers high-performance Raft implementation using multi-threading and asynchronous I/O, improving system throughput and response speed.
  * Introduces a snapshot mechanism for restoring the state of the state machine. This reduces the size of logs, thereby improving performance, and can also be used to quickly recover the state of the state machine in the event of node failures.
  * Provides cluster scaling and migration capabilities, making it easier to add or remove nodes without compromising the stability and consistency of the entire system.

### 1.3 Protocol Layer Support for MySQL Protocol
  * Provides interactive command-line tools like MySQL Shell for efficient management and operation of MySQL databases. It also supports SSL encryption to ensure the security of the database.
  * Provides MySQL JDBC Driver, a database connection driver, to access MySQL databases through the JDBC API in Java applications. It enables connecting to and manipulating MySQL databases.

### 1.4 Cluster Operations and Monitoring
  * Offers visual monitoring, including Grafana monitoring and HTTP monitoring, to monitor cluster nodes (disk, CPU, IO, etc.), tables (partition, region), region monitoring, and Raft group monitoring.
  * Provides multiple deployment options: standalone, docker-compose, and Ansible multi-node deployment.
  * Provides cluster online scaling and scaling down solutions for cluster expansion and contraction operations.

## 2. Function Layer

### 2.1 Common and Basic Modules
  * Supports manual adjustment of log levels, allowing flexible control of the level of detail in logs based on specific scenarios, reducing log file size and storage costs.
  * Optimizes error codes for Store & Dingo Client.
  * Supports C++ version of data serialization, which parses serialized data according to the format used during serialization and restores the parsed data to its original form.

### 2.2 Raft Management and Distributed Storage
  * Provides a snapshot mechanism for restoring the state of the state machine.
  * Supports Region Split, automatically splitting a Region into multiple Regions when it exceeds the maximum limit. This ensures that the sizes of the Regions are similar, facilitating scheduling decisions.
  * Supports Region Merge, merging two smaller adjacent Regions when a Region's size decreases due to a large number of deletions.
  * Optimizes Range validation rules, improving code execution efficiency and reducing data query time, resulting in significant performance improvements.
  * Supports flexible configuration of the number of threads in the dingo-store service, allowing users to adjust the number of threads based on specific scenarios.
  * Supports specifying failure points (failpoints) during runtime for easy testing of corner cases.
  * Supports management of Metric information for Store & Region.
  * supports operator pushdown, where the storage layer provides basic operators and the DingoClient acts as a bridge between SDK and SQL scenarios. The supported operators include:
    * SUM
    * SUM0
    * COUNT
    * MAX
    * MIN
    * COUNTWITHNULL
  * Store supports multiple engines, such as RocksDB, memory, xdp-rocks.
  * Supports Auto Increment ID, where when creating a table with an auto-increment column, DingoDB automatically assigns a unique integer value to each row inserted into the table. This is achieved using a distributed sequence generator to ensure that the values of the auto-increment column are unique across the entire cluster.

### 2.3 SQL Protocol Layer
  * Refactored the architecture of the Executor component, which is responsible for computation. It parses and responds to SQL requests from the client and other management requests.
  * Compatible with the MySQL protocol.
  * Completed the upgrade of Calcite to improve the execution efficiency of SQL queries.
  * Collects metric information at the table level.
  * Added task response mechanisms (STOP/READY/QUIT) to the network transport layer.

### 2.4 SQL Syntax Extensions
  * Extended the ability to specify the number of replicas and partitioning information when creating tables, along with relevant auxiliary information.
  * Extended the ability to perform Region splitting through SQL, enabling more flexible and user-friendly data distribution management.
  * Extended the MySQL protocol with the following syntax:
    * Support for viewing global/user/session variables.
    * Support for setting global/user/session variables.
    * Support for viewing table structure and information about specific columns.
    * Support for viewing table/user creation statements.
    * Support for setting the idle timeout duration for the MySQL driver.
    * Support for SQL prepared statements.

### 2.5 Java SDK Layer

The SDK (Software Development Kit) is a set of software tools and programs created for developers to interact with the database through specific API interfaces. It allows developers to perform database operations in a more flexible and efficient manner, reducing learning costs and greatly improving development productivity. Here are the functional features supported by the DingoDB Java SDK layer:
  * Supports cluster operations through DingoDB's proprietary API interface.
  * Supports table operations (create/delete).
  * Supports single-record operations (view/insert/delete/update).
  * Supports batch data operations (view/insert/delete/update).
  * Supports aggregated operations on filtered ranges:
    * SUM
    * SUM0
    * COUNT
    * MAX
    * MIN
    * COUNTWITHNULL

### 2.6 DevOps Layer
  * Visual System Monitoring
    * Node information monitoring: Helps users observe server node status changes more effectively.
    * System process monitoring: Helps users promptly detect abnormal processes and respond to them in a timely manner.
  * Addition of System Operations Tools
    * Support for multi-node deployment: Utilizes Ansible automation tool for batch deployment, including system configuration, program deployment, and command execution.
  * Addition of DBA-level System Management Tools
    * Support for Leader migration: Allows switching the Leader of the same group to another follower node for load balancing or emergency restart situations.
    * Support for Region splitting/merging: Automatically splits/merges Regions into multiple Regions to ensure balanced sizes and achieve load balancing.
    * Support for node scaling: Users can add or remove nodes based on the distribution of data in their specific scenarios to achieve load balancing.
    * Visual Schema/Table/Region management: Provides visual tools for effectively monitoring Schema, Table, and Region information.
