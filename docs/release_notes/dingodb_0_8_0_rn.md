# Release Notes v0.8.0

## Major New Features
### 1. Distributed Transaction
The addition of distributed transaction capabilities meets the core ACID features of the database, ensuring the integrity and reliability of the database, and expands the range of applications.
* Transaction-related interfaces are added to the Store layer/Index layer/Executor layer.
* Provides the ability for garbage collection of distributed transaction data, cleaning up completed and no longer needed transaction data, freeing up storage space, and reducing storage space occupancy.
* Transaction table creation: When creating a table, specify ENGINE=LSM_TXN to complete the creation.
* Transaction commit methods:
    * Explicit commit: Use the COMMIT command to complete the commit.
    * Implicit commit: Use SQL commands (BEGIN, START TRANSACTION, etc.) to indirectly complete the commit.
    * Auto commit: After INSERT/UPDATE/DELETE execution, the system automatically completes the commit.
* Three transaction isolation levels: Read Committed, Repeatable Read.
* Two transaction modes: Optimistic and Pessimistic.
* Transaction locking mech anism: Provides table-level and row-level lock management. By locking tables/rows, it ensures transaction consistency and isolation, effectively avoiding data conflicts between concurrent transactions.
* Deadlock detection mechanism: Supports periodic checking of lock resources and waiting relationships in the system to identify potential deadlock situations.

### 2. Compute Pushdown
* Refactoring of compute pushdown, optimizing code execution logic, and improving data query performance.
* Supports expression compute pushdown, handling execution with expression syntax to improve computational efficiency.
* Supports Vector ScalarData operator pushdown: When performing vector approximate nearest neighbor search, filters scalar data to further select data that meets specific conditions.
* Python SDK introduces the Self Query feature, providing filtering capabilities for vector data Scalar Data, satisfying specific query vector data scenarios.

## Product Feature Enhancements
### 1. Data Storage Layer
#### 1.1 Architecture optimization
* Added encapsulation for google::protobuf::Closure to facilitate request statistics and log tracking.
* Refactored the RawRocksEngine class by splitting it into multiple files based on functionality and supporting multi-column family mode to address the bloated issues of the current RawRocksEngine.
* Refactored the StoreService/IndexService modules to unify the logic inside and outside the queue.
* Refactored the Storage class by extracting the execution queue logic and placing it in the traffic control module.
#### 1.2 Region Management (Merge & Split):
* Optimized the region split strategy by introducing backward region splitting in addition to the existing strategy.
* Added region merge functionality to the Store layer/Coordinator layer to dynamically adjust data and optimize storage space utilization.
* Supported splitting in multi-column family mode, greatly improving scalability, performance, and reliability. Adopted a unified encoding format compatible with key encoding formats for distributed transactions.
#### 1.3 Vector Indexing
* Based on retrieval speed, a new IVF_FLAT vector indexing method based on inverted indexes is added, which is suitable for high-dimensional sparse vector data. It provides fast retrieval speed and good retrieval performance.
* Based on memory, a new IVF_PQ vector indexing method is added, which is based on inverted indexes and product quantization. It is suitable for high-dimensional dense vector data and offers good search speed and low storage overhead.
* Based on accuracy, a new BruteForce index is added, which is suitable for small-scale vector datasets or scenarios that require high search accuracy.
#### 1.4 Storage Engines
* Added B+Tree engine to optimize database query performance.
* Added XDP engine to achieve high-performance data processing.
* Diversified storage engine support, allowing users to specify specific storage engines based on their actual business needs.
#### 1.5 Snapshot Capability Upgrade
* Upgraded VectorIndex to support multi-column family storage.
* Snapshot supports multi-column family storage mode and is compatible with key encoding formats for distributed transactions.
* Implemented Fake Snapshot to reduce I/O burden.
* Supported BaikalDB-style save/load snapshot.

### 2. Executor Execution Layer
#### 2.1 Data Types
* Added Blob data type for storing binary data such as images, audio, videos, etc.
#### 2.2 SQL Syntax
* SQL layer provides batch data import and export.
* Added vector distance calculation functions:
    * Inner product distance: ipDistance
    * Euclidean distance: l2Distance
    * Cosine distance: cosineDistance
* Support vector queries without functions, allowing vector queries even without vector indexes.
* Table supports Chinese for table creation, insertion, querying, updating, and deletion.
* Distributed transaction-related parameters:
    * Support transaction parameter settings at different levels: Global/Session.
    * Timeout settings, supporting setting retry or blocking timeout, automatically rolling back after the timeout:
        * Lock_wait_timeout
        * Set [session | global] statement_timeout = timeout
#### 2.3 Module Refactoring
* Based on version-based new features, refactor existing modules such as Store/Task/Job/Calcite/Client for distributed transactions.
* Integrate the client-side with the SQL execution layer to optimize the system architecture and reduce code redundancy.

### 3. SDK Layer
* Added C++ SDK, enabling independent integration testing execution with Dingo-store based on the C++ SDK.

### 4. Operations and Monitoring
* Visual web monitoring interface to monitor the real-time health status of Store, Executor, and Coordinator components, providing cluster-wide monitoring information.
