# Release Notes v0.7.0

## 1.Store Storage Layer
### 1.1 Distributed Storage
  * Provide the ability to manage IndexRegions, supporting dynamic creation and deletion of IndexRegions.
  *  Add functionality for Raft Snapshot creation and installation for IndexRegions, which helps generate and load snapshot data for IndexRegions, enhancing system reliability and recovery capabilities. 
  * Introduce the Build, Rebuild, and Load functions for VectorIndex to enable efficient creation, reconstruction, and loading of vector indexes, facilitating similarity search of vector data. 
  * Enhance the management capability of IndexRegion for capacity expansion and contraction, enabling dynamic adjustment of index size to accommodate changes in data scale. 
  * Support automatic splitting of VectorIndex/ScalarIndex Regions for region partitioning based on data load and distribution. 
  * Introduce a mechanism to load indexes only on the leader (saving memory), by concentrating index loading and maintenance tasks on the leader node to reduce memory consumption on other nodes.

### 1.2 Vector Index
  * Provide the ability to manage vector indexes, including operations such as creation, deletion, and querying of vector indexes. 
  * Offer diverse types of vector indexes, including HNSW, FLAT, IVF_FLAT, and IVF_PQ. 
  * Support read and write operations for scalar data, enabling mixed storage and fusion analysis of multimodal data. 
  * Enable top-N similarity search capability. 
  * Allow precise lookup based on ID. 
  * Provide the ability to perform batch queries based on specified offsets. 
  * Support pre-filtering in vector search by passing a scalar key during VectorSearch operation. 
  * Support post-filtering in vector search by passing a scalar key during VectorSearch operation.

### 1.3 Scalar Index
  * Support the creation of indexes on non-vector columns, providing more efficient query and retrieval capabilities for non-vector data. 
  * Provide the ability to manage scalar indexes, including operations such as creation, deletion, and querying of scalar indexes. 
  * Support LSM Tree-type ScalarIndex, using LSM Tree as the underlying storage structure to build ScalarIndex.

### 1.4 Distributed Lock
  *  Implement the Lease mechanism for distributed locks, allowing clients to acquire, release, and maintain distributed locks by managing the lifecycle and renewal of leases. 
  * Support MVCC (Multi-Version Concurrency Control) for key-value storage: The Coordinator stores all change records for each key-value pair and generates a globally unique revision for each change. 
  * Provide a simple and efficient OneTimeWatch mechanism for event notification scenarios that only require triggering once.

## 2. Executor Execution Layer
### 2.1 Data Types
  * Extend the Float data type to support high-dimensional data storage and processing for supporting vector databases.

### 2.2 SQL Syntax
  * Extend the CREATE TABLE statement to support creating scalar tables and vector tables. 
  * Add new vector index query functions for retrieving vector data. 
  * Introduce functions for text and image vectorization, converting text and images into vector representations.

### 2.3 SQL Optimizer
  * Support mapping statistics to Calcite selectivity calculations to accurately estimate query costs and select the optimal execution plan, thereby improving query performance and efficiency. 
  * Support different types of statistics: general statistics (e.g., Integer, Double, Float), cm_sketch, histograms, and Calcite's default calculation for all types. 
  * Introduce the ANALYZE TABLE command to collect statistics information, notifying the optimizer to collect and update statistics for specified tables. 
  * Provide a custom CostFactory to implement RelOptCost, redefine interfaces such as isLe, isLt, multiply, plus, and minus. 
  * Rewrite Dingo TableScan cost calculation. 
  * Modify DingoLikeScan selectivity estimateRowCount calculation method.

### 2.4 Pushdown computation
  * Optimize the C++ layer serialization and deserialization logic by reducing the number of deserialized columns, shortening the deserialization time. 
  * Add serialization and deserialization for List data type. 
  * Optimize the C++ expressions to improve computation efficiency. 
  * Support pushdown execution plan with a prefix selection to apply the query conditions to the data source as early as possible, reducing the number of rows that need to be read and processed.

### 2.5 Partitioning strategy
  * Add Hash-range partitioning strategy, which has some hashing properties to reduce data skew problems, achieving even distribution of data.

## 3. SDK Layer
### 3.1 Python SDK
  * Add a Python SDK client for communication with the server. 
  * Provide Python SDK functionality for Index operations. 
  * Support join operations in Python SDK. 
  * Use the pip package management tool to publish the Python SDK, improving its usability, maintainability, and portability. 
  * DingoDB-Python supports data serialization and deserialization using Proto.

### 3.2 Java SDK
  * Provide Java SDK functionality for Index operations. 
  * Provide distance measurement API for vector modules. 
  * Offer partitioning strategy based on Index, distributing data to different partitions based on the range of data index values, facilitating the proper configuration of partitioned data. 
  * DingoClient provides the ability to merge multiple partitions into one, simplifying the merging process and improving data management efficiency. 
  * Provide an index encoding mechanism based on AutoIncrement, automatically assigning a unique identifier to each new record to ensure that each record has a unique identifier.

## 4. Knowledge Assistant Support
  * Successfully integrated with the LangChain framework. 
  * Added support for cosine similarity queries, expanding the vector index query capabilities to include cosine similarity queries. This is useful for retrieving data such as text and images. 
  * Added a count interface to calculate the number of records in a data collection. 
  * Added a scan interface for scanning data collections while also satisfying scalar-based data filtering operations.
