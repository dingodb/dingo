# Release Notes v2.0.0

## New Features
### 1. Rich Vector Index Types, New DiskANN Disk Index 
- New DiskANN Disk Index is added to support efficient disk storage vector search. It improves the efficiency of large-scale vector data retrieval through approximate nearest-neighbor search, which is suitable for machine learning and recommendation system application scenarios. 
- Python SDK has integrated DiskANN to create interfaces, which makes it easy for developers to implement high-performance vector search in applications. 
### 2. Full-text indexing enhancements 
- Introduced full-text search engine, which supports BM25 search algorithm and optimizes the relevance and efficiency of document searching.
- C++ SDK and Python SDK now provide a complete full-text index search interface, supporting multiple query modes to meet different search needs. 
- SQL syntax layer supports full-text index search, allowing users to access full-text index data through standard SQL statements, simplifying the development process and lowering the threshold for users. 
- Full-text indexing supports pre & post filtering, improving query performance and response speed. 
### 3. Multi-Recall Query 
- Provides the ability of multi-recall query, supports vector+scalar+full-text index multi-query, meets complex query scenarios, and improves the query performance. 
### 4. Multi-tenant Management 
- Realizes a comprehensive multi-tenant management architecture, ensures isolation of Executor resources among different tenants, and enhances the system's security and stability.
### 5. MVCC RawKV Implementation 
- MVCC (Multi-Version Concurrency Control) transformation of RawKV interfaces for scalar table, vector table and document table, which improves concurrency performance and data consistency. 
- New TTL (Time to Live) support for RawKV, which allows users to set the expiration time of the data, optimizes data storage management, and cleans up the expired data automatically to save storage space. 
### 6. Online Schema Change 
- Provides online schema change function, which supports dynamic changes of indexes and columns without affecting system availability. Users can add, delete or modify columns and indexes at runtime without downtime during the change period, which significantly improves system flexibility and maintainability. 
### 7. SQL Syntax Expansion 
- New Version() function allows developers to quickly get the current version information of DingoDB, which facilitates the version management. 
- Supports to check the status of the current transaction in the cluster through SQL query, using the statement: SELECT * FROM INFORMATION_SCHEMA.DINGODB_TRX, which facilitates the monitoring and management of transactions, and improves the efficiency of troubleshooting. 
- Fully supports SQL syntax related to full-text indexing, which allows developers to query full-text indexes using standard SQL to support complex query requirements and lower the threshold of database usage. 
- Provides multi-tenant SQL syntax support, making data operations in multi-tenant environments more convenient and efficient, and simplifying cross-tenant data management.

## Feature Optimizations
### 1. Distributed Transaction Optimization 
- Single Region Transaction Support: Support submitting transactions in a single Region, simplifying transaction management 
- Pessimistic Transaction Performance Optimization: Support reading data when locking, allow reading locked data when executing a pessimistic lock, reduce transaction waiting time, thus improving concurrent performance 
### 2. Refactoring Dingo-Client 
- Dingo-Client has been completely refactored to improve the interface design and performance, and enhance the user experience 
### 3. Executor layer Task execution mechanism optimization 
- Manual task cancellation: support for manual task cancellation during task execution, which provides more flexible task management capabilities for developers. task management capabilities, and can effectively handle tasks that run for a long time. 
- Abnormal Task Handling: Implemented an abnormal task monitoring and handling mechanism to ensure that the task can be recovered or retried in a timely manner when it fails, which improves the robustness of the internal system. 
### 4. Shared Raft Log Storage 
- Implemented a shared Raft log storage mechanism, which reduces storage overhead. storage overhead. 
### 5. Optimize the use of Region 
- support for viewing the current Region information: Users can view the status and information of the current Region, which improves the user's visual management of the system. 
- support for querying the Region to which the row data belongs: Users can query the Region information to which the data belongs to a certain row of a table, which facilitates data location and management of the system. Region information: Users can query the information of the Region to which a row of data belongs in a table, which is convenient for data positioning and problem troubleshooting. 
- Manual Modification of Region Status: Supports manual modification of the Region status, which allows users to manually intervene under the necessary conditions, making the use of Region more flexible. 
### 6. GC Mechanism Optimization 
- adds the new function of MVCC Multi-Version Control to add the GC mechanism. GC mechanism 
- new multi-tenant management add GC mechanism 
- new feature full text index add GC mechanism