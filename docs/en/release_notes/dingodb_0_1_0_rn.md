# Release Notes v0.1.0 

* Cluster
    1. Distributed computing. Cluster nodes are classified into coordinator role and executor role.
    1. Distributed meta data storage. Support creating and dropping meta data of tables.
    1. Coordinators support SQL parsing and optimizing, job creating and distributing, result collecting.
    1. Executors support task executing.
* Data store
    1. Using RocksDB storage.
    1. Encoding and decoding in Apache Avro format.
    1. Table partitioning by hash of primary columns.
* SQL parsing and executing
    1. Create (`CREATE TABLE`) and drop table (`DROP TABLE`).
    1. Supporting common SQL data types: TINYINT, INT, BIGINT, CHAR, VARCHAR, FLOAT, DOUBLE, BOOLEAN
    1. Insert into (`INSERT INTO TABLE`) and delete from (`DELETE FROM TABLE`) table.
    1. Query table (`SELECT`).
    1. Support filtering and projecting in query.
    1. Support expressions in filter conditions and projecting columns.
    1. Support point query.
* User interface
    1. Command line interface (CLI)
    1. Support SQL input and executing in CLI.
    1. Output query results in table format in CLI.
    1. Output time consumed by query in CLI.
