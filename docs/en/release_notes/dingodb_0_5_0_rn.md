# Release Notes v0.5.0

## 1. Feature about SQL

 1.1 Support fuzzy search with the 'LIKE' kerword.

 1.2 Support user authentication: user create、delete、upsert and query.

 1.3 Support user permission assignment.

 1.4 Support cluster authentication.

 1.5 Support SQL batch insert.

 1.6 Optimize the Calcite function verification mechanism.

 1.7 Refactor error code information.

## 2. Metadata Management

 2.1 Split cluster table granularity management to the executor.

 2.2 Abandon the original Dingo-jraft module.

 2.3 Migrate the original Dingo-jraft in the Coordiantor to Dingo-mpu.

 2.4 Support SQL-bases metadata table queris.

## 3. Index-related

 3.1 Support for index insert、delete、upsert and retrieval to improve query performance.
 3.2 Support for multiple types of indexes: non-primary key indexes and composite indexes.

## 4.SDK-related

 4.1 Support for chain expression-based calculations to perform aggregation calculations、update and more after multiple range searches.

 4.2 Support for non-primary key column scanning and filtering calculatons.

 4.3 List of metric calculation features:

| No  | Funcation Name   | Description about Function                                                                                                                          |
|-----|------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------|
| 1   | Scan             | scan records by keys                                                                                                                                |
| 2   | Get              | query  records by user key                                                                                                                          |
| 3   | Filter           | Filter records by keys                                                                                                                              |
| 4   | Add              | add values on same data type                                                                                                                        |
| 5   | Put              | insert or update records in table                                                                                                                   |
| 6   | Update           | upsert records by user key                                                                                                                          |
| 7   | Delete           | delete records by user key                                                                                                                          |
| 8   | DeleteRange      | delete range record by user key                                                                                                                     |
| 9   | Max              | calculate the max of columns filtered by keys                                                                                                       |
| 10  | Min              | calculate the min of columns filtered by keys                                                                                                       |
| 11  | Avg              | calculate the avg of columns filtered by keys                                                                                                       |
| 12  | Sum              | calculate the summary of columns filtered by keys                                                                                                   |
| 13  | Count            | calculate the count of columns filtered by key                                                                                                      |
| 14  | SortList         | Sorts input and stored numerical values in ascending order by default                                                                               |
| 15  | DistinctList     | Performs deduplication on input and stored numerical values, only recording duplicate values once.                                                  |
| 16  | List             | Query  records by user keys                                                                                                                         |
| 17  | IncreaseCount    | A sequence of numbers, count the number of times there is a consecutive increase between two adjacent points as the number of increments increases. |
| 18  | DecreaseCount    | A sequence of numbers, count the number of times there is a consecutive decrease between two adjacent points as the number of decrements increases. |
| 19  | maxIncreaseCount | A sequence of numbers, find the maximum number of consecutive increases in any single instance of consecutive increases.                            |
| 20  | maxDecreaseCount | A sequence of numbers, find the maximum number of consecutive decreases in any single instance of consecutive decreases.                            |

## 5.Columnar storage

 Support for columnar storage based on Merge Tree.

## 6.Distributed storage

 6.1 Resolved the issue of slow disk release for rocksdb update/delete

 6.2 Optimized Prefix Scan.

 6.3 Completed RocksDB version upgrade.

 6.4 Optimized RocksDB's I/O process.

 6.5 Released disk space after executing DeleteRange.

  6.6 Configurable fixed parameters for RocksDB.
