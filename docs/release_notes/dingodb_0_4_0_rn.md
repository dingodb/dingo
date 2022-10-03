# Release Notes v0.4.0 

## 1. Feature and Optimization about SQL

### 1.1 Features about SQL

#### 1.1.1 Extended SQL Syntax

- Support TTL when create table using options
- Support to assign partitions when create table

#### 1.1.2 Features about Complex Data Type

- Support Operations about MAP
- Support Operations about MultiSet
- Support Operations about Array

#### 1.1.3 Support to use variables in SQL statement, such as insert, select, delete.

#### 1.1.4 Support stratagy to control messages transmitted between operators in execution plan

#### 1.1.5 Support new SQL function

| No | Function Name | Description about Function                                                                         |
|----|---------------|----------------------------------------------------------------------------------------------------|
| 1  | pow(x,y)      | The POW() function returns the value of a number raised to the power of another number             |
| 2  | round(x,y)    | The ROUND() function rounds a number to a specified number of decimal places                       |
| 3  | ceiling(x)    | The CEILING() function returns the smallest integer value that is bigger than or equal to a number |
| 4  | floor(x)      | The FLOOR() function returns the largest integer value that is smaller than or equal to a number   |
| 5  | mod(x,y)      | The MOD() function returns the remainder of a number divided by another number                     |
| 6  | abs(x)        | The ABS() function returns the absolute (positive) value of a number.                              |

### 1.2 Optimization about SQL

- Optimizate query using range filter
- Optimizate query about range scan
- Optimizate type system about dingo internally
- Optimization about SQL date/time/timestamp function

## 2. Operation of Key-Value

### 2.1 Equivalent operation of Key-Value and SQL

- Support to do table operation using Key-Value API, such as create table, drop table
- Support to insert, update, delete record in table using Key-Value API
- Support to do table operation using Annotation API
- Operations about table and record are equivalent between Key-Value API and SQL

### 2.2 Operation lists about Key-Value SQL

#### 2.2.1 Basic Key-Value Operation

| No | Function Name  | Description about Function                     |
|------|:-------|------------------------------|
| 1    | put    | insert or update records in table |
| 2    | get    | query  records by user key    |
| 3    | delete | delete records by user key    |

#### 2.2.2 Numerical operations

| No | Funcation Name | Description about Function                        |
|----|----------------|---------------------------------------------------|
| 1  | add            | add values on same data type                      |
| 2  | sum            | calculate the summary of columns filtered by keys |
| 3  | max            | calculate the max of columns filtered by keys     |
| 4  | min            | calculate the min of columns filtered by keys     |


#### 2.2.3 Compound operation


| No | Function Name | Description about Function                                                                                  |
|----|---------------|-------------------------------------------------------------------------------------------------------------|
| 1  | Operate       | do multiple operations on a single record, the operation list can be numerical operation or basic operation |
| 2  | OperateList   | do multiple operations on a single record                                                                   |
| 3  | UDF           | defined using LUA script to implement user define function                                                  |


#### 2.2.4 Collection operations

| No | Type  | Function Name       | Description about Function                        |
|----|:------|---------------------|---------------------------------------------------|
| 1  | read  | size                | get size of the elements                          |
| 2  | read  | get_all             | get all the elements of collection                |
| 3  | read  | get_by_key          | get all the elements of collection by input key   |
| 4  | read  | get_by_value        | get all the elements of collection by input value |
| 5  | read  | get_by_index_range  | get all the elements of collection by range index |
| 6  | write | put                 | append a element to the end                       |
| 7  | write | clear               | clear all the elements of collection              |
| 8  | write | remove_by_key       | remove the key from collection                    |
| 9  | write | remove_all_by_value | remove all records match the value                |
| 10 | write | remove_by_index     | remove record by index                            |

#### 2.2.5 Filter operations

- DateFilter 

Query records using range filter with `Date` type.

- NumberRange

Query records using range filter with Numberic type.

- StringRange

Query records using range filter with String type

- ValueEquals

Query records with specifiy record value.


## 3. Optimization about Storage

### 3.1 Distributed Consistency Protocol

-  Refactor the implements of raft protocol to replace `sofa-jraft`
-  Refactor the implements about log replication and leader selection
-  Support new serialization about key and value

### 3.2 Improvement about Rocksdb

- Rocksdb can load configuration by files
- Support TTL features using user timestamp
- Update Rocksdb version and release package about `io.dingodb.` on maven central

## 4. Other features

- Support parameters using JDBC connection such as `timeout`
- Support `explain` to view plan about Dingo SQL
- Support to release related package to maven-central


| No | Module              | Description about module                                 |
|----|---------------------|----------------------------------------------------------|
| 1  | dingo-driver-client | the jdbc driver client used by sql                      |
| 2  | dingo-sdk           | the key-value sdk client to do operation about key-value |
| 3  | dingo-rocksdb       | Extended features on rocksdb                                  |

