# Release Notes v0.3.0 

## 1.Semantics and Function of SQL 

### 1.1 New data type
- Boolean
- Date: default format yyyy-MM-dd
- Time: default format HH:mm:ss
- Timestamp: default format yyyy-MM-dd HH:mm:ss.SSS

### 1.2 Allow assigning a default value to column, either constant or internal functions

### 1.3 Support Join operation 
- Inner Join
- Left Join
- Right Join
- Full Join
- Cross Join

### 1.4 Function list about String

| No |  Function Names |                                        Notes about Function                                       |
|:--:|:---------------:|:-------------------------------------------------------------------------------------------------:|
| 1  | Concat          | Adds two or more expressions together                                                             |
| 2  | Format          | Formats a number to a format like "#,###,###.##", rounded to a specified number of decimal places |
| 3  | Locate          | The LOCATE() function returns the position of the first occurrence of a substring in a string     |
| 4  | Lower           | Converts a string to lower-case                                                                   |
| 5  | Lcase           | Converts a string to lower-case                                                                   |
| 6  | Upper           | Converts a string to upper-case                                                                   |
| 7  | Ucase           | Converts a string to upper-case                                                                   |
| 8  | Left            | Extracts a number of characters from a string (starting from left)                                |
| 9  | Right           | Extracts a number of characters from a string (starting from right)                               |
| 10 | Repeat          | Repeats a string as many times as specified                                                       |
| 11 | Replace         | Replaces all occurrences of a substring within a string, with a new substring                     |
| 12 | Trim            | Removes leading and trailing spaces from a string                                                 |
| 13 | Ltrim           | Removes leading spaces from a string                                                              |
| 14 | Rtrim           | Removes trailing spaces from a string                                                             |
| 15 | Mid             | Extracts a substring from a string (starting at any position)                                     |
| 16 | Substring       | Extracts a substring from a string (starting at any position)                                     |
| 17 | Reverse         | Reverses a string and returns the result                                                          |

### 1.5 Function list about Date and Time

| No |   Function Names  |                Notes about Function                |
|:--:|:-----------------:|:--------------------------------------------------:|
| 1  | Now               | Return current date and time                       |
| 2  | CurrentDate       | Return the current date                            |
| 3  | Current_date      | Return the current date                            |
| 4  | CurTime           | Return the current time                            |
| 5  | Current_time      | Return the current time                            |
| 6  | Current_timestamp | Return the current date and time                   |
| 7  | From_UnixTime     | Convert unix time to timestamp                     |
| 8  | Unix_Timestamp    | Format the time to unix timestamp                  |
| 9  | Date_Format       | Formats a date                                     |
| 10 | DateDiff          | Returns the number of days between two date values |
| 11 | Time_Format       | Formats a time by a specified format               |

## 2. Management of Replicator

### 2.1 Management of metadata

- Physical table can be split into N partitions based on data size
- Management of physical tables such as table creation time, table status, partition strategy, split conditions, etc

### 2.2 Scheduler of partition replicator

- Support multiple partition modes, such as One table with one partition, One table with multiple partitions
- Support multiple split strategies, such as auto-split or manually split by API
- Support resource isolation between physical tables

### 2.3 Tools of partition management

- Support to view status about partition, such as leader, follower, etc
- Support to migrate, split partition by internal API
- Support to view metrics about partition, such as write, read latency, size, record count

## 3. The data access method for DingoDB

### 3.1 JDBC mode

- Support to connect to dingo by JDBC

### 3.2 SDK client mode

- Support to put, get, and delete records to tables in dingo
- Support to batch write records to tables in dingo

### 3.3 Import data from external

- Support to import data from local files in CSV, JSON format
- Support to import data from Kafka in JSON and Avro format


## 4. Tools and Monitor

- Support to monitor dingo cluster by grafana and prometheus
- Support to management partitions of the cluster by API
- Support to adjust log level dynamically by tools
- Support to deploy cluster by ansible or docker-compose
- Newly add autotests more than 1300+
