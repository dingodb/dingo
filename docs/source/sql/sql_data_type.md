# Data Types

## List
| Data Type | Sample |
|-----------|--------|
| Int   | 123   |
| BigInt| 123   |
| Blob|   |
| Float|12.234   |
| Double|12.00   |
| Varchar|‘abc’ |
| Date|‘2022-03-07’   |
| Time|‘18:00:00.000’   |
| TimeStamp|‘2022-03-01 18:00:00.000’|
| Boolean|‘true’   |
| Array|array['v1','v2','v3']   |
| Map   | map['k1','v1','k2','v2'] |

## Specification
### Int
| Data Type | Size   | Min Value | Max Value |
|-----------|--------|-----------|-----------|
| Int       | 4 byte | -2^31     | 2^31-1    |

### BigInt
| Data Type | Size   | Min Value | Max Value |
|-----------|--------|-----------|-----------|
| BigInt    | 8 byte | -2^63     | 2^63-1    |

### Blob
| Data Type | Size   |
| --------- | ------ |
| Blob   | 65535 byte |

### Float
| Data Type | Size   | Precision | Syntax |
| --------- | ------ | --------- | ------ |
| Float32   | 4 byte | 23 bits   | FLOAT  |

### Double
| Data Type | Size   | Precision | Syntax |
| --------- | ------ | --------- | ------ |
| Float64   | 8 byte | 53 bits   | DOUBLE |

### Varchar
| Data Type | Syntax  |
| --------- | ------- |
| String    | Varchar |

### Date
| Data Type | Size   | Resolution | Min Value       | Max Value       | Precision     |
|-----------| ------ |------------|-----------------|-----------------|---------------|
| Date      | 4 byte | day        | 1000-01-01      | 9999-12-31      | yyyy-MM-dd    |

### Time
| Data Type | Size   | Resolution | Min Value    | Max Value    | Precision    |
| --------- | ------ | ---------- |--------------|--------------|--------------|
| Time      | 4 byte | Time       | 00:00:00.000 | 59:59:59.999 | HH:mm:ss.sss |

### TimeStamp
| Data Type | Size   | Resolution | Min Value               | Max Value               | Precision               |
| --------- | ------ | ---------- |-------------------------|-------------------------|-------------------------|
| Timestamp | 4 byte |  Timestamp | 1000-01-01 00:00:00.000 | 9999-12-31 59:59:59.999 | yyyy-MM-dd HH:mm:ss.sss |

### Boolean
| Date Type | Value      |
|-----------|------------|
| Boolean   | ture/false |

### Array
| Data Type    | Syntax                          |
|--------------|---------------------------------|
| Array        | Array [value1,value2,value3]    |

### Map
| Data Type    | Syntax                          |
|--------------|---------------------------------|
| Map          | Map [key,value]                 |