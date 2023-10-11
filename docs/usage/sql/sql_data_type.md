# Data Types

## Integer Numbers

| Data Type | Size   | Min Value | Max Value |
|-----------|--------|-----------|-----------|
| Int       | 4 byte | -2^31     | 2^31-1    |
| BigInt    | 8 byte | -2^63     | 2^63-1    |

## Real Numbers

| Data Type | Size   | Precision | Syntax |
| --------- | ------ | --------- | ------ |
| Float32   | 4 byte | 23 bits   | FLOAT  |
| Float64   | 8 byte | 53 bits   | DOUBLE |

## String Types

| Data Type | Syntax  |
| --------- | ------- |
| String    | Varchar |

## Boolean Type
| Date Type | Value      |
|-----------|------------|
| Boolean   | ture/false |

## Date Types

| Data Type | Size   | Resolution | Min Value       | Max Value       | Precision     |
|-----------| ------ |------------|-----------------|-----------------|---------------|
| Date      | 4 byte | day        | 1000-01-01      | 9999-12-31      | yyyy-MM-dd    |

## Time Types

| Data Type | Size   | Resolution | Min Value    | Max Value    | Precision    |
| --------- | ------ | ---------- |--------------|--------------|--------------|
| Time      | 4 byte | Time       | 00:00:00.000 | 59:59:59.999 | HH:mm:ss.sss |


## Timestamp Types

| Data Type | Size   | Resolution | Min Value               | Max Value               | Precision               |
| --------- | ------ | ---------- |-------------------------|-------------------------|-------------------------|
| Timestamp | 4 byte |  Timestamp | 1000-01-01 00:00:00.000 | 9999-12-31 59:59:59.999 | yyyy-MM-dd HH:mm:ss.sss |


## Composite Types

| Data Type    | Syntax                          |
|--------------|---------------------------------|
| Map          | Map [key,value]                 |
| Array        | Array [value1,value2,value3]    |
