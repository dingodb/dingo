# Data Types

## Integer Numbers

| Data Type | Size   | Min Value | Max Value |
| --------- | ------ | :-------- | --------- |
| Int       | 1 byte | -128      | 127       |

## Real Numbers

| Data Type | Size   | Precision | Syntax |
| --------- | ------ | --------- | ------ |
| Float32   | 4 byte | 23 bits   | FLOAT  |
| Float64   | 8 byte | 53 bits   | DOUBLE |

## String Types

| Data Type | Syntax  |
| --------- | ------- |
| String    | Varchar |

## Time and Date Types

| Data Type | Size   | Resolution | Min Value  | Max Value  | Precision  |
| --------- | ------ | ---------- | ---------- | ---------- | ---------- |
| Date      | 2 byte | day        | 1000-01-01 | 9999-12-31 | YYYY-MM-DD |

For example:

```
CREATE TABLE test
(
    id Int,
    date Date
);

INSERT INTO test VALUES (1,'2022-02-21');
```

