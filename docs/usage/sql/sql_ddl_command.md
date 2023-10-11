 # DDL Commands

## Create Table

### Syntax

```
CREATE TABLE [IF NOT EXISTS] table_name
(
    <col_name> <col_type> [ { DEFAULT <expr> }],
    <col_name> <col_type> [ { DEFAULT <expr> }],
    ...
)
```

### Examples

```
dingo> CREATE TABLE test(id Int, name Varchar, address Varchar);

dingo> INSERT INTO test values(1,'xiaoming','shanghai');

dingo> select * from test;
+------+--------+---------+
| ID   | NAME   |ADDRESS  |
+------+--------+---------+
|  1   |xiaoming|shanghai |
+------+--------+---------+
```
## Create IndexTable

### Syntax

* Index 

```
INDEX
[ index_name ]
( VECTOR | [SCALAR] ) ( {index_parameters} )
[ WITH ( {columns} ) ]
[ PARTITION BY
(  RANGE( { columns } )    |  HASH( {columns} )
(  PARTITIONS={partitions} |  VALUES(( {partition_values} )*) )
]
( PARAMETERS ) (vector_parameters)
[ REPLICA={replica}]
```

* Index Table

```
CREATE TABLE [IF NOT EXISTS] table_name (
  <col_name> <col_type> [ { DEFAULT <expr> }],
  <col_name> <col_type> [ { DEFAULT <expr> }],
  INDEX [ index_name ] ( VECTOR | [SCALAR] ) ( {index_parameters} ) [ WITH ( {columns} ) ]
  [ PARTITION BY
    (  RANGE( { columns } )    |  HASH( {columns} )
    (  PARTITIONS={partitions} |  VALUES(( {partition_values} )*) )
  ]
  ( PARAMETERS ) (vector_parameters) 
  [ REPLICA={replica}]
);
```

### Examples

* Create a table with scalar indexes

```
dingo> CREATE TABLE test
(
    id BIGINT NOT NULL AUTOINCREMENT,
    value BIGINT NOT NULL,
    tag1 VARCHAR NOT NULL,
    tag2 VARCHAR NOT NULL,
    INDEX tag1_index (tag1),  //scalar index
    INDEX tag2_index (tag2) WITH (value) PARTITION BY RANGE(tag1) values('a'), ('b') REPLICA = 3,  //scalar index
    PRIMARY KEY(id)
);
```

* Create a table with vector indexes

```
CREATE TABLE test
(
    id BIGINT NOT NULL AUTOINCREMENT,
    tag VARCHAR NOT NULL,
    feature FLOAT ARRAY NOT NULL, //vector column
    feature_id BIGINT NOT NULL,
    INDEX feature_index VECTOR(feature_id,feature) WITH (id, tag) PARTITION BY HASH(tag1) PARTITIONS=5 REPLICA = 3 parameters(type=flat, metricType=L2, dimension=3),   //vector index
    PRIMARY KEY(id)
)
```
## Drop Table

Deletes the table.

### Syntax

```sql
DROP TABLE [IF EXISTS] name
```

### Examples

```sql
dingo> CREATE TABLE test(id INT, name Varchar);
dingo> DROP TABLE test;
```

