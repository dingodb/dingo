# DDL Commands

## Create Table

### Syntax

```sql
CREATE TABLE [IF NOT EXISTS] table_name
(
    <col_name> <col_type> [ { DEFAULT <expr> }],
    <col_name> <col_type> [ { DEFAULT <expr> }],
    ...
)
```

### Examples

```sql
dingo> CREATE TABLE test(id Int, name Varchar, address Varchar);

dingo> INSERT INTO test values(1,'xiaoming','shanghai');

dingo> select * from test;
+------+--------+---------+
| ID   | NAME   |ADDRESS  |
+------+--------+---------+
|  1   |xiaoming|shanghai |
+------+--------+---------+
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

