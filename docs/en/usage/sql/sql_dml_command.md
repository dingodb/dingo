# DML Commands

## Insert Into Statement

### Syntax

```SQL
INSERT INTO table [(c1, c2, c3)] VALUES (v11, v12, v13), (v21, v22, v23), ...
```

### Examples

```SQL
dingo> CREATE TABLE test(id Int, name Varchar);

dingo> INSERT INTO test(id,name) values(1, 'qwe');
dingo> INSERT INTO test values(2, 'asd');

dingo> SELECT * FROM test;
+------+-------+
| id   | name  |
+------+-------+
|  1   | qwe   |
|  2   | asd   |
+------+-------+

dingo> INSERT OVERWRITE test values(2048, 'stars');
dingo> SELECT * FROM test;
+------+-------+
| a    | b     |
+------+-------+
| 2048 | stars |
+------+-------+
```

## Inserting the Results of SELECT

### Syntax

```sql
INSERT INTO table [(c1, c2, c3)] SELECT ...
```

### Examples

- basic example

```sql
dingo> CREATE TABLE select_table(id INT, name Varchar, dsc Varchar);
dingo> INSERT INTO select_table values(1,'qwe','asdasd');
dingo> SELECT * FROM select_table;
+------+------+------+
| id   | name | dsc  |
+------+------+------+
| 1    | qwe  |asdasd|
+------+------+------+

dingo> CREATE TABLE test(id Int, name varchar, dsc Varchar);
dingo> INSERT INTO test SELECT * FROM select_table;
dingo> SELECT * from test;
+------+------+------+
| id   | name | dsc  |
+------+------+------+
| 1    | qwe  |asdasd|
+------+------+------+
```

- aggeration example

```sql
# create table
dingo> CREATE TABLE test(id Int,name varchar,age int,amount double);

# insert some datas to test
dingo> insert into Test values
(1, 'Alice', 18, 3.5),
(2, 'Betty', 22, 4.1),
(3, 'Cindy', 39, 4.6),
(4, 'Doris', 25, 5.2),
(5, 'Emily', 24, 5.8),
(6, 'Alice', 32, 6.1),
(7, 'Betty', 30, 6.9),
(8, 'Alice', 22, 7.3),
(9, 'Cindy', 26, 7.5);

# COUNT()
dingo> select count(*) from Tets;
+------+
|EXPR$0|
+------+
|    9 |
+------+

# SUM()
dingo> select sum(amount) from Test;
+------+
|EXPR$0|
+------+
|  51  |
+------+

# MAX()
dingo> select MAX(amount) from Test;
+------+
|AMOUNT|
+------+
| 7.5  |
+------+

# MIN()
dingo> select MIN(amount) from Test;
+------+
|AMOUNT|
+------+
| 3.5  |
+------+

# AVG()
dingo> select AVG(amount) from Test;

```