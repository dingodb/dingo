# Query and Operators

## SELECT

Retrieves data from a table.

### SELECT  all

```sql
dingo> select * from TEST;
+----+-------+--------+
| ID | NAME  | AMOUNT |
+----+-------+--------+
| 1  | Alice | 3.5    |
| 2  | Betty | 4.0    |
| 3  | Cindy | 4.5    |
| 4  | Doris | 5.0    |
| 5  | Emily | 5.5    |
| 6  | Alice | 6.0    |
| 7  | Betty | 6.5    |
| 8  | Alice | 7.0    |
| 9  | Cindy | 7.5    |
+----+-------+--------+
```

### SELECT clause

```sql
dingo> SELECT amount FROM TEST;
+--------+
| AMOUNT |
+--------+
| 3.5    |
| 4.0    |
| 4.5    |
| 5.0    |
| 5.5    |
| 6.0    |
| 6.5    |
| 7.0    |
| 7.5    |
+--------+

```

### WHERE

```sql
dingo> SELECT * FROM TEST WHERE id > 5;
+----+-------+--------+
| ID | NAME  | AMOUNT |
+----+-------+--------+
| 6  | Alice | 6.0    |
| 7  | Betty | 6.5    |
| 8  | Alice | 7.0    |
| 9  | Cindy | 7.5    |
+----+-------+--------+
```

### Distinct 

```sql
dingo>insert into Test values
(14, 'QWE', 3.5),
(11, 'ASE', 3.5);

dingo>select * from TEST;
+----+-------+--------+
| ID | NAME  | AMOUNT |
+----+-------+--------+
| 1  | Alice | 3.5    |
| 2  | Betty | 4.0    |
| 3  | Cindy | 4.5    |
| 4  | Doris | 5.0    |
| 5  | Emily | 5.5    |
| 6  | Alice | 6.0    |
| 7  | Betty | 6.5    |
| 8  | Alice | 7.0    |
| 9  | Cindy | 7.5    |
| 15 | QWE   | 3.5    |
| 16 | ASE   | 3.5    |
+----+-------+--------+
11 rows selected (0.107 seconds)


dingo> select distinct amount from Test;
+--------+
| AMOUNT |
+--------+
| 7.5    |
| 3.5    |
| 7.0    |
| 6.5    |
| 6.0    |
| 5.5    |
| 5.0    |
| 4.5    |
| 4.0    |
+--------+
9 rows selected (0.499 seconds)

```

