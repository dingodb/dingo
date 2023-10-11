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

### SELECT vector

```sql
select * from vector(table_name, vector_index,m,n,y); 
```
- Paramter

 | paramter     | description                                                                |
|--------------|----------------------------------------------------------------------------|
 | table_name   | Table‘s name.                                                              |
| vector_index | Vector index.                                                              |
| m            | The vector to be queried.                                                  |
 | n            | Refers to the number of rows to be queried.                                |
| y            | Refers to additional query parameters, such as efSearch in HNSW algorithm. |

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
### LIKE

```
dingo> SELECT * FROM TEST;
+----+-------+-----+--------+----------+-----------------------+
| ID | NAME  | AGE | AMOUNT |  CLASS   |     BIR               |
+----+-------+-----+--------+----------+-----------------------+
| 1  | lisa  | 25  | 92.0   | class_03 | 2000-02-23 18:00:00.0 |
| 2  | alice | 18  | 99.0   | class_01 | 2003-02-23 17:00:00.0 |
| 3  | Ben   | 19  | 90.0   | class_03 | 2002-01-11 16:00:00.0 |
| 4  | Joy   | 18  | 99.5   | class_03 | 2003-11-25 23:00:00.0 |
| 5  | Susan | 19  | 98.5   | class_02 | 2003-02-23 23:05:00.0 |
+----+-------+-----+--------+----------+-----------------------+

dingo> SELECT * FROM TEST where name like ‘%e%’;
+----+-------+-----+--------+----------+-----------------------+
| ID | NAME  | AGE | AMOUNT |  CLASS   |     BIR               |
+----+-------+-----+--------+----------+-----------------------+
| 2  | alice | 18  | 99.0   | class_01 | 2003-02-23 17:00:00.0 |
| 3  | Ben   | 19  | 90.0   | class_03 | 2002-01-11 16:00:00.0 |
+----+-------+-----+--------+----------+-----------------------+
```

### GROUP BY

```sql
dingo> SELECT class,count(amount) FROM  TEST group by class;
+----------+--------+
|  CLASS   | EXPR$1 
+----------+--------+
| class_01 | 2      |
| class_03 | 6      |
| class_02 | 2      |
+----------+--------+
```
