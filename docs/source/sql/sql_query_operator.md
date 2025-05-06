# Query Operators

## SELECT

Retrieves data from a table.

### 1. SELECT  all

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

### 2. SELECT vector

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

### 3. SELECT clause

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

### 4. WHERE

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

### 5. Distinct 

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
### 6. LIKE

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

### 7. GROUP BY

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
## Join

### 1. Inner Join

The relationship in the inner join condition is an equal-quantity relation, returning all data that satisfies the join condition. If no match is found, the query result returns an empty result set.

Syntax

```
SELECT <column> 
FROM <table1>
Inner JOIN <table2>
ON table1.column = table2.column;
```

- Return Type

```
Result set
```

- Examples

```sql
dingo::///> SELECT student_01.name,student_01.age,student_01.amount,student_01.class, student_02.name,student_02.age,student_02.amount,student_02.school
FROM student_01
Inner JOIN student_02
ON student_01.age = student_02.age;
+-----+----+-------+------+------+----+-------+---------+
| NAME  | AGE | AMOUNT |  CLASS  |   NAME   | AGE | AMOUNT |     SCHOOL     |
+-----+----+-------+------+------+----+-------+---------+
| Alice | 15  | 98.5   | class01 | zhangsna | 15  | 86.0   | haidianyizhong |
| Cindy | 18  | 100.0  | class01 | liuliu   | 18  | 100.0  | shiyanzhognxue |
| Doris | 17  | 100.0  | class03 | yangyi   | 17  | 100.0  | shiyanyizhong  |
+-----+----+-------+------+------+----+-------+---------+
```

### 2. Inner Join (non-equivalent)

The relationship in the inner join condition is a non-equivalent relationship, returning all data that satisfies the join condition. If no match is found, the query result returns an empty result set.

- Syntax

```
SELECT <column> 
FROM <table1>
Inner JOIN <table2>
ON table1.column （>、<、<> ) table2.column;
```

- Return Type

```
Result set
```

- Examples

```sql
dingo::///>SELECT student_01.name,student_01.age,student_01.amount,student_01.class, student_02.name,student_02.age,student_02.amount,student_02.school
FROM student_01
Inner JOIN student_02
ON student_01.age = student_02.age AND student_01.amount > student_02.amount;
+-----+----+-------+------+------+----+-------+---------+
| NAME  | AGE | AMOUNT |  CLASS  |   NAME   | AGE | AMOUNT |     SCHOOL     |
+-----+----+-------+------+------+----+-------+---------+
| Alice | 15  | 98.5   | class01 | zhangsna | 15  | 86.0   | haidianyizhong |
+-----+----+-------+------+------+----+-------+---------+
```

### 3. Full join

 The join table will contain all the records from the two tables. The join statement is equivalent to multiplying the two tables and using the NULL value as the result of missing matches on both sides.

- Syntax

```
SELECT <column 
FROM <table1>
FULL JOIN <table2>
ON table1.column = table2.column;
```

- Return Type

Result set

- Examples

```sql
dingo::///> SELECT student_01.*,student_02.* FROM student_01 FULL JOIN student_02 ON student_01.age = student_02.age;
+------+-------+------+--------+---------+-----------+------+----------+------+---------+---------+----------------+
|  ID  | NAME  | AGE  | AMOUNT |  CLASS  |  ADDRESS  | ID0  |  NAME0   | AGE0 | AMOUNT0 | CLASS0  |     SCHOOL     |
+------+-------+------+--------+---------+-----------+------+----------+------+---------+---------+----------------+
| 1    | Alice | 15   | 98.5   | class01 | beijing   | 1    | zhangsna | 15   | 86.0    | class01 | haidianyizhong |
| 2    | Joy   | 13   | 95.0   | class02 | beijing   | NULL | NULL     | NULL | NULL    | NULL    | NULL           |
| 3    | Cindy | 18   | 100.0  | class01 | shanghai  | 4    | liuliu   | 18   | 100.0   | class01 | shiyanzhognxue |
| 4    | Betty | 22   | 99.0   | class01 | guangzhou | NULL | NULL     | NULL | NULL    | NULL    | NULL           |
| 5    | Doris | 17   | 100.0  | class03 | shenzhen  | 5    | yangyi   | 17   | 100.0   | class03 | shiyanyizhong  |
| NULL | NULL  | NULL | NULL   | NULL    | NULL      | 2    | lisi     | 19   | 72.0    | class02 | rendafuzhong   |
| NULL | NULL  | NULL | NULL   | NULL    | NULL      | 3    | wangzi   | 16   | 90.0    | class01 | qinghuafuzhong |
+------+-------+------+--------+---------+-----------+------+----------+------+---------+---------+----------------+
```

### 4. Cross join

Returns the Cartesian product of joined tables.

- Syntax

```
SELECT <column>
FROM <table1> 
CROSS JOIN <table2>
[WHERE ... ]；
OR
SELECT <column>
FROM <table1>, <table2>
[WHERE ... ] ；
```

- Return Type

```
Result set
```

- Examples

```sql
dingo::///> select student_01.*,student_02.* from student_01 cross join student_02
     WHERE student_01.amount = student_02.amount;
+----+-------+-----+--------+---------+----------+-----+--------+------+---------+---------+----------------+
| ID | NAME  | AGE | AMOUNT |  CLASS  | ADDRESS  | ID0 | NAME0  | AGE0 | AMOUNT0 | CLASS0  |     SCHOOL     |
+----+-------+-----+--------+---------+----------+-----+--------+------+---------+---------+----------------+
| 3  | Cindy | 18  | 100.0  | class01 | shanghai | 4   | liuliu | 18   | 100.0   | class01 | shiyanzhognxue |
| 3  | Cindy | 18  | 100.0  | class01 | shanghai | 5   | yangyi | 17   | 100.0   | class03 | shiyanyizhong  |
| 5  | Doris | 17  | 100.0  | class03 | shenzhen | 4   | liuliu | 18   | 100.0   | class01 | shiyanzhognxue |
| 5  | Doris | 17  | 100.0  | class03 | shenzhen | 5   | yangyi | 17   | 100.0   | class03 | shiyanyizhong  |
+----+-------+-----+--------+---------+----------+-----+--------+------+---------+---------+----------------+
```

### 5. Left join

Get all records in the left table, even if there is no corresponding matching data in the right table. If no match is found, the query result returns an empty result set.

- Syntax

```
SELECT <column> 
FROM <table1>
LEFT JOIN <table2>
ON table1.column = table2.column;
```

- Return Type

```
Result set
```

- Examples

```sql
dingo::///> SELECT student_01.name,student_01.age,student_01.address,student_02.name,student_02.age,student_02.school FROM student_01 LEFT JOIN student_02 ON student_01.age = student_02.age;
+-----+--- +--------+-------+-----=+--------+
| NAME  | AGE |  ADDRESS  |   NAME   | AGE  |     SCHOOL     |
+-----+--- +--------+-------+-----=+--------+
| Alice | 15  | beijing   | zhangsna | 15   | haidianyizhong |
| Joy   | 13  | beijing   | NULL     | NULL | NULL           |
| Cindy | 18  | shanghai  | liuliu   | 18   | shiyanzhognxue |
| Betty | 22  | guangzhou | NULL     | NULL | NULL           |
| Doris | 17  | shenzhen  | yangyi   | 17   | shiyanyizhong  |
+-----+--- +--------+-------+-----=+--------+
```

### 6. Right join

 Get all the data in the data table on the right, even if there is no corresponding data in the table on the left. If no match is found, the query result returns an empty result set.

- Syntax

```
SELECT <column> 
FROM <table1>
RIGHT JOIN <table2>
ON table1.column = table2.column;
```

- Return Type

```
Result set
```

- Examples

```sql
dingo::///> SELECT student_01.name,student_01.age,student_01.address,student_02.name,student_02.age,student_02.school
FROM student_01
RIGHT JOIN student_02
ON student_01.age = student_02.age;
+-----+--- +--------+-------+-----=+--------+
| NAME  | AGE  | ADDRESS  |   NAME   | AGE |     SCHOOL     |
+-----+--- +--------+-------+-----=+--------+
| Alice | 15   | beijing  | zhangsna | 15  | haidianyizhong |
| Cindy | 18   | shanghai | liuliu   | 18  | shiyanzhognxue |
| Doris | 17   | shenzhen | yangyi   | 17  | shiyanyizhong  |
| NULL  | NULL | NULL     | lisi     | 19  | rendafuzhong   |
| NULL  | NULL | NULL     | wangzi   | 16  | qinghuafuzhong |
+-----+--- +--------+-------+-----=+--------+
```