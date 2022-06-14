# Join

## Inner Join

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

## Inner Join (non-equivalent)

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

## Full join

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

## Cross join

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

## Left join

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

## Right join

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

