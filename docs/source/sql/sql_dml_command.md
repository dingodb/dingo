# DML Commands
DML statements are used to manipulate business-related records.
## 1. Insert
- executing a single multi-row INSERT statement is faster than multiple single-row INSERT statements. So when a large amount of data needs to be inserted into a table, it is recommended to use a multi-row INSERT statement.
- he IMPORT statement performs better than the INSERT statement when the amount of data is larger.
#### Insert a single row
```
INSERT INTO student values (1,'lisa',25,92,'class_03','2000-02-23');
0: jdbc:dingo::///> select * from student;
+----+------+-----+--------+----------+------------+
| ID | NAME | AGE | AMOUNT |  CLASS   |    BIR     |
+----+------+-----+--------+----------+------------+
| 1  | lisa | 25  | 92.0   | class_03 | 2000-02-23 |
+----+------+-----+--------+----------+------------+
1 row selected (0.042 seconds)
```
#### Insert multiple rows
```
INSERT INTO student values 
(2,'lisa',25,92,'class_03','2000-02-23'),
(3,'alice',18,99,'class_01','2003-02-23'),
(4,'Ben',19,90,'class_03','2002-01-11'),
(5,'Joy',18,99.5,'class_03','2003-11-25'),
(6,'Susan',19,98.5,'class_02','2003-02-23');
0: jdbc:dingo::///> select * from student;
+----+-------+-----+--------+----------+------------+
| ID | NAME  | AGE | AMOUNT |  CLASS   |    BIR     |
+----+-------+-----+--------+----002D-----+------------+
| 1  | lisa  | 25  | 92.0   | class_03 | 2000-02-23 |
| 2  | lisa  | 25  | 92.0   | class_03 | 2000-02-23 |
| 3  | alice | 18  | 99.0   | class_01 | 2003-02-23 |
| 4  | Ben   | 19  | 90.0   | class_03 | 2002-01-11 |
| 5  | Joy   | 18  | 99.5   | class_03 | 2003-11-25 |
| 6  | Susan | 19  | 98.5   | class_02 | 2003-02-23 |
+----+-------+-----+--------+----------+------------+
6 rows selected (0.036 seconds)
```
#### Insert vector data
```
insert into demo2 values
(1,'OQlLm3',1,8683.31,'1985-11-16',array[0.19151945412158966, 0.6221087574958801, 0.43772774934768677, 0.7853586077690125, 0.7799758315086365, 0.27259260416030884, 0.2764642536640167, 0.801872193813324],1),
(2,'jEBsNq',20,1724.91,'1997-04-09',array[0.959139347076416, 0.8759326338768005, 0.35781726241111755, 0.5009950995445251, 0.683462917804718, 0.7127020359039307, 0.37025076150894165, 0.5611962080001831],2),
(3,'9A0RTh',89,1249.61,'2006-11-18',array[0.5050831437110901, 0.013768449425697327, 0.772826611995697, 0.8826411962509155, 0.36488598585128784, 0.6153962016105652, 0.07538124173879623, 0.3688240051269531],3);
```
## 2. Delete
#### Delete specified data
```
DELETE FROM student WHERE id = 3;
0: jdbc:dingo::///> select * from student;
+----+-------+-----+--------+----------+------------+
| ID | NAME  | AGE | AMOUNT |  CLASS   |    BIR     |
+----+-------+-----+--------+----------+------------+
| 1  | lisa  | 25  | 92.0   | class_03 | 2000-02-23 |
| 2  | lisa  | 25  | 92.0   | class_03 | 2000-02-23 |
| 4  | Ben   | 19  | 90.0   | class_03 | 2002-01-11 |
| 5  | Joy   | 18  | 99.5   | class_03 | 2003-11-25 |
| 6  | Susan | 19  | 98.5   | class_02 | 2003-02-23 |
+----+-------+-----+--------+----------+------------+
5 rows selected (0.046 seconds)
```
#### Delete all data
```
DELETE FROM student;
0: jdbc:dingo::///> select * from student;
+----+------+-----+--------+-------+-----+
| ID | NAME | AGE | AMOUNT | CLASS | BIR |
+----+------+-----+--------+-------+-----+
No rows selected (0.037 seconds)
```
## 3. Update
#### Update specified data
```
UPDATE student SET name = 'aaa' WHERE id = 5;
0: jdbc:dingo::///> select * from student where id =5;
+----+------+-----+--------+----------+------------+
| ID | NAME | AGE | AMOUNT |  CLASS   |    BIR     |
+----+------+-----+--------+----------+------------+
| 5  | aaa  | 18  | 99.5   | class_03 | 2003-11-25 |
+----+------+-----+--------+----------+------------+
1 row selected (0.244 seconds)
```