# DQL

数据查询语言（Data Query Language），主要是从数据库中检索或查询数据。

## 一、传统SQL查询
这类查询通常用于处理结构化数据。这些查询操作可以直接应用于表结构，对表中的行和列进行操作。
### 1、全表查询
查询数据表中的全部数据纪录，不添加任何查询条件，也不对数据记录进行排序。
```sql
select * from table_name;
```
### 2、查询指定列
在`select`字段后添加想要查看的列，可以是单列也可以是多列，多列时使用`,`分隔。

```sql
select fields from table;
```
### 3、Where条件过滤
在`WHERE`子句中，筛选条件被精心设定，用于确定哪些数据应被包含在查询结果中。这些条件基于表中的列，通过运用比较运算符（如=、<>、>、<、>=、<=以及模糊匹配Like等）来制定。当需要同时满足多个条件时，可以通过逻辑运算符（如And、Or、Not）灵活地组合这些条件，以实现更精确的数据筛选。
#### 1）单一条件过滤
单一条件过滤指在`Where`子句中只指定一个条件来筛选纪录
```sql
Select * from table where condition；
```
示例：
```sql
Select * from table where name='alice';
```
#### 2）多个条件过滤
在`WHERE`子句后指定多个条件来筛选纪录。条件可以基于表中的不同列，使用逻辑运算符（如AND、OR、NOT）来组合。
```sql
select * from table where condition1 AND|OR condition2 [NOT] conditionN；
```
### 4、GROUP BY查询
分组查询通常结合聚合函数（如COUNT(),MAX(),MIN(),SUM(),AVG()等）使用，对一组纪录进行分组，并对每个分组进行聚合计算。
```sql
SELECT column_name, COUNT(*)  
FROM table_name  
GROUP BY column_name;
```
**注意:**
- Select列表时，`HAVING`子句或者`ORDER BY`子句中使用列时，如果这些列没有包含在聚合函数中，那么它们必须出现在`GROUP BY`子句中；
- `GROUP BY`可以在没有聚合函数的情况下使用，但通常是与聚合函数一起使用；
- `GROUP BY`的执行顺序在`WHERE`子句之后，`ORDER BY`和`LIMIT`之前；
- 使用`GROUP BY`时，可以选择对结果集进行排序

### 5、ORDER BY查询
使用`ORDER BY`子句对查询结果进行排序，可以在句末添加`ASC`或`DESC`来指定排序顺序，默认为升序`ASC`。

```sql
SELECT *  
FROM table_name  
ORDER BY column_name DESC; 
```
### 6、HAVING 查询
使用`HAVING`字句对分组后的结果进行过滤。
```sql
SELECT column_name, COUNT(*)  
FROM table_name  
GROUP BY column_name  
HAVING COUNT(*) > 10;
```
### 7、SUBQUERY 子查询
在一个查询中嵌套另一个查询。
```sql
SELECT *  
FROM table1  
WHERE column_name IN (SELECT column FROM table2);
```
### 8、UNION 联合查询
使用`UNION`操作符将两个或多个`SELECT`语句的结果集组合成一个结果集；每个`SELECT`语句必须选择相同数量的列，并且被选择列的数据类型需要相互箭筒。
```sql
SELECT column_name FROM table1  
UNION  
SELECT column_name FROM table2;
```
### 9、LIMIT 查询
用于限制返回的行数。`LIMIT`关键字决定返回行数，`OFFSET`关键字指从第几条开始输出。
```sql
SELECT *  
FROM table_name  
LIMIT 10 OFFSET 20; 
```
### 10、LIKE 模糊查询
在`WHERE`子句中使用`LIKE`进行模糊匹配，具体用法如下：
```sql
SELECT * FROM table_name WHERE colume LIKE '%a'
```

> #### 1）常见用法
> - `LIKE` ‘指定字符串’：用于搜索匹配字段中的指定内容。
> - `NOT LIKE` ‘指定字符串’：用于搜索字段中的内容与指定字符串不匹配的数据。
> - `LIKE BINARY` ‘指定字符串’：BINARY用于区分大小写，LIKE关键字默认忽略大小写，如果只需要搜索A字符，使用BINARY关键字执行搜索命令。
>#### 2）通配符
> - `%`：最常用的通配符，代表任何长度的字符串，字符串的长度可以为0；%可以在 ' '内任意位置使用。*详细使用：*
>   - 查询以指定字符串开头的值 ：LIKE  '指定字符串%'；
>   - 查询以指定字符串结束的值：LIKE  '%指定字符串；
>   - 查询含指定字符串的值：LIKE  '%指定字符串%'；
>   - 查询指定中间字符串的值：LIKE '指定字符串%指定字符串'；
>   - 查询指定字符串的值：Like '指定字符串%指定字符串%指定字符串'。
> - `_`：只能代表单个字符，字符的长度不能为0；通配符_一般在特定情况下使用，需要多少位就有几个通配符，可以与%通配符组合使用。*详细使用：*
>   - LIKE  '%指定字符串_指定字符串%'
>- `[ ]`：表示括号内所列字符中的一个。指定一个字符、字符串或范围，要求所匹配对象为它们中的任一个。*详细使用：*
>   - LIKE '[指定字符串]指定字符串'
> - `[^]`：表示不在括号所列之内的单个字符。取值与[ ]相同，但它要求所匹配对象为指定字符意外的任一个。*详细使用：*
>   - LIKE '[^指定字符串]指定字符串'
>#### 3）注意
>  如果模糊匹配的字符串包含一些正则符号（比如`.` `?` `*` `+`），需要用转义字符`\`进行转义。

### 11、AS 别名
为表或列指定别名，以便在查询中简化引用。
```sql
SELECT column_name AS alias_name  
FROM table_name AS alias_table;
```
### 12、DISTINCT 去重查询
使用`DISTINCT`关键字查询表中不重复的值。
```sql
SELECT DISTINCT column_name FROM table_name;
```
### 13、CASE 子句查询
用于在查询中使用条件逻辑。
```sql
SELECT column_name,  
       CASE  
           WHEN column_name = 'value1' THEN 'Result 1'  
           WHEN column_name = 'value2' THEN 'Result 2'  
           ELSE 'Other Result'  
       END AS result  
FROM table_name;
```
### 14、JOIN 连接查询
#### 1）INNER JOIN 内连接
返回满足连接条件的所有纪录。
```sql
SELECT A.column, B.column  
FROM table1 A  
INNER JOIN table2 B ON A.key = B.key;  
```
#### 2）LEFT JOIN 左外连接
返回左表中的所有纪录与由右表中匹配的数据纪录。
```sql
SELECT A.column, B.column  
FROM table1 A  
LEFT JOIN table2 B ON A.key = B.key;
```
#### 3）RIGHT JOIN 右外链接
返回右表中的所有记录与左表中匹配的数据纪录。
```sql
SELECT A.column, B.column  
FROM table1 A  
RIGHT JOIN table2 B ON A.key = B.key;
```
#### 4）FULL JOIN 全连接
返回被连接表的所有纪录，结合左外连接和右外连接的结果。
```sql
SELECT A.column, B.column
FROM table1 A  
FULL JOIN table2 B ON A.key = B.key;
```
#### 5）CROSS JOIN 交叉连接
返回被连接两个表的笛卡尔积。
```sql
SELECT A.column, B.column 
FROM table1 A  
Right JOIN table2 B;
```
## 二、向量查询
向量查询通常用于处理非结构化或半结构化数据，这些数据常常以数组形式来表示。
### 1、向量索引查询
使用`VECTOR()`函数执行向量数据检索。
```sql
SELECT
* 
FROM
VECTOR(TABLE_NAME, VECINDEX, ARRAY[], N, MAP[]);   
```
#### VECTOR函数说明

|参数|说明|是否必填|
|:-|:-|:-|
|TABLE_NAME|表名|是|
|VECINDEX|向量索引列|是|
|N|查询的数据行数|是|
|ARRAY[ ]|指要查询的向量|是|
|MAP[ ]|附加参数，依据不同索引类型，map中参数也不同。<br>  FLAT：无此参数； <br>  HNSW：map[efsearch，40] <br>  IVF_PQ：map[nprobe,20] <br>  IVF_FLAT：map[nprobe,20]|除FLAT索引外，其他索引类型必填|

> *Map[ ]详解：*
> - IVF_FLAT:
>    - nprobe : 查询时给定的查询深度。值越大召回率越高，但相应的查询时间越长。nprobe  =0， 默认为80。【必填】
>   - parallel_on_queries：是否启用并行查询。这是设置为1 即可。默认也是并行查询。 parallel_on_queries=1。【可选】
> 
>     示例：map[nprobe, 2, parallel_on_queries,1]
>- IVF_PQ
>   - nprobe : 查询时给定的查询深度。值越大召回率越高，但相应的查询时间越长。nprobe  =0， 默认为80。【必填】
>    - parallel_on_queries：是否启用并行查询。这是设置为1 即可。默认也是并行查询。 parallel_on_queries=1。【可选】
>    - recall_num  ： 未使用 。使用默认值 recall_num = 0. 即可。【可选】
>    
>      示例:map[nprobe, 2, parallel_on_queries, 1, recall_num,0]

### 2、前置过滤查询
前置过滤查询时使用`/*+ vector_pre */ `关键字来执行前置查询，即先通过标量字段对全表过滤出结果集，再在此结果集的基础上进行向量查询。

```sql
select /*+ vector_pre */ column(s) //查询字段 
from 
vector(table_name, vecIndex, array[], n)
where name='alice';
```
### 3、后置过滤查询
即先进行向量查询出结果集，再在此结果集的基础上进行标量字段过滤。

```sql
select 
column(s)
from 
vector(table_name, vecIndex, array[], n) 
where name='alice';
```