# DQL Commands
DQL are used to retrieve or query data from the database.
### 1. Full table query
```
0: jdbc:dingo::///> select * from student;
+----+-------+-----+--------+----------+------------+
| ID | NAME  | AGE | AMOUNT |  CLASS   |    BIR     |
+----+-------+-----+--------+----------+------------+
| 1  | lily  | 25  | 92.0   | class_03 | 2000-02-23 |
| 2  | lisa  | 25  | 92.0   | class_03 | 2000-02-23 |
| 3  | laice | 18  | 99.0   | class_01 | 2003-02-23 |
| 4  | Ben   | 19  | 90.0   | class_03 | 2002-01-11 |
| 5  | aaa   | 18  | 99.5   | class_03 | 2003-11-25 |
| 6  | Susan | 19  | 98.5   | class_02 | 2003-02-23 |
| 7  | nancy | 18  | 99.0   | class_01 | 2003-02-23 |
| 8  | Bob   | 19  | 90.0   | class_03 | 2002-01-11 |
| 9  | Jan   | 18  | 99.5   | class_03 | 2003-11-25 |
| 10 | Marry | 19  | 98.5   | class_02 | 2003-02-23 |
+----+-------+-----+--------+----------+------------+
10 rows selected (0.072 seconds)
```
### 2. Vector index query
```
select 
* 
from
vector(test, feature, array[0.5,0.6,0.3], 10, map[efSearch, 1]); 
```
In the example, `test` is the table name, `feature` is the vector index, `array` is the vector to be queried, `10` is the number of rows to be queried, and `map` is an additional parameter to the query (e.g., `efSearch` for hnsw).
### 3. Full-text index query 
- New `text_search` function for full-text indexing `bm25` recall;
- `Text_search` parameter description:
- The first parameter is the table name;
- the second parameter is the full-text index name.
- The third parameter is the expression used for retrieval, which needs to meet the `Tantivy` requirements, please note that whether the expression is legal or not, is checked by `Tantivy`. (If the third parameter is not written, it defaults to a null character.)
- The fourth argument is similar to the vector's `top 5`, returning the top 5 rows with the highest scores. (If the four parameters are left out, the default is 1.)
- Note: The third parameter retrieves the expression used to retrieve multiple clauses is when the logical operations `OR`, `AND` will be used, which must be in uppercase. If it is lowercase, there will be different semantics. tantivy will be treated as multiple words of the corresponding field to match, rather than multiple clauses to match.
```
select description, category, rating, text_index$rank_bm25 from text_search(test, text_index, '(description:keyboard OR category:electronics) AND rating:>2', 5) order by test_index$rank_bm25 desc;
```
### 4. Multi-way recall query
- Provides the `hybrid_search` function, which implements a hybrid search of full-text indexes and vectors;
- The first argument is a full-text index query;
- the second argument is a vector index query;
- The third parameter is the weight of the full-text indexed search;
- The fourth parameter is the weight of the vector search.
(If the third and fourth parameters are omitted, the default is 0.5.)
- The `similarity_limit_n`, `bm25_limit_n`, Dingo is specified by the `top` parameter in the corresponding function.
```
SELECT m.description, m.category, m.embedding, m.rating, s.rank_hybrid
FROM mock_items m
LEFT JOIN (
    SELECT * FROM hybrid_search(
        text_search(test, text_index, '(description:keyboard OR category:electronics) AND rating:>2', 5),
         vector(test, feature, array[0.8894774317741394, 0.7277960181236267, 0.692345142364502, 0.47235092520713806, 0.8568729162216187,            0.6647433042526245, 0.3333759307861328, 0.5181455016136169], 5),
        0.9,
        0.1
    )
) s
ON m.id = s.id where m.rating > 4
LIMIT 5;
```
### 5. Index-front filtering query
First filter the result set by scalar fields for the whole table, then vector query on the basis of this result set.
```
select /*+ vector_pre */ id,name,feature_id,feature_index$distance //query field
from 
vector(demo3, feature, array[0.8894774317741394, 0.7277960181236267, 0.692345142364502, 0.47235092520713806, 0.8568729162216187, 0.6647433042526245, 0.3333759307861328, 0.5181455016136169], 3) 
where name='we' 
order by feature_index$distance 
limit 3;
```
### 6. Index backward filtering query
Advanced row vector query to produce a result set, then scalar field filtering on the basis of this result set.
```
select 
id,name,feature_id,feature_index$distance 
from 
vector(demo3, feature, array[0.8894774317741394, 0.7277960181236267, 0.692345142364502, 0.47235092520713806, 0.8568729162216187, 0.6647433042526245, 0.3333759307861328, 0.5181455016136169], 5) 
where name='' 
order by feature_index$distance 
limit 5;
```
### 7. Query specified columns
```
0: jdbc:dingo::///> select name from student;
+-------+
| NAME  |
+-------+
| lily  |
| lisa  |
| laice |
| Ben   |
| aaa   |
| Susan |
| nancy |
| Bob   |
| Jan   |
| Marry |
+-------+
10 rows selected (0.122 seconds)
```
### 8. Conditional Filtering Query
Single Condition Filtering
```
0: jdbc:dingo::///>select * from student where class='class_03';
+----+------+-----+--------+----------+------------+
| ID | NAME | AGE | AMOUNT |  CLASS   |    BIR     |
+----+------+-----+--------+----------+------------+
| 1  | lily | 25  | 92.0   | class_03 | 2000-02-23 |
| 2  | lisa | 25  | 92.0   | class_03 | 2000-02-23 |
| 4  | Ben  | 19  | 90.0   | class_03 | 2002-01-11 |
| 5  | Joy  | 18  | 99.5   | class_03 | 2003-11-25 |
| 8  | Bob  | 19  | 90.0   | class_03 | 2002-01-11 |
| 9  | Jan  | 18  | 99.5   | class_03 | 2003-11-25 |
+----+------+-----+--------+----------+------------+
6 rows selected (0.116 seconds)
```
Multiple Condition Filtering
```
0: jdbc:dingo::///> select * from student where class='class_03' and age=19;
+----+------+-----+--------+----------+------------+
| ID | NAME | AGE | AMOUNT |  CLASS   |    BIR     |
+----+------+-----+--------+----------+------------+
| 4  | Ben  | 19  | 90.0   | class_03 | 2002-01-11 |
| 8  | Bob  | 19  | 90.0   | class_03 | 2002-01-11 |
+----+------+-----+--------+----------+------------+
2 rows selected (0.083 seconds)
```
### 9. De-duplication query
De-duplication query for the specified columns
```
0: jdbc:dingo::///> select distinct age from student;
+-----+
| AGE |
+-----+
| 18  |
| 19  |
| 25  |
+-----+
3 rows selected (0.545 seconds)
```
### 10. LIKE
#### Common Usage
- LIKE 'specified string': used to search for matching the specified content in the field.
- NOT LIKE 'specified string': used to search for the content of the field does not match the specified string of data.
- LIKE BINARY 'specified string': BINARY is used for case-sensitive, LIKE keyword ignores case by default, if you only need to search for A characters, use the BINARY keyword to execute the search command.
#### wildcard
- %: the most commonly used wildcard character, representing any length of the string, the length of the string can be 0; % can be used in any position within the ' '.
Example: 
Query the value of the beginning of the specified string: LIKE '% of the specified string'.
LIKE '%SpecifiedString' for values ending with the specified string.
LIKE '%SpecifiedString%' for values containing the specified string.
Query the value of a specified intermediate string: LIKE '%specified string'
Query the value of a specified string: LIKE '%specified string%specified string%specified string'
- _: can only represent a single character, the length of the character can not be 0; wildcard _ is generally used in specific cases, how many bits are needed to have a few wildcards, can be used in combination with the % wildcard.
Syntax: LIKE '%specified string_specified string%'
- [ ]: Indicates one of the characters listed in parentheses. Specifies a character, string, or range that requires the match to be any one of them.
Syntax: LIKE '[specified string]specified string'
Example: select * from table where name like '[Zhang Li Wang] San' ; Zhang San, Li San, Wang San
- [^]: Indicates a single character that is not included in the parentheses. The value is the same as [ ], but it requires the matched object to be any one of the specified characters.
Syntax: LIKE '[^specified string]specified string'
Example: select * from table where name like '[^ZhangLiWang]San'; LiuSan, YangSan 
#### Remarks
If the fuzzy match string contains some regular symbols (e.g.. ? * +), you need to escape with the escape character “\”.
#### Sample
```
0: jdbc:dingo::///> select * from STUDENT;
+----+-------+-----+--------+----------+-----------------------+
| ID | NAME| AGE | AMOUNT |  CLASS   |     BIR          |
+----+-------+-----+--------+----------+---------------+
| 1  | lisa | 25  | 92.0| class_03 | 2000-02-23 18:00:00.0 |
| 2  | alice| 18 | 99.0 | class_01 | 2003-02-23 17:00:00.0 |
| 3  | Ben  | 19 | 90.0 | class_03 | 2002-01-11 16:00:00.0 |
| 4  | Joy  | 18 | 99.5 | class_03 | 2003-11-25 23:00:00.0 |
| 5  | Susan | 19 | 98.5| class_02 | 2003-02-23 23:05:00.0 |
+----+-------+-----+--------+----------+----------------+
5 rows selected (0.494 seconds)
0: jdbc:dingo::///> select * from student where name like 'e%';
+----+------+-----+--------+-------+-----+
| ID | NAME | AGE | AMOUNT | CLASS | BIR |
+----+------+-----+--------+-------+-----+
No rows selected (0.529 seconds)
0: jdbc:dingo::///> select * from student where name like '%e';
+----+-------+-----+------+----------+-----------------------+
| ID | NAME  | AGE |AMOUNT|  CLASS   |          BIR          |
+----+-------+-----+------+----------+-----------------------+
| 2  | alice | 18  | 99.0 | class_01 | 2003-02-23 17:00:00.0 |
+----+-------+-----+------+----------+-----------------------+
1 row selected (0.052 seconds)
0: jdbc:dingo::///> select * from student where name like '%e%';
+----+-------+-----+------+----------+-----------------------+
| ID | NAME  | AGE |AMOUNT|  CLASS   |          BIR          |
+----+-------+-----+------+----------+-----------------------+
| 2  | alice | 18  | 99.0 | class_01 | 2003-02-23 17:00:00.0 |
| 3  | Ben   | 19  | 90.0 | class_03 | 2002-01-11 16:00:00.0 |
+----+-------+-----+------+----------+-----------------------+
2 rows selected (0.04 seconds)
0: jdbc:dingo::///> select * from student where name like 'b%';
+----+------+-----+------+----------+-----------------------+
| ID | NAME | AGE |AMOUNT|  CLASS   |          BIR          |
+----+------+-----+------+----------+-----------------------+
| 3  | Ben  | 19  | 90.0 | class_03 | 2002-01-11 16:00:00.0 |
+----+------+-----+------+----------+-----------------------+
1 row selected (0.037 seconds)
```
### 11. GROUP BY
```
0: jdbc:dingo::///> select class,count(amount) from student group by class;
```