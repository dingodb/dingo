# Document Index
The relevant syntax for DingoDB document indexing is designed as follows:
## Create Table
```
CREATE TABLE test
(
       id bigint not null,
       feature float array not null, //vector column
       feature_id bigint not null,
       description varchar not null,
       category varchar not null,
       rating bigint not null,
       text_id bigint not null,
       INDEX text_index TEXT(text_id, description, category, rating, text_id) engine=TXN_BTREE PARTITION BY RANGE values(10) parameters(text_fields='{"description": {"tokenizer": {"type": "stem"}}, "category": {"tokenizer": {"type": "stem"}}, "rating": {"tokenizer": {"type": "i64"}}, "text_id": {"tokenizer": {"type": "i64"}}'),// document index
       index feature_index vector(feature_id, feature) parameters(type=hnsw, metricType=L2, dimension=8, efConstruction=40, nlinks=32),//vector index
       primary key(id)
) engine=TXN_LSM;
```

Parameters for full text index when building table:

text_id is required, full-text index unique identification ID, ID > 0, text_id can be feature_id, if it is INT64 type, if you want to index text_id, you need to add one more parameter with the same name in TEXT().

description, category, rating are the fields of full text index, only 4 types are allowed: INT64, DOUBLE, STRING, BYTES, the fields of full text index should include at least two fields, one of the two fields must be text_id.

> Note: text_fields in parameters is the user filled in about the full-text indexed fields of the word splitter parameters, must be full-text indexed fields are filled in.

## Insert
```
insert into test values (1, array[0.19151945412158966, 0.6221087574958801, 0.43772774934768677, 0.7853586077690125, 0.7799758315086365, 0.27259260416030884, 0.2764642536640167, 0.801872193813324], 1, 'Plastic Keyboard', 'Electronics', 4, 1),(2, array[0.959139347076416, 0.8759326338768005, 0.35781726241111755, 0.5009950995445251, 0.683462917804718, 0.7127020359039307, 0.37025076150894165, 0.5611962080001831], 2 , 'Ergonomic metal keyboard', 'Electronics', 4, 2), (3, array[0.5050831437110901, 0.013768449425697327, 0.772826611995697, 0.8826411962509155, 0.36488598585128784, 0.6153962016105652, 0.07538124173879623, 0.3688240051269531], 3, 'Sleek running shoes', 'Footwear', 5, 3), (4, array[0.9361401200294495, 0.6513781547546387, 0.39720258116722107, 0.7887301445007324, 0.3168361186981201, 0.5680986642837524, 0.8691273927688599, 0.4361734092235565], 4, 'Plastic Keyboard', 'Electronics', 4, 4);
```
## Query
#### Sample 1
```
select description, category, rating from text_search(test, test_index, 'description:keyboard', 5) order by rating;

       description        | rating |  category   
--------------------------+--------+-------------
 Plastic Keyboard         |      4 | Electronics
 Ergonomic metal keyboard |      4 | Electronics
(2 rows)
```
New text_search function for full-text index bm25 recall, the first parameter is the table name, the second parameter is the full-text index name, the third parameter is the expression used for retrieval, need to comply with the Tantivy requirements of the expression, please note that whether the expression is legal or not, it is Tantivy check. If the third parameter is not written, the default is a null character. The fourth parameter is similar to the vector's top 5, returning the top 5 rows with the highest scores. If the four parameters are not written, the default is 1.
> Note: The third parameter retrieves the expression used to retrieve multiple clauses is when the logical operation OR, AND will be used, which must be used in uppercase. If it is lowercase, there will be different semantics. tantivy will be treated as multiple words in the corresponding field to match, rather than multiple clauses to match.

#### Sample 2
```
select description, category, rating, text_index$rank_bm25 from text_search(test, text_index, '(description:keyboard OR category:electronics) AND rating:>2', 5) order by test_index$rank_bm25 desc;

         description         | rating |  category   | test_index$rank_bm25  
-----------------------------+--------+-------------+---------------------
  Plastic Keyboard           |      4 | Electronics | 6.3764954
 Ergonomic metal keyboard    |      4 | Electronics |  5.931014
 Innovative wireless earbuds |      5 | Electronics | 3.1096356
 Fast charging power bank    |      4 | Electronics | 3.1096356
 Bluetooth-enabled speaker   |      3 | Electronics | 3.1096356
(5 rows)
```
Get the 5 rows with the highest scores and sort them by rank_bm25 score.

## Multi-way Recall
#### Sample 1
```
select * from hybrid_search(
text_search(test, text_index, '(description:keyboard OR category:electronics) AND rating:>2', 5),
vector(test, feature, array[0.8894774317741394, 0.7277960181236267, 0.692345142364502, 0.47235092520713806, 0.8568729162216187, 0.6647433042526245, 0.3333759307861328, 0.5181455016136169], 5),
0.9,
0.1
);
 id | rank_hybrid
----+-------------
  2 |  0.95714283
  1 |   0.8487012
 29 |         0.1
 39 |         0.1
  9 |         0.1
(5 rows)
```
Support hybrid_search function, realize the full-text index and vector hybrid search, the first parameter is the full-text index query, the second parameter is the vector index query, the third parameter is the weight of the full-text index search, the fourth parameter is the weight of the vector search. The third and fourth parameters if omitted, the default is 0.5. which such as paradeDB in similarity_limit_n, bm25_limit_n, Dingo is through the corresponding function in the top parameter specified.

#### Sample 2
```
SELECT m.description, m.category, m.embedding, s.rank_hybrid
FROM mock_items m
LEFT JOIN (
    SELECT * FROM hybrid_search(
        text_search(test, text_index, '(description:keyboard OR category:electronics) AND rating:>2', 5),
		vector(test, feature, array[0.8894774317741394, 0.7277960181236267, 0.692345142364502, 0.47235092520713806, 0.8568729162216187, 		    0.6647433042526245, 0.3333759307861328, 0.5181455016136169], 5),
		0.9,
		0.1
    )
) s
ON m.id = s.id
LIMIT 5;
       description        |  category   | embedding | rank_hybrid
--------------------------+-------------+-----------+-------------
 Plastic Keyboard         | Electronics | [4,5,6]   |  0.95714283
 Ergonomic metal keyboard | Electronics | [3,4,5]   |   0.8487012
 Designer wall paintings  | Home Decor  | [1,2,3]   |         0.1
 Handcrafted wooden frame | Home Decor  | [1,2,3]   |         0.1
 Modern wall clock        | Home Decor  | [1,2,3]   |         0.1
(5 rows)
```
#### Sample 3
```
SELECT m.description, m.category, m.embedding, m.rating, s.rank_hybrid
FROM mock_items m
LEFT JOIN (
    SELECT * FROM hybrid_search(
        text_search(test, text_index, '(description:keyboard OR category:electronics) AND rating:>2', 5),
		 vector(test, feature, array[0.8894774317741394, 0.7277960181236267, 0.692345142364502, 0.47235092520713806, 0.8568729162216187, 		    0.6647433042526245, 0.3333759307861328, 0.5181455016136169], 5),
		0.9,
		0.1
    )
) s
ON m.id = s.id where m.rating > 4
LIMIT 5;
         description         |  category   | embedding | rating | rank_hybrid 
-----------------------------+-------------+-----------+--------+-------------
 Designer wall paintings     | Home Decor  | [1,2,3]   |      5 |         0.1
 Handcrafted wooden frame    | Home Decor  | [1,2,3]   |      5 |         0.1
 Slim-fit denim jeans        | Apparel     | [3,4,5]   |      5 | 0.071428575
 Soft cotton shirt           | Apparel     | [3,4,5]   |      5 | 0.071428575
 Innovative wireless earbuds | Electronics | [4,5,6]   |      5 | 0.057142857
(5 rows)
```
Support for `Join` in Mixed Checks

> Specific syntax support, you need to modify Dingo related syntax extension file parser.jj, config.fmpp, note that you need to add text_search function parsing.