# Basic SQL

After DingoDB is installed and deployed successfully, because DingoDB is compatible with MySQL semantics, you can connect to DingoDB and execute SQL statements through SQL tools or MySQL client.

## 1. Create Table

### Parameter Description
|parameter|description|details|syntax|
|:-:|:-|:-|:-|
|Table Name|Table Name|The name of the table, which must be unique in the database and follow the naming convention. |Table table_name|
|Replica|Number of copies|The number of copies of the table. |Replica = {replica}|
|Autoincrement |Autoincrement Fields|Specifies that a field is a self-incrementing column, currently only the `int` type is supported, and you can set the initial self-increment value. | Auto_increment|
|Partition By|Range Partition: partition by range; |Partition the data into different partitions based on the specified range. You can partition by range values for one or more columns. ||Partition by range values (1,100),(101,200);|
||Hash partition: partition by Key's hash value;|Partition by hash value of a column or multiple columns, each hash value determines the partition to which the data belongs. ||Engine
|Engine|Storage Engine | The storage engine specified when creating the table; Dingo provides LSM, BTree, Txn_LSM, Txn_BTree storage engines. The default is: Txn_LSM|Engine = Txn_LSM;|
|Index|Vector Index|Creates an index on a vector field, the index type should be specified as `VECTOR`. |INDEX[index_name ]VECTOR( {index_parameters} )|
||Scalar Index|Index created on a scalar field, default index type is scalar, `Scalar` is optional. ||INDEX[ index_name ] (SCALAR)( {index_parameters} )||

### SQL Example

* Create a table with a scalar index
```sql
CREATE TABLE saltable (
    id int,
    name varchar(32),
    age int,
    gmt bigint,
    price float,
    birthday date,
    create_time time,
    update_time timestamp,
    zip_code varchar(20),
    is_delete boolean,
    index name_index (name), //scalar index
    PRIMARY KEY (id)
);
```
* Create table with vector indexes
```sql
CREATE TABLE vectable (
    id bigint not null, 
    name varchar(32),
    age int,
    amount double,
    birthday date,
    feature float array not null, //vector column
    feature_id bigint not null,
    index feature_index vector(feature_id, feature) parameters(type=hnsw, metricType=L2, dimension=8, efConstruction=40, nlinks=32),//vector index
    PRIMARY KEY(id)
);
```
*Description:*

Create statement in the feature column vector column, type for the floating-point array, vector columns need to be added after the vector index, if you do not add the vector index will be treated as an ordinary array type, vector index in the definition of the scalar index is similar to that of the corresponding definition of the method to use VECTOR, the need for the parameters have two a vector id column, currently only supports integer, the second parameter is the vector column, the second parameter is the vector column, the vector column. Currently only floating point arrays are supported.

## 2. Insert Data
* Take 'vectable' as an example, because the vector field dimension is specified as 8, so the array array needs to contain 8 elements when inserting.
```sql
INSERT INTO vectable VALUES
(1,'Alice',15,98.50,'1995-01-01',array[0.19151945412158966, 0.6221087574958801, 0.43772774934768677, 0.7853586077690125, 0.7799758315086365, 0.10310444235801697, 0.8023741841316223, 0.9455532431602478],1),
(2,'Ben',15,99.00,'1995-03-15',array[0.3833174407482147, 0.053873684257268906, 0.45164841413497925, 0.9820047616958618, 0.12394270300865173,0.2764642536640167, 0.801872193813324, 0.9581393599510193],2),
(3,'Joy',15,100.00,'1996-11-12',array[0.07534254342317581, 0.055006396025419235, 0.32319480180740356, 0.5904818177223206, 0.8538985848426819,0.5524689555168152, 0.2730432450771332, 0.9744951128959656],3),
(4,'Jason',17,97.45,'1994-05-11',array[0.17446526885032654, 0.7370864748954773, 0.1270293891429901, 0.36964988708496094, 0.6043339967727661, 0.35782694816589355, 0.2128199338912964, 0.22331921756267548],4),
(5,'Lily',16,99.50,'1997-05-19',array[0.18726634979248047, 0.797411322593689, 0.6123674511909485, 0.5556533932685852, 0.6294915676116943, 0.686180055141449, 0.24038253724575043,0.038164183497428894],5);
```
## 3. Query Data
* Full table query
```sql
SELECT * FROM vectable.
```
* Conditional query, use `WHERE` conditional statement to filter the data conditionally.
```sql
SELECT * FROM Vectable WHERE name='Ben';
```
## 4. Modify Data
* Use `UPDATE TABLE` statement to modify the data in the table
```sql
UPDATE vectable SET amount=95 WHERE id = 3; ```sql
```
## 5. Drop Table
* Use the `DROP TABLE` statement to delete a table.
```sql
DROP TABLE vectable;
```

