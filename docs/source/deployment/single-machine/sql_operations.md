# SQL基本操作

DingoDB安装部署成功后，因为DingoDB兼容MySQL语义，您可以通过SQL工具或者MySQL客户端中连接DingoDB，并执行SQL语句。

## 1、创建表

### 1）参数说明
|参数|说明|详情|语法|
|:-:|:-|:-|:-|
|Table Name|表名|表的名称，必须在数据库中唯一，并遵循命名规则。|Table table_name|
|Replica|副本数|表的副本数量。|Replica = {replica}|
|Autoincrement |自增字段|指定某字段为自增列，目前仅支持 `int` 类型，并可设置自增初始值。| Auto_increment|
|Partition By|Range分区：按范围分区; |根据指定范围将数据划分到不同的分区中。可以根据某个列或多列的值进行范围分区。|Partition by range values (1,100),(101,200);|
||Hash分区：按Key的Hash值分区；|通过对某列或多列的哈希值进行分区，每个哈希值确定数据所属分区。||
|Engine|存储引擎|创建表时指定的存储引擎；Dingo 提供 LSM、BTree、Txn_LSM、Txn_BTree 四种存储引擎。 默认为：Txn_LSM|Engine = Txn_LSM；|
|Index|向量索引|在向量字段上创建索引，索引类型需指定为 `VECTOR`。|INDEX[index_name ]VECTOR( {index_parameters} )|
||标量索引|在标量字段上创建的索引，默认索引类型为标量， `Scalar`可写可不写。|INDEX[ index_name ] (SCALAR)( {index_parameters} )|

### 2）SQL示例

* 创建一张带有标量索引的表；
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
    index name_index (name), //标量索引
    PRIMARY KEY (id)
);
```
* 创建一张带有向量索引的表；
```sql
CREATE TABLE vectable (
    id bigint not null, 
    name varchar(32),
    age int,
    amount double,
    birthday date,
    feature float array not null, //向量列
    feature_id bigint not null,
    index feature_index vector(feature_id, feature) parameters(type=hnsw, metricType=L2, dimension=8, efConstruction=40, nlinks=32),//向量索引
    PRIMARY KEY(id)
);
```
*说明：*

创建语句中feature列为向量列，类型为浮点型数组，有向量列之后需要添加向量索引，如果不加向量索引会当做普通的数组类型，向量索引在定义时与标量索引类似，对应定义的方法使用VECTOR，需要的参数有两个一个是向量的id列，目前只支持整型，第二个参数是向量列，目前只有浮点型数组。

## 2、插入数据
* 这里以‘vectable’为例，因为向量字段维度指定为8，所以插入时array数组中需要包含8个元素。
```sql
INSERT INTO vectable VALUES
(1,'Alice',15,98.50,'1995-01-01',array[0.19151945412158966, 0.6221087574958801, 0.43772774934768677, 0.7853586077690125, 0.7799758315086365, 0.10310444235801697, 0.8023741841316223, 0.9455532431602478],1),
(2,'Ben',15,99.00,'1995-03-15',array[0.3833174407482147, 0.053873684257268906, 0.45164841413497925, 0.9820047616958618, 0.12394270300865173,0.2764642536640167, 0.801872193813324, 0.9581393599510193],2),
(3,'Joy',15,100.00,'1996-11-12',array[0.07534254342317581, 0.055006396025419235, 0.32319480180740356, 0.5904818177223206, 0.8538985848426819,0.5524689555168152, 0.2730432450771332, 0.9744951128959656],3),
(4,'Jason',17,97.45,'1994-05-11',array[0.17446526885032654, 0.7370864748954773, 0.1270293891429901, 0.36964988708496094, 0.6043339967727661, 0.35782694816589355, 0.2128199338912964, 0.22331921756267548],4),
(5,'Lily',16,99.50,'1997-05-19',array[0.18726634979248047, 0.797411322593689, 0.6123674511909485, 0.5556533932685852, 0.6294915676116943, 0.686180055141449, 0.24038253724575043,0.038164183497428894],5);
```
## 3、查询数据
* 全表查询
```sql
SELECT * FROM vectable;
```
* 条件查询，使用`WHERE`条件语句，对数据进行条件筛选
```sql
SELECT * FROM Vectable WHERE name=‘Ben’;
```
## 4、修改数据
使用`UPDATE TABLE`语句对表中数据进行修改
```sql
UPDATE vectable SET amount=95 WHERE id = 3;
```
## 5、删除表
使用 `DROP TABLE`语句删除表
```sql
DROP TABLE vectable;
```

