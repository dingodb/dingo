# DDL

## 一、数据表
### 1、创建表
#### 1.1 参数说明
|参数|说明|详情|语法|
|:-:|:-|:-|:-|
|TableName|表名|要创建的表名称，该名称在数据库中必须是唯一的，并遵循命名规则。|Table table_name|
|Replica|副本数|设置副本数值。|Replica = {replica}|
|AutoIncrement|自增字段|可以指定字段列作为自增列，当前只支持 int 类型的字段列，并且可以指定自增列开始值。|filed field_type auto_increment|
|Default|缺省值|用于为表中的列指定默认值。|Default default_name|
|Not null|非空|用于确保列中的值不能为空。Dingo中Not null 必须跟在字段最末端。|NOT NULL|
|Primary key|主键|用于唯一标识表中每个记录的一列或多列。主键约束强制要求该列或列组合的值在表中是唯一的，且不能为NULL。在Dingo中不能直接跟在字段后设定主键，需要另起一行设置主键。|Priamry Key(field_name)|
|Partition by|分区|Range分区：按范围分区；根据指定范围将数据划分到不同的分区中。可以根据某个列或多列的值进行范围分区。|Partition by range values(1,100),(101,200);|
|||Hash分区：按Key的Hash值分区；通过对某个列或多列的哈希值进行分区。每个哈希值决定了数据所属的分区。|Partition by hash partitions={partitions}|
|Engine|存储引擎|创建表时，可指定对应的存储引擎；Dingo 提供两种存储 引 擎 ： LSM BTree、Txn_LSM、Txn_BTree;系 统 默 认 为 ：Txn_LSM|Engine = {engine_name}|
|Index|索引字段|向量索引：指在向量字段上创建索引，创建索引时需要指定索引类型Vector。|INDEX [index_name] (vector) ({index_parameters}) [WITH ({columns})] (PARAMETERS) (vector_parameters)|
|||标量索引：指在标量字段上创建的索引。创建索引时，默认指定为标量索引，Scalar可写可不写。|INDEX [index_name] (scalar) ({index_parameters}) [WITH ({columns})] (PARAMETERS) (vector_parameters)|

#### 1.2 SQL示例
这里以一张全面的数据表为例，包含所有字段类型、索引、副本等所支持参数。
```sql
CREATE TABLE IF NOT EXISTS TEST_DEMO(
    ID INT AUTO_INCREMENT NOT NULL,
    NAME VARCHAR(32) NOT NULL COMMENT '用户名',
    AGE INT DEFAULT 20,
    GMT BIGINT,
    PRICE FLOAT,
    AMOUNT DOUBLE NOT NULL DEFAULT 0.0,
    ADDRESS VARCHAR(255) DEFAULT 'BEIJING',
    BIRTHDAY DATE,
    CREATE_TIME TIME,
    UPDATE_TIME TIMESTAMP,
    ZIP_CODE VARCHAR(20) DEFAULT NULL,
    IS_DELETE BOOLEAN,
    USER_INFO ANY, # Map字段
    BYTE_ARRAY BINARY NOT NULL, # byte_array字段
    FEATURE1 FLOAT ARRAY NOT NULL, # array字段，array里的元素类型是float
    FEATURE2 FLOAT ARRAY NOT NULL,
    FEATURE1_ID BIGINT NOT NULL,
    FEATURE2_ID BIGINT NOT NULL,
    PRIMARY KEY(ID,BIRTHDAY), # 主键
    INDEX NAME_INDEX(NAME) ENGINE=TXN_LSM, # 标量索引,目前只有事务引擎支持索引
    INDEX AGE_INDEX(AGE) WITH (CREATE_TIME) ENGINE=TXN_LSM PARTITION BY HASH PARTITIONS=5, # 标量索引带有覆盖索引字段（with后面的字段），对索引表按hash分区，指定分区数5
    INDEX GMT_ADDRESS_INDEX(GMT,ADDRESS) WITH (PRICE,UPDATE_TIME) ENGINE=TXN_LSM PARTITION BY RANGE VALUES (10000,'BEIJING'),(1000000,'SHANGHAI') REPLICA=3, # 标量字段带有覆盖索引字段，对索引表按range value分区，且副本数是3
    INDEX FEATURE1_INDEX VECTOR(FEATURE1_ID, FEATURE1) WITH (PRICE) ENGINE=TXN_LSM PARTITION BY HASH PARTITIONS=5 REPLICA=1 PARAMETERS(type=flat, metricType=L2, dimension=8), # 向量索引flat检索算法带有覆盖索引字段，且指定hash分区为5，副本数为3,
    INDEX FEATURE2_INDEX VECTOR(FEATURE2_ID, FEATURE2) ENGINE=TXN_LSM PARTITION BY RANGE VALUES (10),(20) PARAMETERS(type=hnsw, metricType=COSINE, dimension=8, efConstruction=40, nlinks=32) # 向量索引hnsw检索算法并指定range分区
) ENGINE=TXN_LSM partition by range values (10,'1990-01-01'),(20,'2000-01-01'),(30,'2010-01-01') REPLICA=3 DEFAULT CHARSET=utf8 COMMENT='测试demo'; # 指定range分区，副本数为3，也可指定hash分区
```
### 2、删除表
* 语法
```sql
Drop table table_name;
```

## 二、索引
### 1. 语法
```sql
INDEX
[ index_name ]
( VECTOR | [SCALAR] ) ( {index_parameters} )
[ WITH ( {columns} ) ]
( PARAMETERS ) (vector_parameters)
```
### 2. 示例
创建一张向量表为例；
```sql
create table demo2
(
    id bigint not null, 
    name varchar(32),
    age int,
    amount double,
    birthday date,
    feature float array not null, //向量列
    feature_id bigint not null,
    index feature_index vector(feature_id, feature) parameters(type=hnsw, metricType=L2, dimension=8, efConstruction=40, nlinks=32),//向量索引
    primary key(id)
);
```
**说明**

sql中feature列为向量列，类型为浮点型数组，有了向量列之后需要添加向量索引，如果不加向量索引会当做普通的数组类型，向量索引在定义时与标量索引类似，对应定义的方法使用VECTOR，需要的参数有两个一个是向量的id列，目前只支持整型，第二个参数是向量列，目前只支持浮点型数组。

**注意**

向量列与向量ID均不可为null。

## 三、分区
### 1、Range分区
 * 语法
 ```sql
 [ PARTITION BY
  RANGE 
  VALUES(( {partition_values} )) )]
 ```
 * 示例
 ```
 PARTITION BY RANGE VALUES (1,100),(2,100);
 ```
 **说明**
 
 VALUES后面的值表示将有3个分区；分区范围如下：
 - [Infinity，key(1,100));
 - [key(1,100)，key(2,100))；
 - [key(2,100)，Infinity)；
其中，Infinity表示无限；在左边时表示无限小，在右边是表示无限大。

### 2、Hash分区
* 语法
```sql
[ PARTITION BY
  HASH
  PARTITIONS={partitions} ]
```
* 示例
```sql
PARTITION BY HASH PARTITIONS=10,
```
**说明**

partitions代表分区个数；