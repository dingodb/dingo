# Play with DingoDB using DingoClient

## Introducation

DingoDB is a distributed real-time multi-modal database. In order to be more faster, it presents a Java API which is comprehensive and powerful to do operations on the database, such as DDL or DML operation.

## Operation using DingoClient

### Dependence about DingoClient

`dingo-sdk` artifactory can be download from maven central using pom directly.

```xml
<dependency>
    <groupId>io.dingodb</groupId>
    <artifactId>dingo-client</artifactId>
    <version>0.7.0</version>
</dependency>
```

### Function about dingo-client

**Function about Dingo API**: [API Documents](https://github.com/dingodb/dingo)

## Examples about dingo-client
Implemented in two ways, as follows:

### Mode One

1.Define table Using Pojo
```java
    import io.dingodb.sdk.annotation.DingoKey;
    import io.dingodb.sdk.annotation.DingoRecord;
    import lombok.Getter;
    import lombok.Setter;
    import lombok.ToString;
    
    @DingoRecord(table = "acct_deal")
    @Getter
    @Setter
    @ToString
    public class AcctDeal {
        @DingoKey
        private String acct_no;
        @DingoKey
        private String deal_date;
    
        private double acc_amount;
        private double acc_count;
        private double deb_amount;
        private double deb_count;
        private double cre_amount;
        private double cre_count;
    }
```
2.Create table Using DingoClient
```java
   String remoteHost = "coordinator:19181";
   DingoClient dingoClient = new DingoClient(remoteHost);
   dingoClient.open();

   DingoOpCli dingoOpCli = new DingoOpCli.Builder(dingoClient).build();
   boolean isOK = dingoOpCli.createTable(AcctDeal.class);
   System.out.println("Create table Status: " + isOK);
```
3.Insert records to table
```java
   AcctDeal acctDeal = new AcctDeal();
   acctDeal.setAcct_no("1001");
   acctDeal.setDeal_date("2022-08-08");
   acctDeal.setAcc_amount(100);
   acctDeal.setAcc_count(10);
   acctDeal.setDeb_amount(50);
   acctDeal.setDeb_count(3);
   acctDeal.setCre_amount(30);
   acctDeal.setCre_count(3);
   dingoOpCli.save(acctDeal);
```
4.Query data
- Query a single record
```java
    Record record = dingoClient.get(tableName, new Key(Value.get(1)));
```
- Query multiple records 1
```java
    List<Record> recordList = dingoClient.get(tableName, Arrays.asList(new Key(Value.get(1)), new Key(Value.get(2)), new Key(Value.get(3))));
```
- Query multiple records 2
```java
    Iterator<Record> iterator = dingoClient.scan(tableName, new Key(Value.get(1)), new Key(Value.get(100)), true, true);
```
5.Sample Data of Table

```sql
    mysql> select * from acc_deal limit 5;
    +--------+------------+------------+-----------+------------+-----------+------------+-----------+
    | ACC_NO | DEAL_DATE  | ACC_AMOUNT | ACC_COUNT | DEB_AMOUNT | DEB_COUNT | CRE_AMOUNT | CRE_COUNT |
    +--------+------------+------------+-----------+------------+-----------+------------+-----------+
    | 1001   | 2022-08-08 | 100        | 5         | 50         | 2         | 20         | 2         |
    | 1002   | 2022-08-09 | 150        | 6         | 60         | 3         | 30         | 3         |
    | 1001   | 2022-08-15 | 200        | 6         | 50         | 4         | 40         | 2         |
    | 1004   | 2022-08-20 | 140        | 4         | 55         | 3         | 70         | 2         |
    | 1001   | 2022-08-25 | 180        | 8         | 60         | 3         | 80         | 4         |
    +--------+------------+------------+-----------+------------+-----------+------------+-----------+
```
```text
    ACCO_NO: Account No--> primary key
    DEAL_DATE: Deal Date--> primary key
    ACC_AMOUNT: Trade amount about account
    ACC_COUNT: Trade count about account
    DEB_AMOUNT: Amount of debit
    DEB_COUNT: Count of debit
    CRE_AMOUNT: Amount of lender
    CRE_COUNT: Count of lender
```
6.Drop table and close connection
```java
   isOK = dingoOpCli.dropTable(AcctDeal.class);
   System.out.println("drop table Status:" + isOK + ".............");

   dingoClient.close();
```
### Mode Two
1.To create a table using Dingo Client
```java
    String tableName = "testThread";

    ColumnDefinition c1 = ColumnDefinition.builder().name("id").type("integer").precision(0).scale(0).nullable(false).primary(0).build();
    ColumnDefinition c2 = ColumnDefinition.builder().name("name").type("varchar").precision(0).scale(0).nullable(false).primary(-1).build();
    ColumnDefinition c3 = ColumnDefinition.builder().name("amount").type("double").precision(0).scale(0).nullable(false).primary(-2).build();

    PartitionDetailDefinition detailDefinition = new PartitionDetailDefinition(null, null, Arrays.asList(new Object[]{30}));
    PartitionRule partitionRule = new PartitionRule(null, null, Arrays.asList(detailDefinition));

    TableDefinition tableDefinition = TableDefinition.builder()
            .name(tableName)
            .columns(Arrays.asList(c1, c2, c3))
            .version(1)
            .ttl(0)
            .partition(partitionRule)
            .engine(Common.Engine.ENG_ROCKSDB.name())
            .build();
    boolean isSuccess = dingoClient.createTable(tableDefinition);
    System.out.println("create is success: " + isSuccess);
```
2.Insert records to table
- Insert a single record
```java
    Record record = new Record(tableDefinition.getColumns(), new Value[]{Value.get(1), Value.get("col1"), Value.get(1234.0)});
    boolean upsert = dingoClient.upsert(tableName, record);
```
- Insert multiple records
```java
    List<Record> records = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
        records.add(new Record(tableDefinition.getColumns(), new Value[]{Value.get(i), Value.get("col" + i), Value.get(123.0 * i)}));
        }
    List<Boolean> upsert = dingoClient.upsert(tableName, records);
```
3.Query data
- Query a single record
```java
    Record record = dingoClient.get(tableName, new Key(Value.get(1)));
```
- Query multiple records 1
```java
    List<Record> recordList = dingoClient.get(tableName, Arrays.asList(new Key(Value.get(1)), new Key(Value.get(2)), new Key(Value.get(3))));
```
- Query multiple records 2
```java
    Iterator<Record> iterator = dingoClient.scan(tableName, new Key(Value.get(1)), new Key(Value.get(100)), true, true);
```
4.Drop table and close connection
```java
    isOK = dingoClient.dropTable(tableName);
    System.out.println("drop table Status:" + isOK + ".............");

    dingoClient.close();
```
## Operation using Annotation

DingoDB presents a Java API which is comprehensive and powerful, but requires a measure of boiler plate code to map the data from Java POJOs to the database. The Annotation is aim to lower the amount of code required when mapping POJOs to DingoDB and back as well as reducing some of the brittleness of the code.

### Keys

The key to an DingoDB record can be specified either as a field or a property. Remember that DingoDB keys can be Strings, numberical types and date types only.

To use a field as the key, simply mark the field with the DingoKey annotation:

```java
    @DingoKey
    private String acct_no;
    @DingoKey
    private String deal_date;
```

### Column

Fields in Java can be mapped to the database irrespective of the visibility of the column. To do so, simply specify the column to map to with the @DingoColumn annotation:

```java
@DingoColumn(name = "vrsn")
private int version;
```

This will map the contents of the `version` field to a `vrsn` column in DingoDB.

If the name of the column matches the name of the field in Java, the name can be omitted:

```Java
@DingoColumn
private int age;
```

This will appear in DingoDB as the `age` column.

By default, all fields will be mapped to the database. Fields can be excluded with the @DingoExclude annotation, and renamed with the @DingoColumn annotation. For example:

```Java
@DingoRecord(table = "testSet")
public static class Test {
	public int a;
	public int b;
	public int c;
	public int d;
}
```
This saves the record with 4 columns, `a`,`b`,`c`,`d`. To save only fields `a`,`b`,`c` you can do either:

```Java
@DingoRecord(table = "testSet")
public static class Test {
	public int a;
	public int b;
	public int c;
	@DingoExclude
	public int d;
}
```

If a field is marked with both @DingoExclude, the column will not be mapped to the database.

You can force the name of a particular column or columns by specifying them in an DingoColumn:

```Java
@DingoRecord(table = "testSet")
public static class Test {
	public int a;
	public int b;
	@DingoColumn(name = "longCname")
	public int c;
	public int d;
}
```

This will save 4 fields in the database, `a`, `b`, `longCname`, `d`.



