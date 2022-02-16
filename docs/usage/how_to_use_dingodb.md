# How to Use DingoDB?

## JDBC Driver

### Connect DingoDB Using JDBC Driver

Currently, dingodb supports JDBC driver and sqlline side modes. Before starting, first, compile the project to generate the corresponding jar package. In Gradle - > execute Gradle task, execute the following command:
```shell
gradle dingo-cli:build
```

- Start JDBC Proxy:  
  The current JDBC mode uses the thin client mode. After receiving the SQL request, the corresponding request is routed to the back-end execution engine Executor. The driver is generated in previsous step. To start a JDBC proxy can use such command:  
```shell
  java -cp libs/dingo-cli-all.jar io.dingodb.cli.Tools driver --config conf/config-driver.yaml
```
  Configuration file, config-driver.yaml：  
```
  client:
     coordinator:
         servers: 172.20.3.200:19181,172.20.3.201:19181,172.20.3.202:19181
     executor:
         servers: 172.20.3.200:19191,172.20.3.201:19191,172.20.3.202:19191
  instance.host: 172.20.3.200 
```

- Connect to DingoDB
  
  After starting the server, you can connect to the dingodb database in the code：
```
Class.forName("io.dingodb.driver.client.DingoDriverClient");
Connection connection = DriverManager.getConnection("jdbc:dingo:thin:url=http://172.20.3.200:8765");
```
Connection string：
```
jdbc:dingo:thin:url=http://172.20.3.200:8765
```

**The IP address should be the actual server IP address, and the default port is 8765.**

### Read and Write Using JDBC

- Create table
```
Statement statement = connection.createStatement();
String sql = "create table exampleTest ("
            + "id int,"
            + "name varchar(32) not null,"
            + "age int,"
            + "amount double,"
            + "primary key(id)"
            + ")";
statement.execute(sql);
```

- Insert data
```
Statement statement = connection.createStatement();
String sql = "insert into exampleTest values (1, 'example001', 19, 11.0)";
int count = statement.executeUpdate(sql);
log.info("Insert data count = [{}]", count);
```

### Query data

```
Statement statement = connection.createStatement();
String sql = "select * from exampleTest";
try (ResultSet resultSet = statement.executeQuery(sql)) {
  while (resultSet.next()) {
    // iterator the result set
	}
}
```


## Sqlline Mode

Execute directly on the server```./bin/sqlline.sh```to start to sqlline. 

### Examples by Sqlline

- Create data table  
```sql
CREATE TABLE table_name (column_name column_type);
```
- Delete data table  
  
```sql
DROP TABLE table_name ;
```

- Insert data 

```sql
INSERT INTO table_name ( field1, field2,...fieldN) VALUES( value1, value2,...valueN );
```

- Query data  
  
```sql
SELECT column_name,column_name FROM table_name [WHERE Clause] [LIMIT N]
```

- Update data  
  
```sql
UPDATE table_name SET field1=new-value1, field2=new-value2 [WHERE Clause]
```

- Delete data  
  
```sql
DELETE FROM table_name [WHERE Clause]
```

### Aggregation Functions

- avg
- count
- sum
- max
- min

