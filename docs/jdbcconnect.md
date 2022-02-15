# 1. How to use and connect DingoDB?
Currently, dingodb supports JDBC driver and sqlline side modes. Before starting, first, compile the project to generate the corresponding jar package. In Gradle - > execute Gradle task, execute the following command:
````
gradle dingo-cli:build
````
- (Using the jar):  
  The current JDBC mode uses the thin client mode. After receiving the SQL request, the corresponding request is routed to the back-end execution engine Executor. The driver is generated in previsous step. To start a JDBC proxy can use such command:  
  ```java -cp libs/dingo-cli-all.jar io.dingodb.cli.Tools driver --config conf/config-driver.yaml```  
  Configuration file, config-driver.yaml：  
  ```
  client:
     coordinator:
         servers: 172.20.3.200:19181,172.20.3.201:19181,172.20.3.202:19181
     executor:
         servers: 172.20.3.200:19191,172.20.3.201:19191,172.20.3.202:19191
  instance.host: 172.20.3.200 
  ```
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
- Sqlline mode:  
  Execute directly on the server```./bin/sqlline.sh```to start.

# 2. How to read and write data through JDBC?
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

- Query data
```
Statement statement = connection.createStatement();
String sql = "select * from exampleTest";
try (ResultSet resultSet = statement.executeQuery(sql)) {
	ResultSetMetaData metaData = resultSet.getMetaData();
	List<Map<String, String>> result = new ArrayList<>();
    int columnCount = metaData.getColumnCount();
    int line = 1;
    while (resultSet.next()) {
    	Map<String, String> lineMap = new HashMap<>();
        for (int i = 1; i < columnCount + 1; i++) {
        	try {
            	System.out.printf("line = " + line + "   column = " + metaData.getColumnName(i)
                            + "=" + resultSet.getString(i));
                        lineMap.put(metaData.getColumnName(i), resultSet.getString(i));
 			} catch (SQLException e) {
                        System.out.printf(e.getMessage());
            }
         }
         line++;
         result.add(lineMap);
	}
}
```

# 3. Read and write data through sqlline
Common commands:
- Create data table  
  ```CREATE TABLE table_name (column_name column_type);```
- Delete data table  
  ```DROP TABLE table_name ;```
- Insert data  
```INSERT INTO table_name ( field1, field2,...fieldN) VALUES( value1, value2,...valueN );```
                      

- Query data  
```SELECT column_name,column_name FROM table_name [WHERE Clause] [LIMIT N]```

- Update data  
```UPDATE table_name SET field1=new-value1, field2=new-value2 [WHERE Clause]```

- Delete data  
```DELETE FROM table_name [WHERE Clause]```

- Function use  
| Function name | function |  
| ------------- | -------- |  
|     AVG       | Average  |   
|     count     | Find the number of records and return an integer of type int |  
|      max      | Find the maximum value |  
|      min      | Find the minimum value |   
|      sum      | Sum      | 
