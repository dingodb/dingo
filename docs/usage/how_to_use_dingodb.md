# How to Use DingoDB

DingoDB is a real-time Hybrid Serving & Analytical Processing (HSAP) Database. It provide three ways to access to DingoDB.

## JDBC Driver

In DingoDB cluster, A JDBC Driver proxy should be start, it will pass the SQL query to `Executor`.

```shell
./bin/start-executor.sh
```

The usage about SQL can be reference to [Play with DingoDB using SQL](./play_with_dingo_using_sql.md)


## Sqlline Mode

* DingoDB

    In DingoDB install path, you can start a sqlline to build connection to the cluster.

```shell
   ./bin/sqlline.sh host root
```
![DingoDB Sqlline](../images/dingoDB_sqlline.png)

* MySQL

    In MySQL install path, you can start a sqlline to build connection to the cluster.
```shell
mysql -h ip -P 3307 -u root -p ******
```
![MySQL Shell](../images/mysql_shell.png)

## Using Key-Value SDK

DingoDB is hybird Serving & Analytical Processing (HSAP) Database. In order to be more faster, it presents a Java API which is comprehensive and powerful to do operations on the database, such as DDL or DML operation. The detail about the SDK client can be found in [Play with dingo using SDK](./play_with_dingo_using_dingoclient.md).
