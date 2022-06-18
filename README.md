# DingoDB

DingoDB is a real-time Hybrid Serving & Analytical Processing (HSAP) Database. It can execute high-frequency queries and
upsert, interactive analysis, multi-dimensional analysis in extremely low latency.

![](docs/images/dingo_stack.png)

## Key Features

1. Compliant with MySQL-Compatible
   Based on the popular [Apache Calcite](https://calcite.apache.org/) SQL engine, DingoDB can parse, optimize and
   execute standard SQL statements, and is capable to run part of TPC-H and TPC-DS (See [TPC](http://www.tpc.org/))
   queries. DingoDB is also compliant with JDBC and can be seamlessly integrated with web services, BI tools, etc.
2. Support high frequency write operation  
   By using the log-structured key-value storage [RocksDB](https://rocksdb.org/), DingoDB support high frequency write
   operations like INSERT, UPDATE, DELETE.
3. Support point query and multi-dimensional analysis simultaneously  
   DingoDB can store table data in both row-oriented and column-oriented format, providing capability of fast point
   query and fast multi-dimensional analysis in low latency.
4. Easily integrated with streaming data and other DBMS's  
   By providing dedicated APIs for popular streaming data processing engine,
   e.g. [Apache Flink](https://flink.apache.org/), DingoDB can easily accept data from them, and support more analysis
   working or web serving that is not applicable to be done in stream. DingoDB can also access databases of many types,
   using pluggable connectors for each of them.
5. A distributed architecture with flexible and elastic scalability  
   DingoDB stores and processes data in a distributed manner with strong cluster and resource management functionality,
   which make it easy to expand the capacity.
6. Supports high availability with automatic failover when a minority of replicas fail; transparent to applications.


## Documentation

The documentation of DingoDB is located on the website: [https://dingodb.readthedocs.io](https://dingodb.readthedocs.io)
or in the `docs/` directory of the source code.

## Developing DingoDB

We recommend IntelliJ IDEA to develop the DingoDB codebase. Minimal requirements for an IDE are:

* Support for Java
* Support for Gradle

### How to make a clean pull request

- Create a personal fork of dingo on GitHub.
- Clone the fork on your local machine. Your remote repo on GitHub is called origin.
- Add the original repository as a remote called upstream.
- If you created your fork a while ago be sure to pull upstream changes into your local repository.
- Create a new branch to work on. Branch from develop.
- Implement/fix your feature, comment your code.
- Follow the code style of Google code style, including indentation.
- If the project has tests run them!
- Add unit tests that test your new code.
- In general, avoid changing existing tests, as they also make sure the existing public API is
  unchanged.
- Add or change the documentation as needed.
- Squash your commits into a single commit with git's interactive rebase.
- Push your branch to your fork on GitHub, the remote origin.
- From your fork open a pull request in the correct branch. Target the Dingo's develop branch.
- Once the pull request is approved and merged you can pull the changes from upstream to your local
  repo and delete your branch.
- Last but not least: Always write your commit messages in the present tense. Your commit message
  should describe what the commit, when applied, does to the code – not what you did to the code.

### IntelliJ IDEA

The IntelliJ IDE supports Java and Gradle out of the box. Download it
at [IntelliJ IDEA website](https://www.jetbrains.com/idea/).

## Special Thanks

### DataCanvas

DingoDB is Sponsored by [DataCanvas](https://www.datacanvas.com/), a new platform to do data science and data process in real-time.

### Java Profiler tools: YourKit

[![YourKit](https://www.yourkit.com/images/yklogo.png)](https://www.yourkit.com/java/profiler/index.jsp)

I highly recommend YourKit Java Profiler for any preformance critical application you make.

Check it out at https://www.yourkit.com/


DingoDB is an open-source project licensed in **Apache License Version 2.0**, welcome any feedback from the community.
For any support or suggestion, please contact us.
