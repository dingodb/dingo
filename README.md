# DingoDB

[DingoDB](https://github.com/dingodb/dingo) is a distributed multi-modal vector database. It combines the features of a data lake and a vector database, allowing for the storage of any type of data (key-value, PDF, audio, video, etc.) regardless of its size. Utilizing DingoDB, you can construct your own **Vector Ocean** (the next-generation data architecture following data warehouse and data lake, as introduced by [DataCanvas](https://www.datacanvas.com/)). This enables the analysis of both structured and unstructured data through a singular SQL with exceptionally low latency in real time.

![](docs/images/dingo_stack.png)

## Key Features

### As a Distributed Vector Database for Any Data

1. Provides comprehensive data storage solutions, accommodating a wide range of data types including but not limited to embeddings, audio files, text, videos, images, PDFs, and annotations.
2. Facilitates efficient querying and vector searching with minimal latency using a singular SQL approach.
3. Employs a hybrid search mechanism that caters to both structured and unstructured data, supporting operations like metadata querying and vector querying.
4. Possesses the ability to dynamically ingest data and construct corresponding indexes in real time, promoting operational efficiency.
  
### As a Distributed Relation database

1. MySQL Compatibility
   Built upon the acclaimed [Apache Calcite](https://calcite.apache.org/) SQL engine, DingoDB is capable of parsing, optimizing, and executing standard SQL statements, and can handle parts of TPC-H and TPC-DS(See [TPC](http://www.tpc.org/)) queries. Compliant with MySQL Shell and MySQL-JDBC-Driver Client, it offers seamless integration with web services, BI tools, and more.
2. Supports High Frequency Write Operations
   DingoDB is designed to handle high-frequency write operations, such as INSERT, UPDATE, and DELETE, while maintaining strong data consistency using the [RAFT](https://raft.github.io/) consensus protocol. In the short future, it will also support Redis protocol, You can use redis-cli to access DingoDB RawKV.
3. Facilitates Point Queries and Multi-dimensional Analysis Simultaneously:
   DingoDB can push down expressions to accelerate queries and quickly carry out multi-dimensional analysis with low latency.
4. Distributed Storage Capabilities
   As a distributed storage engine, DingoDB has the capacity to store vast amounts of data. It allows for easy horizontal scaling operations on clusters as data scale increases, and implemented using [dingo-store](https://github.com/dingodb/dingo-store).
5. High Data Reliability and Recovery
   Based on the [Raft](https://raft.github.io/) distributed consensus protocol, DingoDB can ensure data reliability and recovery in the event of machine or disk failures and offers a swift automatic recovery mechanism.
6. Supports Multiple Vector Searches
   DingoDB supports vector searches, which are queries that involve vector data types, such as vectors of integers or vectors of floating-point numbers.

## Related Projects about DingoDB

Welcome to visit [DingoDB](https://github.com/dingodb/dingo). The documentation of DingoDB is located on the website: [https://dingodb.readthedocs.io](https://dingodb.readthedocs.io).  The main projects about DingoDB are as follows:

- [DingoDB](https://github.com/dingodb/dingo): A Unified SQL Engine to parse and compute for both structured and unstructured data.
- [Dingo-Store](https://github.com/dingodb/dingo-store): A strongly consistent distributed storage system based on the Raft protocol.
- [Dingo-Deploy](https://github.com/dingodb/dingo-deploy): The deployment project of compute nodes and storage nodes.

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

## Contact us

If you have any technical questions or business needs, please contact us.

Attach the Wetchat QR Code

![](./docs/images/dingo_contact.jpg)
