# DingoDB

[DingoDB](https://github.com/dingodb/dingo) is a distributed real-time multi-modal database. It combines the features of a data lake and a vector database. It can store any type of data (key-value, PDF, audio, video, etc) with data of any size.Using it, you can build your **Vector Ocean** (The Next Generation data architecture following data warehouse and data lake initiated by [DataCanvas](https://www.datacanvas.com/)) and analyze structured and unstructured data with extremely low latency.

![](docs/images/dingo_stack.png)

## Key Features

### As A Distributed Vector database for Any Data

1. Offering storage for all data types (embeddings, audio, text, videos, images, pdfs, annotations, etc.)
2. query and vector search in low latency 
3. Perform hybrid search including embeddings and structured data such as label or attributes
  
### As A Distributed Relation database

1. Compliant with MySQL-Compatible
   Based on the popular [Apache Calcite](https://calcite.apache.org/) SQL engine, DingoDB can parse, optimize and
   execute standard SQL statements, and is capable to run part of TPC-H and TPC-DS (See [TPC](http://www.tpc.org/))
   queries. DingoDB is also compliant with MySQL Shell and MySQL-JDBC-Driver Client, So you can be seamlessly integrated with web services, BI tools, etc.
2. Support high frequency write operation  
   By using [RAFT](https://raft.github.io/) and log-structured key-value storage [RocksDB](https://rocksdb.org/). You can perform high-frequency INSERT, UPDATE, DELETE and short-QUERY while ensuring strong data consistency. 
3. Support point query and multi-dimensional analysis simultaneously  
   DingoDB can push down the expressioin to store to accelerate 
   query and fast multi-dimensional analysis in low latency.
4. As a distributed storage engine, it provides the ability to store massive amounts of data. At the same time, with the increase in data scale, it can easily perform horizontal scaling operations on clusters.
5. As a distributed storage engine, It is designed based on Raft to provide a multi-replicated management mechanism, which provides extremely high data reliability. It can ensure high data consistency in the event of disk or machine failures, and also provides a fast automatic recovery mechanism.

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
