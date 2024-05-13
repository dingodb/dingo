# DingoDB

[DingoDB](https://github.com/dingodb/dingo) is an open-source distributed multimodal vector database designed and developed by [DataCanvas](https://www.datacanvas.com/). It integrates multiple features such as online strong consistency, relational semantics, and vector semantics, making it a unique multimodal database product. In addition, DingoDB has excellent horizontal scalability and scaling capabilities, easily meeting enterprise-level high availability requirements. At the same time, it supports multiple language interfaces and is fully compatible with the MySQL protocol, providing users with high flexibility and convenience. DingoDB demonstrates comprehensive and outstanding advantages in terms of functionality, performance, and ease of use, bringing users an unprecedented data management experience.

![](docs/images/dingo_stack.png)

## Key Features

**1. Comprehensive access interface**

DingoDB provides comprehensive access interfaces, supporting various flexible access modes such as SQL, SDK, and API to meet the needs of different developers. Additionally, it introduces Table and Vector as first-class citizen data models, providing users with efficient and powerful data processing capabilities.

**2.Built-in data high availability**

DingoDB provides fully functional and highly available built-in configurations without the need to deploy any external components, which can significantly reduce users' deployment and operation and maintenance costs and significantly improve the efficiency of system operation and maintenance.

**3.Fully automatic elastic data sharding**

DingoDB supports dynamic configuration of data shard size, automatic splitting and merging, realizing efficient and friendly resource allocation strategies, and easily responding to various business expansion needs.

**4.Scalar-vector hybrid retrieval**

DingoDB supports both traditional database index types and various vector index types, providing a seamless scalar and vector hybrid retrieval experience, reflecting industry-leading retrieval capabilities. In addition, it also supports fusion of scalars and vectors. Distributed transaction processing.

**5.Built-in real-time index optimization**

DingoDB can build scalar and vector indexes in real time, providing users with unconscious background automatic index optimization. At the same time, it ensures no delays during data retrieval.

## Related Projects about DingoDB

Welcome to visit [DingoDB](https://github.com/dingodb/dingo). The documentation of DingoDB is located on the website: [https://dingodb.readthedocs.io](https://dingodb.readthedocs.io).  The main projects about DingoDB are as follows:

- [DingoDB](https://github.com/dingodb/dingo): A Unified SQL Engine to parse and compute for both structured and unstructured data.
- [Dingo-Store](https://github.com/dingodb/dingo-store): A strongly consistent distributed storage system based on the Raft protocol.
- [Dingo-Deploy](https://github.com/dingodb/dingo-deploy): The deployment project of compute nodes and storage nodes.

## Get Start

### Docs
All Documentation [Docs](https://dingodb.readthedocs.io/en/latest/)

### Install
How to install and deploy [Docker](https://dingodb.readthedocs.io/en/latest/deployment/deploy_in_single_node_using_docker.html) or [Ansible](https://dingodb.readthedocs.io/en/latest/deployment/deploy_on_cluster_by_ansible.html#)

### Usage
How to use DingoDB [Usage](https://dingodb.readthedocs.io/en/latest/usage/how_to_use_dingodb.html)

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

![](./docs/en/images/dingo_contact_Wetchat.png)

Attach the Offical Account QR Code

![](./docs/en/images/dingo_contact_officalAccount.png)
