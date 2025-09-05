
# DingoDB
[DingoDB](https://github.com/dingodb/dingo) is an open-source distributed multi-modal vector database independently designed and developed by [DataCanvas](https://www.datacanvas.com/), which integrates real-time strong consistency, relational semantics, and vector semantics into a unified platform, DingoDB positioning itself as a distinctive multi-modal database solution. With exceptional horizontal scalability and elastic scaling capabilities, it effortlessly meets enterprise-grade high availability requirements. Furthermore, DingoDB offers extensive multi-language interfaces and seamless compatibility with the MySQL protocol, delivering unparalleled flexibility and convenience for users. Demonstrating comprehensive excellence in functionality, performance, and user-friendliness, DingoDB stands out as a robust solution for modern data-driven applications.

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

**6.Cold-Hot Tiered Retrieval for Massive Datasets**
DingoDB provides disk-based vector search capabilities to minimize memory consumption, and supports dynamic switching between different indexes based on data scale requirements.

## Get Start

### Docs
All Documentation [Docs](https://dingodb.readthedocs.io/en/latest/)

### Install
How to install and deploy [Docker](https://dingodb.readthedocs.io/en/latest/deployment/cluster/deploy_in_single_node_using_docker.html) or [Ansible](https://dingodb.readthedocs.io/en/latest/deployment/cluster/deploy_on_cluster_by_ansible/index.html)

### Usage
How to use DingoDB [Usage](https://dingodb.readthedocs.io/en/latest/usage/how_to_use_dingodb.html)

## Developing DingoDB

### VS Code
We recommend [VS Code](https://code.visualstudio.com/) to develop the DingoDB codebase. 

### Java Profiler tools: YourKit

We recommend YourKit Java Profiler for any preformance critical application you make.

Check it out at https://www.yourkit.com/

## Projects about DingoDB
The main projects about DingoDB are as follows:
- [Dingo-Store](https://github.com/dingodb/dingo-store): A strongly consistent distributed storage system based on the Raft protocol.
- [Dingo-Deploy](https://github.com/dingodb/dingo-deploy): The deployment project of compute nodes and storage nodes.

## How to make a clean pull request

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


## Special Thanks

### DataCanvas

DingoDB is Sponsored by [DataCanvas](https://www.datacanvas.com/), a new platform to do data science and data process in real-time.

DingoDB is an open-source project licensed under the **Apache License Version 2.0**, welcome any feedback from the community.
For any support or suggestion, please contact us.

## Contact us

If you have any technical questions or business needs, please contact us.

Attach the Wetchat QR Code

![](./docs/en/images/dingo_contact_Wetchat.png)
