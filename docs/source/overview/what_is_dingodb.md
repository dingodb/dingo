# What is DingoDB

## Introduction

[DingoDB](https://github.com/dingodb/dingo) is a distributed real-time multi-modal database. It combines the features of a data lake and a vector database. It can store any type of data (key-value, PDF, audio, video, etc) with data of any size.Using it, you can build your **Vector Ocean** (The Next Generation data architecture following data warehouse and data lake initiated by [DataCanvas](https://www.datacanvas.com/)) and analyze structured and unstructured data with extremely low latency.

## Key Features

**1. Comprehensive Access Interface**

DingoDB provides comprehensive access interfaces, supporting various flexible access modes such as SQL, SDK, and API to meet the needs of different developers. Additionally, it introduces Table and Vector as first-class citizen data models, providing users with efficient and powerful data processing capabilities.

**2. Built-in data High Availability**

DingoDB provides fully functional and highly available built-in configurations without the need to deploy any external components, which can significantly reduce users' deployment and operation and maintenance costs and significantly improve the efficiency of system operation and maintenance.

**3. Fully Automatic Elastic Data Sharding**

DingoDB supports dynamic configuration of data shard size, automatic splitting and merging, realizing efficient and friendly resource allocation strategies, and easily responding to various business expansion needs.

**4. Scalar-vector Hybrid Retrieval**

DingoDB supports both traditional database index types and various vector index types, providing a seamless scalar and vector hybrid retrieval experience, reflecting industry-leading retrieval capabilities. In addition, it also supports fusion of scalars and vectors. Distributed transaction processing.

**5. Built-in Real-time index Optimization**

DingoDB can build scalar and vector indexes in real time, providing users with unconscious background automatic index optimization. At the same time, it ensures no delays during data retrieval.

**6. Cold-Hot Tiered Retrieval for Massive Datasets**

 DingoDB provides disk-based vector search capabilities to minimize memory consumption, and supports dynamic switching between different indexes based on data scale requirements.


## Use Cases

**1. Enterprise Knowledge Base Construction**  

DingoDB aims to provide vectorized storage for enterprise knowledge bases and collaborate with large-scale models. Its goal is to help build intelligent knowledge bases, comprehensively enhancing knowledge-sharing capabilities. It can significantly improve employee work efficiency by reducing time spent searching for information; supports governments and traditional enterprises in constructing enterprise-level knowledge bases to achieve semantically precise search and federated analysis.  

**2. Large Model Memory System**  

DingoDB delivers reliable and efficient storage solutions through its distributed storage system. It manages Prompts before assisting large model generation, where Prompts store and organize diverse user inputs to enable rapid retrieval of relevant Prompts as inputs during queries. It provides high-concurrency search and answer generation capabilities. With the support of the large model memory system, it enhances query response speed, user experience, and answer generation efficiency while meeting query demands in large model application scenarios.  

**3. Vector Ocean Data Support Platform**  

As a powerful data storage platform, DingoDB can store and analyze structured and unstructured data while providing reliable access and query interfaces to improve query efficiency. It also features multimodal data analysis and scientific computing capabilities, enabling the processing of multimodal data, offering rich analytical algorithms to explore and mine information from data, and supporting complex data analysis tasks and application scenarios.  

**4. Real-Time Decision Metric Calculation Capability**  

DingoDB’s high-performance and low-latency features allow users to process and analyze large-scale data within seconds, supporting real-time risk control decisions. It employs data replication and failover mechanisms to ensure high availability and uses persistence technology to prevent data loss. Additionally, DingoDB supports horizontal scaling of computing and storage resources based on business needs to accommodate growing data processing demands. Its high-frequency Serving capabilities meet stringent regulatory standards, enabling sub-second decision-making scenarios such as real-time risk control, marketing, and recommendation systems.  

**5. Unstructured Data Retrieval**  

DingoDB supports retrieval of unstructured data such as audio, video, images, and text. It provides disk-based vector retrieval capabilities and allows switching between different indexes based on business scenarios or data scale. It enables real-time, continuous index reconstruction in the backend as users write data, ensuring synchronization between data and indexes. DingoDB supports mainstream vector indexes like Flat and HNSW.  

**6. Integrated Analysis of Structured and Unstructured Data**  

DingoDB supports processing and vectorized storage of diverse unstructured data types, including audio, video, and text, facilitating federated analysis and scientific computing. This integrated analysis capability allows users to leverage both structured data and vector data to uncover latent information and value from multiple dimensions and depths. Applications include risk prediction, public opinion monitoring, intelligent customer service, and recommendation systems. For example, in finance, structured financial data can be combined with unstructured news reports for fusion analysis to detect market trends and risk fluctuations.  