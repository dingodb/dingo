# What is DingoDB

DingoDB is a new generation open-source real-time interactive analytical database that integrates analysis and services, 
known as HSAP (Hybrid Serving & Analytical Processing). It can execute high-frequency query and upsert, 
interactive analysis, and multidimensional analysis with low latency, providing massive storage, real-time computing,
and high-concurrency service capabilities for scenarios such as massive data processing, machine learning, and AI.

DingoDB can provide integrated persistent storage of historical and real-time data based on distributed object storage, 
and provide high-concurrency microsecond-level data query and second-level data calculation service capabilities. 
It also realizes unified data processing based on SQL, with integrated writing, analysis, and calculation SQL.

![Dingo stack](../images/dingo_stack.png)

## Key Features

1. **Compliant with MySQL-Compatible**  
   
   Based on the popular [Apache Calcite](https://calcite.apache.org/) SQL engine, DingoDB can parse, optimize and
   execute standard SQL statement, and is capable to run part of TPC-H and TPC-DS (See [TPC](http://www.tpc.org/))
   queries. DingoDB is also compliant with JDBC and can be seamlessly integrated with web services, BI tools, etc.
2. **Intelligent optimizer**

   DingoDB supports row storage, column storage, and row-column hybrid, while table-level supports multi-partition and 
   replica mechanisms. DingoDB's SQL optimizer provides the optimal execution plan based on the metadata of the data, 
   achieving automatic selection of rows and columns.
3. **Real-time high-frequency updates**  
   
   DingoDB can implement data record Upsert and Delete operations based on the primary key. At the same time,
   the data uses a multi-partition replica mechanism, which can convert Upsert and Delete operations into
   Key-Value operations to achieve high-frequency updates. With real-time capability as the center, it has
   millisecond and microsecond-level query response, real-time writing, and real-time updates.
4. **Multi-replica policy storage**
  
   DingoDB supports row, column, and mixed data formats. In addition, it has a plug-in API that allows developers
   to easily expand storage modes, such as on local disks or remote object storage.
5. **Storage and calculation separation, elastic deployment**
  
   Storage and calculation separation, unified storage, reducing data migration, supporting various connectors for 
   rich data access and calculation, fully distributed and scalable architecture, dynamic cluster management, and 
   elastic scaling.
6. **Unified data storage**
   
   Based on distributed object storage, it provides integrated persistent storage of historical and real-time data.
   It supports cold (distributed) and hot (local) storage of data.
7. **Unified data service**
   
   Integrated analysis service, supporting point query, interactive analysis, and offline acceleration; providing 
   high-concurrency microsecond-level data query and second-level data calculation service capabilities; providing 
   7*24-hour service capabilities.
8. **Unified data processing**
   
   Based on SQL, it realizes unified data processing, with integrated writing, analysis, and calculation SQL.

## Scenes

- **Fusion metric calculation**
   
    Real-time metric services are core to assisting businesses in making real-time decisions. By incorporating multiple efficient calculation operation types, more scenario needs can be met, and the optimal calculation operation type can be provided for different scenarios, thereby generating information that can serve specific domains.

- **Real-time data calculation**
   
    DingoDB is suitable for tracking KPIs and other important metrics, allowing users to build BI dashboards and scorecards to track enterprise goals and metrics in real-time. It provides a fast and concise way to measure KPIs and indicate the progress made by the enterprise towards achieving its goals.

- **Real-time interactive analysis**

  Users perform interactive data queries and probe data to extract trends and solutions from historical and current discoveries to drive valuable data-driven decisions. Different functional personnel such as HR managers, sales representatives, and marketing teams can make data-driven decisions directly through self-service business intelligence tools supported by DingoDB.

- **Real-time artificial intelligence**

  Artificial intelligence and machine learning simulate complex tasks performed by the human brain. This approach uses statistical data, database systems, and machine learning to discover patterns in large datasets stored on DingoDB. Data mining also requires DingoDB to preprocess the data.

- **Multi-dimensional real-time data analysis**

  Analysts are advanced BI users who use centralized company data, DingoDB, and powerful analysis tools to understand opportunities for improvement and make strategic recommendations to the company's leadership.

- **Semantic text search**

    Combining vector search with metadata filtering to obtain more relevant result data; using embedding models to convert text data into vector embeddings and searching these vectors; merging the query results and returning them.
