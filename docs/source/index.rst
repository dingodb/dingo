.. DingoDB documentation master file, created by
   sphinx-quickstart on Wed Apr 23 14:54:10 2025.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to DingoDB Docs!
==========================

Here you will learn about what DingoDB is, and how to install and deploy DingoDB to build an application according to your need.

.. _cards-clickable:
    :text-align:center
    :shadow:none

.. card:: About DingoDB
    :link: get_started/index.html
    :link-type: ref
    :link-alt: clickable cards

    The AI-Native multimodal database provides strongly consistent storage for relational, vector, text, and other multimodal data, while offering SQL-based unified analysis capabilities across multiple modalities.

Product Advantages
...............
.. grid:: 2

    .. grid-item-card::  Enterprise Storage Reliability
    
        Based on Multi-Raft multi-copy storage, ensuring data strong consistency and meeting enterprise-level disaster recovery requirements.

    .. grid-item-card::  Scalar-Vector Joint Retrieval

        Supports SQL execution of scalar and vector joint retrieval, provides a variety of vector index types, simplifies the development complexity of RAG applications, and meets various scenario requirements.

    .. grid-item-card::  Multi-Modal Database Capabilities
    
        Deploying a set of DingoDB can have the service capabilities of Key-Value cache acceleration, relational database, and vector database, reduce the maintenance and management costs of multiple database systems, and improve the efficiency and flexibility of the overall system.

    .. grid-item-card::  Compatible with MySQL Protocol

        DingoDB is compatible with the MySQL protocol, users can directly access DingoDB using the MySQL client, without the need to learn new database syntax and tools, reducing user learning costs and usage thresholds.
        
    .. grid-item-card::  Horizontal Expansion and Contraction
    
        Based on the architecture design of storage and calculation separation, DingoDB can achieve one-click horizontal expansion and contraction of performance and resources, enabling enterprises to quickly adjust the database scale according to business needs and effectively respond to traffic changes.

    .. grid-item-card::  Compatible with Multiple Storage Engines

        DingoDB supports multiple storage engines, enabling enterprises to choose the most suitable storage engine according to business characteristics, maximizing performance benefits.

    .. grid-item-card::  Distributed Transaction Support
    
        DingoDB supports distributed transactions that combine scalar and vector, and provides multiple isolation levels, compatible with optimistic transactions and pessimistic transactions, ensuring the integrity and consistency of transactions in a distributed environment

    .. grid-item-card::  Multi-Tenant Support

        DingoDB natively supports multi-tenancy, service isolation to prevent data leakage and interference. Supports request-level dynamic flow control, flexible resource allocation. Supports mixed storage and isolation by tenant and resource group to ensure reasonable allocation and efficient use of resources.





.. toctree::
   :maxdepth: 3

   overview/index
   get_started/index
   architecture/index
   operation_and_maintenance/index
   sql/index
   release_notes/index
   faq/index
