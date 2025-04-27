# Terminology

The following terminologies are sorted in alphabetical order.

### **Client**  
Performs data access and operations, supporting connectivity via JDBC, Java SDK, and Python SDK. It also enables database connections and operations through command-line tools like `sqlline` or `MySQL Shell`.  

### **Coordinator**  
Maintains the status of global and branch transactions, driving global commits or rollbacks. Additionally, it provides metadata services for the entire cluster, manages the lifecycle of all cluster nodes, and delivers globally consistent services required by other database components.  

### **Dingo-Proxy**  
Serves as a bridging layer for vector operations and provides HTTP/gRPC interfaces for the Python SDK.  

### **Executor**  
Acts as the computing node for executing job tasks. It handles the execution of SQL-related distributed tasks, interacts with data storage, parses and responds to SQL requests from Clients, and manages other administrative operations. The Executor implements the JDBC protocol, manages jobs, coordinates distributed task execution, and invects underlying Meta and Store APIs via SDKs to deliver database functionalities.  

### **Partition**  
The minimal unit of data storage in DingoDB, each representing a specific data range. By default, each Partition maintains three replicas. A replica of a Partition is termed a Peer. Multiple Peers of the same Partition replicate data via the Raft consensus algorithm, making them members of a Raft instance. DingoDB employs Multi-Raft for data management, ensuring each Partition corresponds to an isolated Raft group.  

### **Partition Split**  
Partitions are dynamically created as data volume grows. The splitting mechanism uses an initial Partition to cover the entire key space and generates new Partitions by splitting existing ones when predefined thresholds (e.g., data size or key count) are reached.  

### **Raft**  
A leader-based consensus algorithm that guarantees data consistency across multiple nodes. It enhances system fault tolerance even under partial node failures, network delays, or partitions.  

### **Raft Engine**  
An embedded persistent storage engine with a log-structured design, purpose-built for distributed key-value stores to manage multi-Raft logs. DingoDB leverages Raft Engine as its log storage engine.  

### **Region**  
Represents a physical data partition. A table is divided into multiple Regions using configurable splitting strategies. Each Region exclusively belongs to one Partition and cannot span multiple Partitions.  

### **Store**  
DingoDB’s storage layer, delivering high-availability, scalable, and high-performance distributed KV storage for massive datasets. Beyond standard KV operations, it natively supports vector data types and vector similarity search.  
