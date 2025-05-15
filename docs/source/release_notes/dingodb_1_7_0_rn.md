# Release Notes v1.7.0

## New Features
### 1. License Management Mechanism 
provides License management function to protect the intellectual property rights of DingoDB. Through the License activation and management tool, users can conveniently manage and monitor the use of the software to ensure legal compliance.
### 2. Stand-alone Lite version DingoDB 
realizes stand-alone Lite version DingoDB, which reduces the threshold of users. This version can be run on a single machine without complex distributed deployment, and is suitable for development testing and small-scale application scenarios, helping users to quickly get started and verify the functions of DingoDB.
### 3. New C++ SDK 
provides a new C++ SDK, which is convenient for developers to carry out secondary development and integration. Rich API interfaces are provided to support efficient data operation and management and improve development efficiency.

## Feature Optimizations
### Storage Layer Optimization 
#### 1. Braft Transformation 
Supports election priority control between peers to solve the vector index Leader balancing problem. It helps to improve the stability and performance of the cluster and avoid single-point overload.
#### 2. Prefilter Performance Improvement 
Adjust the data structure of ScalarData to improve the rate of prefilter. By optimizing the data structure, DingoDB can process data filtering faster and reduce query latency.
#### 3. Instruction Set Extension 
In addition to the default SSE instruction of vector distance calculation function FAISS and HNSWLIB, the instruction set AVX2/AVX512 is supported. By expanding the instruction set, the efficiency of vector computation is improved, especially in high-performance computing environments.
#### 4. Vector Distance Calculation Performance Improvement 
realizes CPU instruction set acceleration switching at runtime, which significantly improves the performance of vector distance calculation. It automatically switches instruction sets (e.g. SSE, AVX2, AVX512) as needed, which is especially effective in processing large-scale data sets.
#### 5. Leader Balancing Rate Improvement 
improves the balancing rate of Leader in the cluster by optimizing the algorithm. The improved election algorithm ensures a more balanced distribution of Leaders and enhances the overall performance of the system.
#### 6. Vector Index Data Insertion Performance Improvement 
Optimizes the insertion performance of IVF_FLAT and IVF_PQ vector indexes. The improved insertion algorithm improves the data insertion efficiency and shortens the index construction time.
#### 7. Synchronous Operation Performance Optimization 
Optimizes the synchronous operation performance of BThread and PThread. By reducing the overhead of thread synchronization, it improves the performance in a multi-threaded environment.
#### 8. Vector Search Performance Improvement 
Adjust the parameters such as Region size, Region number and thread number to effectively improve the performance of Vector Search. Optimize system resource allocation to significantly improve the response speed and efficiency of search.
### Computing Layer Optimization 
#### 1. Logging System Optimization 
Modify the Executor layer logging system to provide full-link logging information and improve log traceability. The improved logging system records more detailed information, helping users to comprehensively monitor and analyze all kinds of events in the process of task execution.
#### 2. Observable Metrics 
adds a new Metric information statistics function, which monitors the Metric information of the job at each stage and improves the observability of the task. By monitoring performance indicators in real time, users can better understand the execution of tasks and performance bottlenecks, and improve the efficiency of system operation and maintenance.