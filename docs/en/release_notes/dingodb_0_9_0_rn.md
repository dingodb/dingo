# Release Notes v0.9.0

## 1、New Features
### 1）License Management Mechanism
Introduced a License management feature to protect DingoDB's intellectual property. With the License activation and management tools, users can easily manage and monitor software usage, ensuring legal and compliant use.
### 2）Single Machine Lite Version of DingoDB
Implemented a Single Machine Lite version of DingoDB, lowering the usage threshold for users. This version can run on a single machine without complex distributed deployment, making it ideal for development, testing, and small-scale application scenarios, helping users quickly get started and validate DingoDB's capabilities.
### 3）New C++ SDK
Provided a new C++ SDK to facilitate secondary development and integration for developers. The SDK offers a rich set of APIs that support efficient data operations and management, enhancing development efficiency.

## 2、Feature Optimizations
### 2.1 Storage Layer Optimization
#### 1）Braft Modification
Added support for controlling election priority among peers, addressing the vector index Leader balancing issue. This helps improve cluster stability and performance, avoiding single-point overloads.
#### 2）Prefilter Performance Enhancement
Adjusted the data structure of ScalarData to improve pre-filtering rates. Through data structure optimization, DingoDB can process data filtering faster, reducing query latency.
#### 3）Instruction Set Expansion
In addition to the default SSE instructions for vector distance calculation functions FAISS and HNSWLIB, added support for AVX2/AVX512 instruction sets. By expanding the instruction set, vector computation efficiency is improved, particularly in high-performance computing environments.
#### 4） Vector Distance Calculation Performance Improvement
Implemented runtime CPU instruction set acceleration switching, significantly enhancing vector distance calculation performance. The system automatically switches instruction sets (e.g., SSE, AVX2, AVX512) as needed, especially effective in handling large-scale datasets.
#### 5）Leader Balance Rate Improvement
Optimized algorithms to improve the balance rate of Leaders in the cluster. The improved election algorithm ensures a more balanced distribution of Leaders, enhancing the overall system performance.
#### 6）Vector Index Data Insertion Performance Improvement
Optimized the insertion performance of IVF_FLAT and IVF_PQ vector indexes. The improved insertion algorithm increases data insertion efficiency, reducing index build time.
#### 7）Synchronization Operation Performance Optimization
Optimized the synchronization performance of BThread and PThread. By reducing the overhead of thread synchronization, the performance in multi-threaded environments is enhanced.
#### 8）Vector Search Performance Improvement
Adjusted parameters such as Region size, Region count, and number of threads to effectively improve vector search performance. Optimized resource allocation significantly boosts search response speed and efficiency.

### 2.2 Computation Layer Optimization
#### 1）Log System Optimization
Revamped the Executor layer log system to provide full-link log information, enhancing log traceability. The improved log system records more detailed information, helping users comprehensively monitor and analyze various events during task execution.
#### 2）Observable Metrics
Introduced a new Metric information statistics feature to monitor job metrics at various stages, enhancing task observability. By monitoring performance metrics in real time, users can better understand task execution and performance bottlenecks, improving system operation and maintenance efficiency.