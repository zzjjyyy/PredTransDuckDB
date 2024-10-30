# About Robust Predicate Transfer
We integrate Robust Predicate Transfer into DuckDB, when you finish installing DuckDB, you already have Predicate Transfer in it.
You can find the code of Predicate Transfer in src/optimizer/predicate_tranasfer. The interface of Predicate Transfer can be found at optimizer.cpp at line 120-121, 131.
## Requirement
Arrow 16.0 for Bloom filter implementation
## Code Structure
### LogicalBF operators
  > src/include/duckdb/planner/operator/logical_create_bf.hpp  
  > src/planner/operator/logical_create_bf.cpp  
  > src/include/duckdb/planner/operator/logical_use_bf.hpp  
  > src/planner/operator/logical_use_bf.cpp  
### PhysicalBF operators
  > src/include/duckdb/execution/operator/persistent/physical_create_bf.hpp  
  > src/execution/operator/persistent/physical_create_bf.cpp  
  > src/include/duckdb/execution/operator/filter/physical_use_bf.hpp  
  > src/execution/operator/filter/physical_use_bf.cpp  
### LogicalBF to PhysicalBF
  > src/execution/physical_plan/plan_create_bf.cpp  
  > src/execution/physical_plan/plan_use_bf.cpp  
### Predicate Transfer Optimizer
  > src/optimizer/predicate_transfer  
    >> --src/optimizer/predicate_transfer/bloom_filter: Bloom filter implementation  
    >>>  --src/optimizer/predicate_transfer/bloom_filter/bloom_filter_avx2.cpp: Bloom filter AVX2 version  
    >>>  --src/optimizer/predicate_transfer/bloom_filter/bloom_filter_use_kernel.cpp: Bloom filter probe interface  
    >>>  --src/optimizer/predicate_transfer/bloom_filter/bloom_filter.cpp: Bloom filter core implementation  
    >> --src/optimizer/predicate_transfer/dag_manager.cpp: build DAG from query graph  
    >> --src/optimizer/predicate_transfer/dag.cpp: DAG implementation  
    >> --src/optimizer/predicate_transfer/nodes_manager.cpp: Node manager  
    >> --src/optimizer/predicate_transfer/predicate_transfer_optimizer.cpp: Insert LogicalBF operators into the logical plan  
    
## Join Order Robustness
We use random join order generators in join_order_optimizer.cpp: SolveJoinOrderRandom(), SolveJoinOrderLeftDeepRandom(). Note that when you want to generate the random join order, comment the line 71 in join_order_optimizer.cpp.

# Benchmark
We test TPC-H, JOB, and TPC-DS. The queries we use can be found in TPCH.sql, JOB_temp.sql, and ./TPCDS.

# About DuckDB
DuckDB is a high-performance analytical database system. It is designed to be fast, reliable, portable, and easy to use. DuckDB provides a rich SQL dialect, with support far beyond basic SQL. DuckDB supports arbitrary and nested correlated subqueries, window functions, collations, complex types (arrays, structs), and more. For more information on using DuckDB, please refer to the [DuckDB documentation](https://duckdb.org/docs/).

# Installation
First, download the project and enter the root directory
```
cd PredTransDuckDB
```
Then, use cmake
```
cmake -B build -S .
```
At last, build the project in ./build
```
cd build
cmake --build .
```
