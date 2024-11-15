# About Robust Predicate Transfer
We integrate Robust Predicate Transfer into DuckDB, when you finish installing DuckDB, you already have Predicate Transfer in it.
You can find the code of Predicate Transfer in src/optimizer/predicate_tranasfer. The interface of Predicate Transfer can be found at optimizer.cpp at line 120-121, 131.
## Requirement
Arrow 16.0 for Bloom filter implementation
## Code Structure
### LogicalBF operators
&nbsp;&nbsp;&nbsp;&nbsp; src/include/duckdb/planner/operator/logical_create_bf.hpp  
&nbsp;&nbsp;&nbsp;&nbsp; src/planner/operator/logical_create_bf.cpp  
&nbsp;&nbsp;&nbsp;&nbsp; src/include/duckdb/planner/operator/logical_use_bf.hpp  
&nbsp;&nbsp;&nbsp;&nbsp; src/planner/operator/logical_use_bf.cpp  
### PhysicalBF operators
&nbsp;&nbsp;&nbsp;&nbsp; src/include/duckdb/execution/operator/persistent/physical_create_bf.hpp  
&nbsp;&nbsp;&nbsp;&nbsp; src/execution/operator/persistent/physical_create_bf.cpp  
&nbsp;&nbsp;&nbsp;&nbsp; src/include/duckdb/execution/operator/filter/physical_use_bf.hpp  
&nbsp;&nbsp;&nbsp;&nbsp; src/execution/operator/filter/physical_use_bf.cpp  
### LogicalBF to PhysicalBF
&nbsp;&nbsp;&nbsp;&nbsp; src/execution/physical_plan/plan_create_bf.cpp  
&nbsp;&nbsp;&nbsp;&nbsp; src/execution/physical_plan/plan_use_bf.cpp  
### Predicate Transfer Optimizer
&nbsp;&nbsp;&nbsp;&nbsp; src/optimizer/predicate_transfer  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; src/optimizer/predicate_transfer/bloom_filter: Bloom filter implementation  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; src/optimizer/predicate_transfer/bloom_filter/bloom_filter_avx2.cpp: Bloom filter AVX2 version  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; src/optimizer/predicate_transfer/bloom_filter/bloom_filter_use_kernel.cpp: Bloom filter probe interface  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; src/optimizer/predicate_transfer/bloom_filter/bloom_filter.cpp: Bloom filter core implementation  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; src/optimizer/predicate_transfer/dag_manager.cpp: build DAG from query graph  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; src/optimizer/predicate_transfer/dag.cpp: DAG implementation  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; src/optimizer/predicate_transfer/nodes_manager.cpp: Node manager  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; src/optimizer/predicate_transfer/predicate_transfer_optimizer.cpp: Insert LogicalBF operators into the logical plan  
    
## Join Order Robustness
We use random join order generators in join_order_optimizer.cpp:  
&nbsp;&nbsp;&nbsp;&nbsp; SolveJoinOrderRandom()  
&nbsp;&nbsp;&nbsp;&nbsp; SolveJoinOrderLeftDeepRandom().  

To generate the default join order, you can comment lines 52, 53, and 54 and so that "final_plan = plan_enumerator.SolveJoinOrder()". And then compile the DuckDB project.  

To generate the random join order, you first need to choose one line between line 53 and 54 (line 52 should always be commented) and comment line 51.
So that "auto final_plan = plan_enumerator.SolveJoinOrderRandom();" or "auto final_plan = plan_enumerator.SolveJoinOrderLeftDeepRandom();".
And you also need to comment the line 71 in join_order_optimizer.cpp to lock the left and right sides of a join. And then compile the DuckDB project.  

# Benchmark
We test TPC-H, JOB, and TPC-DS. The queries we used can be found in TPCH.sql, JOB.sql, and ./TPCDS.  
We add the scripts we used to measure the execution time in /test_scripts. JOB and TPC-H share that same script and TPC-DS uses another one.  
For JOB and TPC-H, compile main.cpp by Cmake and make and run the output file "./test".
A "result.txt" file will be generated in the same directory and record the time.
We achieve this by modifying the C++ API code of DuckDB. You can find the relevant code in client_context.cpp line 808-810 and 859-871.  
For TPC-DS, compile main.cpp and run run.sh. A "result.txt" file will be generated as well.

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
