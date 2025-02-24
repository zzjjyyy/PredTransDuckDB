# About Robust Predicate Transfer
We integrate Robust Predicate Transfer into DuckDB, when you finish installing DuckDB, you already have Predicate Transfer in it.
You can find the code of Predicate Transfer in src/optimizer/predicate_transfer. The interface of Predicate Transfer can be found at optimizer.cpp at line 120-121, 131.
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
    
## Configuration
The configuration setting of Robust Predicate Transfer can be found in /src/include/duckdb/optimizer/predicate_transfer/setting.hpp file. Once you change the configuration, you have to recompile the whole project.

If you want to use the original DuckDB, comment all lines;  
If you want to use the DuckDB with Bloom Join optimized, uncomment line 2 (#define BloomJoin);  
If you want to use the original Predicate Transfer (CIDR2024 version), uncomment line 3 (#define PredicateTransfer) and line 10 (#define SmalltoLarge);  
If you want to use the Robust Predicate Transfer, only uncomment line 3 (#define PredicateTransfer).

To generate the left-deep cost-based plan, use #define ExactLeftDeep.  
To generate random left deep join orders, use #define RandomLeftDeep.  
To generate random bushy join orders, use #define RandomBushy.

To enable intermediate result spill to disk, use #define External.

# Benchmark
We test TPC-H, JOB, TPC-DS, and DSB.
TPC-H and TPC-DS are downloaded from the TPC website.
JOB is downloaded from https://github.com/danolivo/jo-bench.
DSB is downloaded from https://github.com/microsoft/dsb.
The queries we used can be found in TPCH.sql, JOB.sql, and ./tpc-ds (DSB uses the same queries).  
We add the scripts we used to measure the execution time in /test_scripts. JOB and TPC-H share that same script, and TPC-DS and DSB use another one.  
For JOB and TPC-H, compile main.cpp by Cmake and make and run the output file "./test".
A "result.txt" file will be generated in the same directory and records the time.
We achieve this by modifying the C++ API code of DuckDB. You can find the relevant code in client_context.cpp line 808-810 and 859-871.  
For TPC-DS and DSB, compile main.cpp and run run.sh. A "result.txt" file will be generated as well.

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

# Reproduction Example
Step 1: Acquire the TPC-H/JOB/TPC-DS/DSB.  
Step 2: Change the /src/include/duckdb/optimizer/predicate_transfer/setting.hpp: use #define PredicateTransfer and #define RandomLeftDeep / RandomBushy. This tests the RPT join order robustness.  
Step 3: Cmake and Make the project.  
Step 4: Cmake and Make ./test_scripts/JOB&TPCH/ (or ./test_scripts/TPCDS/).  
Step 5: Run ./test (or run.sh). The result will be recorded in result.txt under the current directory.  
Step 6: Change the /src/include/duckdb/optimizer/predicate_transfer/setting.hpp: use #define RandomLeftDeep / RandomBushy and comment #define PredicateTransfer. This tests the DuckDB join order robustness. 
Step 7: Repeat Step 3-5.
