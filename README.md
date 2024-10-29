# About Robust Predicate Transfer
We integrate Robust Predicate Transfer into DuckDB, when you finish installing DuckDB, you already have Predicate Transfer in it.
You can find the code of Predicate Transfer in src/optimizer/predicate_tranasfer. The interface of Predicate Transfer can be found at optimizer.cpp at line 120-121, 131.
## Requirement
Arrow 16.0 for Bloom filter implementation
## Join Order Robustness
We use random join order generators in join_order_optimizer.cpp: SolveJoinOrderRandom(), SolveJoinOrderLeftDeepRandom(). Note that when you want to generate the random join order, comment the line 71 in join_order_optimizer.cpp.

# Benchmark
We test TPC-H, JOB, and TPC-DS. The queries we use can be found in TPCH.sql, JOB_temp.sql, and ./TPCDS.

# About DuckDB
DuckDB is a high-performance analytical database system. It is designed to be fast, reliable, portable, and easy to use. DuckDB provides a rich SQL dialect, with support far beyond basic SQL. DuckDB supports arbitrary and nested correlated subqueries, window functions, collations, complex types (arrays, structs), and more. For more information on using DuckDB, please refer to the [DuckDB documentation](https://duckdb.org/docs/).

# Installation
If you want to install and use DuckDB, please see [our website](https://www.duckdb.org) for installation and usage instructions.
