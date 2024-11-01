#include "duckdb.hpp"
#include <string>
#include <iostream>

using namespace duckdb;

#define REPEAT 200

int main() {
    std::string dbname = "JOB";
    // std::string dbname = "TPCH";
    std::string dbfilepath = "/home/junyi/PredTransDuckDB/JoinOrderBenchmark.db";
    // std::string dbfilepath = "/home/junyi/PredTransDuckDB/tpch.db";
    char qry_file[64];
    sprintf(qry_file, "/home/junyi/PredTransDuckDB/%s.sql", dbname.c_str());
    FILE* fp_qry = fopen(qry_file, "r");
    if (fp_qry == NULL) {
        std::cout << "Fail to open .sql file!\n";
        exit(0);
    }
    char query[4096] = "";
    int query_id = 1;
    while (fgets(query, 4096, fp_qry) != NULL) {
        if (query[0] == '\n') {
            query_id++;
            continue;
        }
        std::cout << query_id << std::endl;
        FILE* fp = fopen("result.txt", "a+");
        fprintf(fp, "----------------------------query %d----------------------------\n", query_id);
        fclose(fp);
        DuckDB db(dbfilepath.c_str());
        Connection con(db);
        // execute creat tmp table
        auto result = con.Query(query);
        result.release();
        // string query_str = query;
        // string explain_query = "EXPLAIN " + query_str;
        con.Query("SET THREADS = 1;");
        fgets(query, 4096, fp_qry);
        for(int cnt = 0; cnt < REPEAT; cnt++) {
            result = con.Query(query);
            // std::cout << "finish" << query_id << std::endl;
            if(result->HasError()) {
                std::cout << result->GetError() << "\n";
            }
            result.release();
        }
    }
    fclose(fp_qry);
    return 0;
}
