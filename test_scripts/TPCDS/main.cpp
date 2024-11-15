#include "duckdb.hpp"
#include <string>
#include <iostream>
#include <fstream>

using namespace duckdb;

const int N = 99;
const int repeat = 1;

int main(int argc, char**argv) {
    std::string dbfilepath = "/home/junyi/PredTransDuckDB/tpcds_10.db";
    int query_id = atoi(argv[1]);
    for(int i = query_id; i <= query_id; i++) {
        std::cout << i << std::endl;
        FILE* fp = fopen("result.txt", "a+");
        fprintf(fp, "----------------------------query %d----------------------------\n", query_id);
        fclose(fp);
        std::string create_tmp_tbl_file = "/home/junyi/PredTransDuckDB/tpc-ds/in_mem/query";
        create_tmp_tbl_file = create_tmp_tbl_file + to_string(i) + ".sql";
        std::ifstream create_tmp_tbl_fp(create_tmp_tbl_file);
        std::string create_tmp_tbl;
        if (create_tmp_tbl_fp.is_open()) {
            while (std::getline(create_tmp_tbl_fp, create_tmp_tbl)) {
            }
            create_tmp_tbl_fp.close();
        } else {
            std::cout << "Cannot open file " << create_tmp_tbl_file << std::endl;
        }
        DuckDB db(dbfilepath.c_str());
        Connection con(db);
        auto result = con.Query(create_tmp_tbl);
        if(result->HasError()) {
            std::cout << result->GetError() << "\n";
        }
        result.release();
        std::string tpcds_file = "/home/junyi/PredTransDuckDB/tpc-ds/query";
        tpcds_file = tpcds_file + to_string(i) + ".sql";
        std::ifstream tpcds_fp(tpcds_file);
        std::string tpcds;
        std::string tmp;
        if (tpcds_fp.is_open()) {
            while (std::getline(tpcds_fp, tmp)) {
                tpcds += tmp;
                tpcds += " ";
            }
            tpcds_fp.close();
        } else {
            std::cout << "Cannot open file " << tpcds_file << std::endl;
        }
        result = con.Query("SET THREADS = 1;");
        if(result->HasError()) {
            std::cout << result->GetError() << "\n";
        }
        result.release();
        for (int j = 0; j < repeat; j++) {
            result = con.Query(tpcds);
            if(result->HasError()) {
                std::cout << result->GetError() << "\n";
            }
            result.release();
        }
    }
    return 0;
}
