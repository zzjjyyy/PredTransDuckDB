cmake_minimum_required(VERSION 2.8.12)
project(SQL)

set(CMAKE_CXX_STANDARD 17)

include_directories(../PredTransDuckDB/src/include) # You may have to chage the include_directory
include_directories(../PredTransDuckDB/third_party/pcg)
link_directories(../PredTransDuckDB/src)

add_executable(test main.cpp)
target_link_libraries(test duckdb)
