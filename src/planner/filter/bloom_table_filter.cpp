#include "duckdb/planner/filter/bloom_table_filter.hpp"

namespace duckdb {

string BloomTableFilter::ToString(const string &column_name) {
	return column_name + " Bloom Filter";
}
}