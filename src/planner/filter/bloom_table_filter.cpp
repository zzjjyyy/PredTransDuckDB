#include "duckdb/planner/filter/bloom_table_filter.hpp"

namespace duckdb {
unique_ptr<TableFilter> BloomTableFilter::Copy() {
	auto result = make_uniq<BloomTableFilter>(TableFilterType::BLOOM_FILTER, this->bloom_filter);
	return unique_ptr_cast<BloomTableFilter, TableFilter>(std::move(result));
}

string BloomTableFilter::ToString(const string &column_name) {
	return column_name + " Bloom Filter";
}
}