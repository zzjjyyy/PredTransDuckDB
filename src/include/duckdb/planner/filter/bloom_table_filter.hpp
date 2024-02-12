#pragma once

#include "duckdb/planner/table_filter.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/optimizer/predicate_transfer/bloom_filter/bloom_filter.hpp"

namespace duckdb {

class BloomTableFilter : public TableFilter {
public:
	static constexpr const TableFilterType TYPE = TableFilterType::BLOOM_FILTER;

public:
	BloomTableFilter(TableFilterType filter_type_p, BlockedBloomFilter* bloom_filter_p)
    : TableFilter(filter_type_p), bloom_filter(bloom_filter_p) {
	}

	virtual ~BloomTableFilter() {
	}

	//! The filters of this conjunction
	BlockedBloomFilter* bloom_filter;

public:
	virtual FilterPropagateResult CheckStatistics(BaseStatistics &stats) {
        return FilterPropagateResult::NO_PRUNING_POSSIBLE;
    }
    
	virtual string ToString(const string &column_name) override;
	
	virtual bool Equals(const TableFilter &other) const {
		return TableFilter::Equals(other);
	}
};
}