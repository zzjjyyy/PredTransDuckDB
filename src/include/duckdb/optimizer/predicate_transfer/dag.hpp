#pragma once

#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/optimizer/predicate_transfer/bloom_filter/bloom_filter.hpp"

namespace duckdb {
class DAGEdge;

class DAGNode {
public:
    DAGNode(idx_t id) : id(id) {
    }

    idx_t Id() {
        return id;
    }

    void AddIn(idx_t from, Expression* filter);

    void AddIn(idx_t from, BlockedBloomFilter *bloom_filter);

    void AddOut(idx_t to, Expression* filter);

    void AddOut(idx_t to, BlockedBloomFilter *bloom_filter);

    vector<unique_ptr<DAGEdge>> in_;

    vector<unique_ptr<DAGEdge>> out_;

private:
    idx_t id;
};

class DAGEdge{
public:
    DAGEdge(idx_t id) : dest_(id) {
    }

    void Push(Expression* filter) {
        filters.emplace_back(filter);
    }

    void Push(BlockedBloomFilter *bloom_filter) {
        bloom_filters.emplace_back(bloom_filter);
    }
    
    idx_t GetDest() {
        return dest_;
    }

    vector<Expression*> filters;

    vector<BlockedBloomFilter*> bloom_filters;

    idx_t dest_;
};

class DAG {
public:
    unordered_map<int, unique_ptr<DAGNode>> nodes;
};
}