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

    void AddIn(idx_t from, unique_ptr<Expression> filter);

    void AddIn(idx_t from, shared_ptr<BloomFilter> bloom_filter);

    void AddOut(idx_t to, unique_ptr<Expression> filter);

    void AddOut(idx_t to, shared_ptr<BloomFilter> bloom_filter);

    vector<unique_ptr<DAGEdge>> in_;

    vector<unique_ptr<DAGEdge>> out_;

private:
    idx_t id;
};

class DAGEdge{
public:
    DAGEdge(idx_t id) : dest_(id) {
    }

    void Push(unique_ptr<Expression> filter) {
        filters.emplace_back(std::move(filter));
    }

    void Push(shared_ptr<BloomFilter> bloom_filter) {
        bloom_filters.emplace_back(bloom_filter);
    }
    
    idx_t GetDest() {
        return dest_;
    }

    vector<unique_ptr<Expression>> filters;

    vector<shared_ptr<BloomFilter>> bloom_filters;

    idx_t dest_;
};

class DAG {
public:
    vector<unique_ptr<DAGNode>> nodes;
};
}