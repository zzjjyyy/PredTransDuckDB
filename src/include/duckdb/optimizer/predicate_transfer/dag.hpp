#pragma once

#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/optimizer/predicate_transfer/bloom_filter/bloom_filter.hpp"
#include "duckdb/optimizer/predicate_transfer/hash_filter/hash_filter.hpp"

namespace duckdb {
class DAGEdge;

class DAGNode {
public:
    DAGNode(idx_t id, idx_t estimated_cardinality, bool root) : id(id), size(estimated_cardinality), root(root) {
    }

    idx_t Id() {
        return id;
    }

    void AddIn(idx_t from, Expression* filter,  bool forward);

    /* Hash Filter or Bloom Filter */
    // void AddIn(idx_t from, shared_ptr<HashFilter> bloom_filter, bool forward);
    void AddIn(idx_t from, shared_ptr<BlockedBloomFilter> bloom_filter, bool forward);

    void AddOut(idx_t to, Expression* filter, bool forward);

    /* Hash Filter or Bloom Filter */
    // void AddOut(idx_t to, shared_ptr<HashFilter> bloom_filter, bool forward);
    void AddOut(idx_t to, shared_ptr<BlockedBloomFilter> bloom_filter, bool forward);

    vector<unique_ptr<DAGEdge>> forward_in_;
    vector<unique_ptr<DAGEdge>> backward_in_;
    vector<unique_ptr<DAGEdge>> forward_out_;
    vector<unique_ptr<DAGEdge>> backward_out_;

    bool root;

    int priority = -1;

    idx_t size;

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

    /* Hash Filter or Bloom Filter */
    // void Push(shared_ptr<HashFilter> bloom_filter) {
    void Push(shared_ptr<BlockedBloomFilter> bloom_filter) {
        bloom_filters.emplace_back(bloom_filter);
    }
    
    idx_t GetDest() {
        return dest_;
    }

    vector<Expression*> filters;
    
    /* Hash Filter or Bloom Filter */
    // vector<shared_ptr<HashFilter>> bloom_filters;
    vector<shared_ptr<BlockedBloomFilter>> bloom_filters;

    idx_t dest_;
};

class DAG {
public:
    unordered_map<int, unique_ptr<DAGNode>> nodes;
};
}