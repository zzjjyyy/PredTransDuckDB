#include "duckdb/optimizer/predicate_transfer/dag.hpp"

namespace duckdb {
    void DAGNode::AddIn(idx_t from, Expression* filter) {
        for (auto &node : in_) {
            if(node->GetDest() == from) {
                node->Push(filter);
                return;
            }
        }
        auto node = make_uniq<DAGEdge>(from);
        node->Push(filter);
        in_.emplace_back(std::move(node));
        return;
    }

    void DAGNode::AddIn(idx_t from, shared_ptr<BlockedBloomFilter> bloom_filter) {
        for (auto &node : in_) {
            if(node->GetDest() == from) {
                node->Push(bloom_filter);
                return;
            }
        }
        auto node = make_uniq<DAGEdge>(from);
        node->Push(bloom_filter);
        in_.emplace_back(std::move(node));
        return;
    }

    void DAGNode::AddOut(idx_t to, Expression* filter) {
        for (auto &node : out_) {
            if(node->GetDest() == to) {
                node->Push(filter);
                return;
            }
        }
        auto node = make_uniq<DAGEdge>(to);
        node->Push(std::move(filter));
        out_.emplace_back(std::move(node));
        return;
    }

    void DAGNode::AddOut(idx_t to, shared_ptr<BlockedBloomFilter> bloom_filter) {
        for (auto &node : out_) {
            if(node->GetDest() == to) {
                node->Push(bloom_filter);
                return;
            }
        }
        auto node = make_uniq<DAGEdge>(to);
        node->Push(bloom_filter);
        out_.emplace_back(std::move(node));
        return;
    }
}