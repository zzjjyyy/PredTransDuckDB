#include "duckdb/optimizer/predicate_transfer/bloom_filter/universal_hash_function.hpp"
#include <fmt/format.h>
#include <primesieve.hpp>
#include <random>
#include <cassert>

namespace duckdb {

UniversalHashFunction::UniversalHashFunction(int64_t m) {

  assert(m > 0);

  std::random_device rd;
  std::mt19937 gen(rd());

  m_ = m;

  // Pick a random integer greater than or equal to m (upper bound of 2m)
  int64_t i = std::uniform_int_distribution<int64_t>(m_, 2 * m_)(gen);

  // Get the 1st prime greater than or equal to i
  p_ = primesieve::nth_prime(i);

  // Set a = random int (less than p) (where a != 0)
  a_ = std::uniform_int_distribution<int64_t>(1, p_ - 1)(gen);

  // Set b = random int (less than p)
  b_ = std::uniform_int_distribution<int64_t>(0, p_ - 1)(gen);
}

std::shared_ptr<UniversalHashFunction> UniversalHashFunction::make(int64_t m) {
  return std::make_shared<UniversalHashFunction>(m);
}

UniversalHashFunction::UniversalHashFunction(int64_t a, int64_t b, int64_t m, int64_t p):
  a_(a),
  b_(b),
  m_(m),
  p_(p) {}

std::shared_ptr<UniversalHashFunction> UniversalHashFunction::make(int64_t a, int64_t b, int64_t m, int64_t p) {
  return std::make_shared<UniversalHashFunction>(a, b, m, p);
}

int64_t UniversalHashFunction::hash(int64_t x) const {
  uint64_t sum = a_ * x + b_;   // prevent overflow
  int64_t h = (sum % p_) % m_;

  assert(h >= 0 && h <= m_);
  return h;
}
}