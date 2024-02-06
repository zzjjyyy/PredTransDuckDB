add_library_unity(
  duckdb_bloom_filter
  OBJECT
  bloom_filter_base.cpp
  bloom_filter_create_abstract_kernel.cpp
  bloom_filter_create_kernel.cpp
  bloom_filter_use_kernel.cpp
  bloom_filter.cpp
  universal_hash_function.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_bloom_filter>
    PARENT_SCOPE)