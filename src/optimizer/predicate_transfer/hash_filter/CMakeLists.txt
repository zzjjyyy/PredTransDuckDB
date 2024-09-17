add_library_unity(
  duckdb_hash_filter
  OBJECT
  hash_filter_use_kernel.cpp
  hash_filter.cpp
  hashtable.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_hash_filter>
    PARENT_SCOPE)