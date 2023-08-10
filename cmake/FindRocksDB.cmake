if(TARGET RocksDB::RocksDB)
    return()
endif()

find_library(RocksDB_LIBRARY_FILE NAMES rocksdb)
find_path(RocksDB_INCLUDE_DIR NAMES rocksdb/db.h)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(RocksDB DEFAULT_MSG
    RocksDB_LIBRARY_FILE
    RocksDB_INCLUDE_DIR)

if(RocksDB_LIBRARY_FILE AND RocksDB_INCLUDE_DIR)
    set(RocksDB_FOUND ON)
    add_library(RocksDB::RocksDB SHARED IMPORTED)
    set_target_properties(RocksDB::RocksDB PROPERTIES
        IMPORTED_LOCATION "${RocksDB_LIBRARY_FILE}"
        INTERFACE_INCLUDE_DIRECTORIES "${RocksDB_INCLUDE_DIR}")
else()
    set(RocksDB_FOUND OFF)
endif()

unset(RocksDB_LIBRARY_FILE CACHE)
unset(RocksDB_INCLUDE_DIR CACHE)
