/*
 * Copyright 2022-2022 tsurugi project.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
// #include <boost/filesystem/operations.hpp>

#ifdef SORT_METHOD_USE_ROCKSDB
#include <rocksdb/db.h>
namespace leveldb = rocksdb;
#else
#include <leveldb/db.h>
#endif

#include <glog/logging.h>

#include <limestone/logging.h>

namespace limestone::api {

static constexpr const std::string_view leveldb_dir = "leveldb";

// leveldb_wrapper : the wrapper for LevelDB or compatible one (e.g. RocksDB)
class leveldb_wrapper {
public:
    /**
     * @brief create new object
     * @param dir the directory where LevelDB files will be placed
     */
    explicit leveldb_wrapper(const boost::filesystem::path& dir) : lvldb_path_(dir / boost::filesystem::path(std::string(leveldb_dir))) {
        clear_directory();
        
        leveldb::Options options;
        options.create_if_missing = true;
        if (leveldb::Status status = leveldb::DB::Open(options, lvldb_path_.string(), &lvldb_); !status.ok()) {
            LOG_LP(ERROR) << "Unable to open/create LevelDB database, status = " << status.ToString();
            std::abort();
        }
    }

    /**
     * @brief destruct object
     */
    ~leveldb_wrapper() {
        delete lvldb_;
        clear_directory();
    }

    leveldb_wrapper() noexcept = delete;
    leveldb_wrapper(leveldb_wrapper const& other) noexcept = delete;
    leveldb_wrapper& operator=(leveldb_wrapper const& other) noexcept = delete;
    leveldb_wrapper(leveldb_wrapper&& other) noexcept = delete;
    leveldb_wrapper& operator=(leveldb_wrapper&& other) noexcept = delete;

    bool put(const std::string& key, const std::string& value) {
        leveldb::WriteOptions write_options{};
        auto status = lvldb_->Put(write_options, key, value);
        return status.ok();
    }

    bool get(const std::string& key, std::string* value) {
        leveldb::ReadOptions read_options{};
        auto status = lvldb_->Get(read_options, key, value);
        return status.ok();
    }

    void each(const std::function<void(std::string_view, std::string_view)>& fun) {
        leveldb::Iterator* it = lvldb_->NewIterator(leveldb::ReadOptions());  // NOLINT (typical usage of leveldb)
        for (it->SeekToFirst(); it->Valid(); it->Next()) {
            leveldb::Slice key = it->key();
            leveldb::Slice value = it->value();
            fun(std::string_view(key.data(), key.size()), std::string_view(value.data(), value.size()));
        }
        delete it;  // NOLINT (typical usage of leveldb)
    }
    
private:
    leveldb::DB* lvldb_{};
    boost::filesystem::path lvldb_path_;

    void clear_directory() const noexcept {
        if (boost::filesystem::exists(lvldb_path_)) {
            if (boost::filesystem::is_directory(lvldb_path_)) {
                boost::filesystem::remove_all(lvldb_path_);
            } else {
                LOG_LP(ERROR) << lvldb_path_.string() << " is not a directory";
                std::abort();
            }
        }
    }
};

} // namespace limestone::api
