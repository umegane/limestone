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
#include <boost/filesystem/operations.hpp>
#include <boost/foreach.hpp>
#include <leveldb/db.h>

#include <glog/logging.h>
#include <limestone/logging.h>

#include <limestone/api/datastore.h>
#include "log_entry.h"

namespace limestone::api {

static constexpr const std::string_view leveldb_dir = "leveldb";  // NOLINT

class leveldb_wrapper {
public:
    /**
     * @brief create new object
     * @param dir the directory where LevelDB files will be placed
     */
    leveldb_wrapper(boost::filesystem::path dir) : lvldb_path_(dir / boost::filesystem::path(std::string(leveldb_dir))) {
        clear_directory();
        
        leveldb::Options options;
        options.create_if_missing = true;
        if (leveldb::Status status = leveldb::DB::Open(options, lvldb_path_.string(), &lvldb_); status.ok() == false) {
            LOG(ERROR) << "Unable to open/create LevelDB database, status = " << status.ToString();
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

    leveldb_wrapper() = delete;
    leveldb_wrapper(leveldb_wrapper const& other) = delete;
    leveldb_wrapper& operator=(leveldb_wrapper const& other) = delete;
    leveldb_wrapper(leveldb_wrapper&& other) noexcept = delete;
    leveldb_wrapper& operator=(leveldb_wrapper&& other) noexcept = delete;

    leveldb::DB* db() {
        return lvldb_;
    }
    
private:
    leveldb::DB* lvldb_{};
    boost::filesystem::path lvldb_path_;

    void clear_directory() {
        if (boost::filesystem::exists(lvldb_path_)) {
            if (boost::filesystem::is_directory(lvldb_path_)) {
                boost::filesystem::remove_all(lvldb_path_);
            } else {
                LOG(ERROR) << lvldb_path_.string() << " is not a directory";
                std::abort();
            }
        }
    }
};

static epoch_id_type last_durable_epoch(boost::filesystem::path file) {
    boost::filesystem::ifstream istrm;
    log_entry e;
    istrm.open(file, std::ios_base::in | std::ios_base::binary);
    while (e.read(istrm));
    istrm.close();
    return e.epoch_id();
}

void datastore::create_snapshot() {
    auto& from_dir = location_;
    auto lvldb = std::make_unique<leveldb_wrapper>(from_dir);

    epoch_id_type ld_epoch = last_durable_epoch(from_dir / boost::filesystem::path(std::string(epoch_file_name)));

    BOOST_FOREACH(const boost::filesystem::path& p, std::make_pair(boost::filesystem::directory_iterator(from_dir), boost::filesystem::directory_iterator())) {
        if (p.filename().string().substr(0, log_channel::prefix.length()).compare(log_channel::prefix) == 0) {
            DVLOG(log_debug) << "processing pwal file: " << p.filename().string();
            log_entry e;
            epoch_id_type current_epoch{UINT64_MAX};

            boost::filesystem::ifstream istrm;
            istrm.open(p, std::ios_base::in | std::ios_base::binary);
            while(e.read(istrm)) {
                switch(e.type()) {
                case log_entry::entry_type::marker_begin:
                {
                    current_epoch = e.epoch_id();
                    break;
                }
                case log_entry::entry_type::normal_entry:
                {
                    if(current_epoch <= ld_epoch) {
                        bool need_write = true;
                        std::string value;
                        write_version_type write_version;
                        e.write_version(write_version);

                        leveldb::ReadOptions read_options;
                        if (auto status = lvldb->db()->Get(read_options, e.key_sid(), &value); status.ok()) {
                            if ((log_entry::write_version_epoch_number(value) < write_version.epoch_number_) ||
                                ((log_entry::write_version_epoch_number(value) == write_version.epoch_number_) && (log_entry::write_version_minor_write_version(value) == write_version.minor_write_version_))) {
                                need_write = false;
                            }
                        }
                        if (need_write) {
                            leveldb::WriteOptions write_options;
                            lvldb->db()->Put(write_options, e.key_sid(), e.value_etc());
                        }
                    }
                    break;
                }
                default:
                    break;
                }
            }
            istrm.close();
        }
    }

    boost::filesystem::ofstream ostrm{};
    ostrm.open(snapshot_->file_path(), std::ios_base::out | std::ios_base::trunc | std::ios_base::binary);
    leveldb::Iterator* it = lvldb->db()->NewIterator(leveldb::ReadOptions());
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
        log_entry::write(ostrm, it->key().ToString(), it->value().ToString());
    }
    ostrm.close();
    delete it;
}

} // namespace limestone::api
