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
#pragma once

#include <string>
#include <string_view>

#include <boost/filesystem/path.hpp>
#include <boost/filesystem/fstream.hpp>

#include <limestone/api/storage_id_type.h>
#include <limestone/api/write_version_type.h>

namespace limestone::api {

class datastore;

class log_entry {
public:
    log_entry() = default;

// for writer
    void write(boost::filesystem::ofstream& strm) {
        write(strm, storage_id_, key_, value_, write_version_);
    }

    static void write(boost::filesystem::ofstream& strm, storage_id_type storage_id, std::string_view key, std::string_view value, write_version_type write_version) {
        std::int32_t key_len = key.length();
        strm.write((char*)&key_len, sizeof(std::int32_t));
        std::int32_t value_len = value.length();
        strm.write((char*)&value_len, sizeof(std::int32_t));

        strm.write((char*)&write_version.epoch_number_, sizeof(epoch_id_type));
        strm.write((char*)&write_version.minor_write_version_, sizeof(std::uint64_t));

        strm.write((char*)&storage_id, sizeof(storage_id_type));
        strm.write((char*)key.data(), key_len);
        strm.write((char*)value.data(), value_len);
    }

// for reader
    log_entry* read(boost::filesystem::ifstream& strm) {
        std::int32_t key_len;
        strm.read((char*)&key_len, sizeof(std::int32_t));
        if (strm.eof()) {
            return nullptr;
        }
        
        std::int32_t value_len;
        strm.read((char*)&value_len, sizeof(std::int32_t));

        strm.read((char*)&write_version_.epoch_number_, sizeof(epoch_id_type));
        strm.read((char*)&write_version_.minor_write_version_, sizeof(std::uint64_t));

        strm.read((char*)&storage_id_, sizeof(storage_id_type));
        key_.resize(key_len);
        strm.read((char*)key_.data(), key_len);
        value_.resize(value_len);
        strm.read((char*)value_.data(), value_len);

        return this;
    }

    storage_id_type storage() {
        return storage_id_;
    }
    void key(std::string& buf) {
        buf = key_;
    }
    void value(std::string& buf) {
        buf = value_;
    }

private:
    storage_id_type storage_id_{};
    std::string key_{};
    std::string value_{};
    write_version_type write_version_{};
};

} // namespace limestone::api
