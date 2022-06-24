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
    enum class entry_type : std::uint8_t {
        this_id_is_not_used,
        normal_entry,
        marker_begin,
        marker_end,
    };
    
    log_entry() = default;


    static void begin_session(boost::filesystem::ofstream& strm, epoch_id_type epoch) {
        entry_type type = entry_type::marker_begin;
        strm.write((char*)&type, sizeof(entry_type));
        strm.write((char*)&epoch, sizeof(epoch_id_type));
    }
    static void end_session(boost::filesystem::ofstream& strm, epoch_id_type epoch) {
        entry_type type = entry_type::marker_end;
        strm.write((char*)&type, sizeof(entry_type));
        strm.write((char*)&epoch, sizeof(epoch_id_type));
    }

// for writer (entry)
    void write(boost::filesystem::ofstream& strm) {
        switch(entry_type_) {
        case entry_type::normal_entry:
            write(strm, storage_id_, key_, value_, write_version_);
            break;
        case entry_type::marker_begin:
            begin_session(strm, epoch_id_);
            break;
        case entry_type::marker_end:
            end_session(strm, epoch_id_);
            break;
        case entry_type::this_id_is_not_used:
            break;
        }
    }

    static void write(boost::filesystem::ofstream& strm, storage_id_type storage_id, std::string_view key, std::string_view value, write_version_type write_version) {
        entry_type type = entry_type::normal_entry;
        strm.write((char*)&type, sizeof(entry_type));

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
    bool read(boost::filesystem::ifstream& strm) {
        strm.read((char*)&entry_type_, sizeof(entry_type));
        if (strm.eof()) {
            return false;
        }

        switch(entry_type_) {
        case entry_type::normal_entry:
            std::int32_t key_len;
            strm.read((char*)&key_len, sizeof(std::int32_t));
            std::int32_t value_len;
            strm.read((char*)&value_len, sizeof(std::int32_t));

            strm.read((char*)&write_version_.epoch_number_, sizeof(epoch_id_type));
            strm.read((char*)&write_version_.minor_write_version_, sizeof(std::uint64_t));

            strm.read((char*)&storage_id_, sizeof(storage_id_type));
            key_.resize(key_len);
            strm.read((char*)key_.data(), key_len);
            value_.resize(value_len);
            strm.read((char*)value_.data(), value_len);
            break;

        case entry_type::marker_begin:
        case entry_type::marker_end:
            strm.read((char*)&epoch_id_, sizeof(epoch_id_type));
            break;

        case entry_type::this_id_is_not_used:
            return false;
        }

        return true;
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
    void write_version(write_version_type& buf) {
        buf = write_version_;
    }
    entry_type type() {
        return entry_type_;
    }
    epoch_id_type epoch_id() {
        return epoch_id_;
    }

private:
    entry_type entry_type_{};
    epoch_id_type epoch_id_{};
    storage_id_type storage_id_{};
    std::string key_{};
    std::string value_{};
    write_version_type write_version_{};
};

} // namespace limestone::api
