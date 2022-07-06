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
        marker_durable,
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
    static void durable_epoch(boost::filesystem::ofstream& strm, epoch_id_type epoch) {
        entry_type type = entry_type::marker_durable;
        strm.write((char*)&type, sizeof(entry_type));
        strm.write((char*)&epoch, sizeof(epoch_id_type));
    }

// for writer (entry)
    void write(boost::filesystem::ofstream& strm) {
        switch(entry_type_) {
        case entry_type::normal_entry:
            write(strm, key_sid_, value_etc_);
            break;
        case entry_type::marker_begin:
            begin_session(strm, epoch_id_);
            break;
        case entry_type::marker_end:
            end_session(strm, epoch_id_);
            break;
        case entry_type::marker_durable:
            durable_epoch(strm, epoch_id_);
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

        strm.write((char*)&storage_id, sizeof(storage_id_type));
        strm.write((char*)key.data(), key_len);

        strm.write((char*)&write_version.epoch_number_, sizeof(epoch_id_type));
        strm.write((char*)&write_version.minor_write_version_, sizeof(std::uint64_t));
        strm.write((char*)value.data(), value_len);
    }

    static void write(boost::filesystem::ofstream& strm, std::string_view key_sid, std::string_view value_etc) {
        entry_type type = entry_type::normal_entry;
        strm.write((char*)&type, sizeof(entry_type));

        std::int32_t key_len = key_sid.length() - sizeof(storage_id_type);
        strm.write((char*)&key_len, sizeof(std::int32_t));

        std::int32_t value_len = value_etc.length() - (sizeof(epoch_id_type) + sizeof(std::uint64_t));
        strm.write((char*)&value_len, sizeof(std::int32_t));

        strm.write((char*)key_sid.data(), key_sid.length());
        strm.write((char*)value_etc.data(), value_etc.length());
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

            key_sid_.resize(key_len + sizeof(storage_id_type));
            strm.read((char*)key_sid_.data(), key_sid_.length());
            value_etc_.resize(value_len + sizeof(epoch_id_type) + sizeof(std::uint64_t));
            strm.read((char*)value_etc_.data(), value_etc_.length());
            break;

        case entry_type::marker_begin:
        case entry_type::marker_end:
        case entry_type::marker_durable:
            strm.read((char*)&epoch_id_, sizeof(epoch_id_type));
            break;

        case entry_type::this_id_is_not_used:
            return false;
        }

        return true;
    }

    void write_version(write_version_type& buf) {
        memcpy(reinterpret_cast<char*>(&buf), value_etc_.data(), sizeof(epoch_id_type) + sizeof(std::uint64_t));
    }
    storage_id_type storage() {
        storage_id_type storage_id;
        memcpy(reinterpret_cast<char*>(&storage_id), key_sid_.data(), sizeof(storage_id_type));
        return storage_id;
    }
    void value(std::string& buf) {
        buf = value_etc_.substr(sizeof(epoch_id_type) + sizeof(std::uint64_t));
    }
    void key(std::string& buf) {
        buf = key_sid_.substr(sizeof(storage_id_type));
    }
    entry_type type() {
        return entry_type_;
    }
    epoch_id_type epoch_id() {
        return epoch_id_;
    }

    // for the purpose of storing key_sid and value_etc into LevelDB
    std::string& value_etc() {
        return value_etc_;
    }
    std::string& key_sid() {
        return key_sid_;
    }
    static epoch_id_type write_version_epoch_number(std::string_view value_etc) {
        epoch_id_type epoch_id;
        memcpy(reinterpret_cast<char*>(&epoch_id), value_etc.data(), sizeof(epoch_id_type));
        return epoch_id;
    }
    static std::uint64_t write_version_minor_write_version(std::string_view value_etc) {
        std::uint64_t minor_write_version;
        memcpy(reinterpret_cast<char*>(&minor_write_version), value_etc.data() + sizeof(epoch_id_type), sizeof(std::uint64_t));
        return minor_write_version;
    }

private:
    entry_type entry_type_{};
    epoch_id_type epoch_id_{};
    std::string key_sid_{};
    std::string value_etc_{};
};

} // namespace limestone::api
