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
#include <sstream>
#include <iomanip>
#include <iostream>  // FIXME

#include <limestone/api/log_channel.h>

#include <limestone/api/datastore.h>

namespace limestone::api {

log_channel::log_channel(boost::filesystem::path location, std::size_t id, datastore* envelope)
    : envelope_(envelope), location_(location), id_(id)
{
    std::stringstream ss;
    ss << "pwal_" << std::setw(4) << std::setfill('0') << std::dec << id_;
    file_ = ss.str();
}

void log_channel::begin_session() {
    auto log_file = file_path();
    strm_.open(log_file, std::ios_base::out | std::ios_base::app | std::ios_base::binary );
    envelope_->add_file(log_file);
}

void log_channel::end_session() {
    strm_.flush();
    strm_.close();
}

void log_channel::abort_session([[maybe_unused]] error_code_type error_code, [[maybe_unused]] std::string message) {
}

void log_channel::add_entry(storage_id_type storage_id, std::string_view key, std::string_view value, write_version_type write_version) {
    std::int32_t key_len = key.length();
    strm_.write((char*)&key_len, sizeof(std::int32_t));
    std::int32_t value_len = value.length();
    strm_.write((char*)&value_len, sizeof(std::int32_t));

    strm_.write((char*)&write_version.epoch_number_, sizeof(epoch_t));
    strm_.write((char*)&write_version.minor_write_version_, sizeof(std::uint64_t));

    strm_.write((char*)&storage_id, sizeof(storage_id_type));
    strm_.write((char*)key.data(), key_len);
    strm_.write((char*)value.data(), value_len);
}

boost::filesystem::path log_channel::file_path() {
    return location_ / file_;
}

} // namespace limestone::api
