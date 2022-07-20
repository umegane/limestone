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

#include <limestone/api/log_channel.h>

#include <limestone/api/datastore.h>
#include "log_entry.h"

namespace limestone::api {

log_channel::log_channel(boost::filesystem::path location, std::size_t id, datastore* envelope)
    : envelope_(envelope), location_(location), id_(id)
{
    std::stringstream ss;
    ss << prefix << std::setw(4) << std::setfill('0') << std::dec << id_;
    file_ = ss.str();
}

void log_channel::begin_session() {
    do {
        current_epoch_id_.store(envelope_->epoch_id_switched_.load());
        std::atomic_thread_fence(std::memory_order_acq_rel);
    } while (current_epoch_id_.load() != envelope_->epoch_id_switched_.load());

    auto log_file = file_path();
    strm_.open(log_file, std::ios_base::out | std::ios_base::app | std::ios_base::binary );
    if (!registered_) {
        envelope_->add_file(log_file);
        registered_ = true;
    }
    log_entry::begin_session(strm_, static_cast<epoch_id_type>(current_epoch_id_.load()));
}

void log_channel::end_session() {
    strm_.flush();
    finished_epoch_id_.store(current_epoch_id_.load());
    current_epoch_id_.store(UINT64_MAX);
    envelope_->update_min_epoch_id();
    strm_.close();
}

void log_channel::abort_session([[maybe_unused]] status status_code, [[maybe_unused]] std::string message) {
}

void log_channel::add_entry(storage_id_type storage_id, std::string_view key, std::string_view value, write_version_type write_version) {
    log_entry::write(strm_, storage_id, key, value, write_version);
    write_version_ = write_version;
}

boost::filesystem::path log_channel::file_path() {
    return location_ / file_;
}

} // namespace limestone::api
