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
#include <thread>
#include <chrono>

#include <limestone/api/datastore.h>
#include <iostream>  // FIXME

namespace limestone::api {

datastore::datastore() : datastore(configuration()) {}

datastore::datastore([[maybe_unused]] configuration conf) {}

datastore::~datastore() {}

void datastore::recover() {}

void datastore::ready() {}

snapshot* datastore::get_snapshot() {
    return snapshot_.get();
}

std::shared_ptr<snapshot> datastore::shared_snapshot() { return nullptr; }

log_channel& datastore::create_channel(boost::filesystem::path location) {
    std::lock_guard<std::mutex> lock(mtx_);
    
    auto id = log_channel_id_.fetch_add(1);
    log_channels_.emplace_back(std::make_unique<log_channel>(location, id, this));
    return *log_channels_.at(id);
}

epoch_id_type datastore::last_epoch() { return 0; }

void datastore::switch_epoch(epoch_id_type epoch_id) {
    epoch_id_ = epoch_id;
}

void datastore::add_persistent_callback(std::function<void(epoch_id_type)> callback) {
    persistent_callback_ = callback;
}

void datastore::switch_safe_snapshot([[maybe_unused]] write_version_type write_version, [[maybe_unused]] bool inclusive) {}

void datastore::add_snapshot_callback(std::function<void(write_version_type)> callback) {
    snapshot_callback_ = callback;
}

std::future<void> datastore::shutdown() {
    return std::async(std::launch::async, []{ std::this_thread::sleep_for(std::chrono::seconds(1)); });
}

backup& datastore::begin_backup() {
    backup_ = std::make_unique<backup>(files_);
    return *backup_;
}

tag_repository& datastore::epoch_tag_repository() {
    return tag_repository_;
}

void datastore::recover([[maybe_unused]] epoch_tag tag) {}

void datastore::add_file(boost::filesystem::path file) {
    files_.insert(file);
}

} // namespace limestone::api
