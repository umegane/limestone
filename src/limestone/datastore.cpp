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
#include <limestone/api/datastore.h>
#include <iostream>  // FIXME

namespace limestone::api {

datastore::datastore([[maybe_unused]] configuration conf) {
}

datastore::~datastore() {}

void datastore::recover() {}

void datastore::ready() {}

// snapshot& datastore::get_snapshot() {}

std::shared_ptr<snapshot> datastore::shared_snapshot() { return nullptr; }

log_channel& datastore::create_channel(boost::filesystem::path location) {
    std::cout << __func__ << ":" << location.string() << std::endl;
    auto ch = std::make_unique<log_channel>(location, this);
    auto& rv = *ch;
    channels_.emplace(std::move(ch));
    return rv;
}

epoch_id_type datastore::last_epoch() { return 0; }

void datastore::switch_epoch(epoch_id_type epoch_id) {
    std::cout << __func__ << ":" << epoch_id << std::endl;
}

void datastore::add_persistent_callback(std::function<void(epoch_id_type)> callback) {
    persistent_callback_ = callback;
}

void datastore::switch_safe_snapshot([[maybe_unused]] write_version_type write_version, [[maybe_unused]] bool inclusive) {}

void datastore::add_snapshot_callback(std::function<void(write_version_type)> callback) {
    snapshot_callback_ = callback;
}

// std::future<void> datastore::shutdown() {}

backup& datastore::begin_backup() {
    backup_ = std::make_unique<backup>();
    return *backup_;
}

tag_repository& datastore::epoch_tag_repository() {
    return tag_repository_;
}

void datastore::recover([[maybe_unused]] epoch_tag tag) {}

void datastore::erase_log_channel(log_channel* lc) {
    if (auto itr = channels_.find(lc); itr != channels_.end()) {
        channels_.erase(itr);
    }
}

} // namespace limestone::api
