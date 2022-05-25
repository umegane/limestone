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

namespace limestone::api {

using namespace limestone::detail;

datastore::datastore([[maybe_unused]] configuration conf) {}

datastore::~datastore() {}

void datastore::recover() {}

void datastore::ready() {}

// snapshot& datastore::get_snapshot() {}

std::shared_ptr<snapshot> datastore::shared_snapshot() { return nullptr; }

log_channel& datastore::create_channel(boost::filesystem::path location) {
    channel_ = std::make_unique<log_channel>(location);
    return *channel_;
}

epoch_id_type datastore::last_epoch() { return 0; }

void datastore::switch_epoch([[maybe_unused]] epoch_id_type epoch_id) {}

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

// tag_repository& datastore::epoch_tag_repository() {}

void datastore::recover([[maybe_unused]] epoch_tag tag) {}

} // namespace limestone::api
