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

#include <boost/filesystem/operations.hpp>
#include <boost/foreach.hpp>

#include <limestone/api/datastore.h>
#include "log_entry.h"

#include "glog/logging.h"

namespace limestone::api {

datastore::datastore() {}

datastore::datastore(configuration const& conf) {
    location_ = conf.data_locations_.at(0);
    boost::system::error_code error;
    const bool result_check = boost::filesystem::exists(location_, error);
    snapshot_ = std::make_shared<snapshot>(location_);
    if (!result_check || error) {
        const bool result_mkdir = boost::filesystem::create_directory(location_, error);
        if (!result_mkdir || error) {
            LOG(ERROR) << "fail to create directory: result_mkdir: " << result_mkdir << ", error_code: " << error << ", path: " << location_;
            std::abort();
        }
    }
}

datastore::~datastore() = default;

void datastore::recover() {
    check_before_ready(__func__);
    
    auto file = snapshot_->file_path();
    if (!boost::filesystem::exists(file)) {
        recover(location_.string(), false);
    }
}
void datastore::recover(std::string_view from, [[maybe_unused]] bool overwrite) {
    auto from_dir = boost::filesystem::path(std::string(from));

    snapshot_ = std::make_unique<snapshot>(location_);
    boost::filesystem::ofstream ostrm{};
    ostrm.open(snapshot_->file_path(), std::ios_base::out | std::ios_base::app | std::ios_base::binary);
    BOOST_FOREACH(const boost::filesystem::path& p, std::make_pair(boost::filesystem::directory_iterator(from_dir), boost::filesystem::directory_iterator())) {
        if (!boost::filesystem::is_directory(p)) {
            boost::filesystem::ifstream istrm;
            istrm.open(p, std::ios_base::in | std::ios_base::binary);
            for (log_entry e{}; e.read(istrm); e.write(ostrm));
            istrm.close();
        }
    }
    ostrm.close();
}

void datastore::ready() {
    ready_ = true;
}

snapshot* datastore::get_snapshot() {
    check_after_ready(__func__);
    return snapshot_.get();
}

std::shared_ptr<snapshot> datastore::shared_snapshot() {
    check_after_ready(__func__);
    return snapshot_;
}

log_channel& datastore::create_channel(boost::filesystem::path location) {
    check_before_ready(__func__);
    
    std::lock_guard<std::mutex> lock(mtx_channel_);
    
    auto id = log_channel_id_.fetch_add(1);
    log_channels_.emplace_back(std::make_unique<log_channel>(location, id, this));
    return *log_channels_.at(id);
}

epoch_id_type datastore::last_epoch() { return static_cast<epoch_id_type>(epoch_id_informed_.load()); }

void datastore::switch_epoch(epoch_id_type new_epoch_id) {
    check_after_ready(__func__);

    auto neid = static_cast<std::uint64_t>(new_epoch_id);
    if (neid == 0) {
        LOG(WARNING) << "switch to epoch_id_type of " << neid << " is curious";
    }

    epoch_id_switched_.store(neid);
    if (epoch_id_informed_.load() < (neid - 1)) {
        update_min_epoch_id();
    }
}

void datastore::update_min_epoch_id() {
    std::uint64_t min_epoch = static_cast<std::uint64_t>(search_min_epoch_id());
    std::uint64_t old_epoch_id = epoch_id_informed_.load();
    while (old_epoch_id < min_epoch) {
        if (epoch_id_informed_.compare_exchange_strong(old_epoch_id, min_epoch)) {
            if (persistent_callback_) {
                persistent_callback_(min_epoch);
            }
            return;
        }
    }
}

epoch_id_type datastore::search_min_epoch_id() {
    epoch_id_type min_epoch = static_cast<epoch_id_type>(epoch_id_switched_.load());

    for (const auto& e : log_channels_) {
        auto lc_epoch = static_cast<epoch_id_type>(e->current_epoch_id_.load());
        if (lc_epoch < min_epoch) {
            min_epoch = lc_epoch;
        }
    }
    return min_epoch - 1;
}

void datastore::add_persistent_callback(std::function<void(epoch_id_type)> callback) {
    check_before_ready(__func__);
    persistent_callback_ = callback;
}

void datastore::switch_safe_snapshot([[maybe_unused]] write_version_type write_version, [[maybe_unused]] bool inclusive) {
    check_after_ready(__func__);
}

void datastore::add_snapshot_callback(std::function<void(write_version_type)> callback) {
    check_before_ready(__func__);
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

void datastore::recover([[maybe_unused]] epoch_tag tag) {
    check_before_ready(__func__);
}

void datastore::add_file(boost::filesystem::path file) {
    std::lock_guard<std::mutex> lock(mtx_files_);

    files_.insert(file);
}

void datastore::check_after_ready(const char* func) {
    if (!ready_) {
        LOG(ERROR) << func << " called before ready()";
    }
}

void datastore::check_before_ready(const char* func) {
    if (ready_) {
        LOG(ERROR) << func << " called after ready()";
    }
}

} // namespace limestone::api
