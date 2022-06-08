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

#include <memory>
#include <future>
#include <atomic>
#include <vector>
#include <set>
#include <mutex>

#include <boost/filesystem/path.hpp>

#include <limestone/api/backup.h>
#include <limestone/api/log_channel.h>
#include <limestone/api/configuration.h>
#include <limestone/api/snapshot.h>
#include <limestone/api/epoch_id_type.h>
#include <limestone/api/write_version_type.h>
#include <limestone/api/tag_repository.h>

namespace limestone::api {

class datastore {
    friend class log_channel;

public:
    datastore();
    explicit datastore(configuration const& conf);

    ~datastore();

    void recover();
    void recover(std::string_view from, bool overwrite);

    void ready();

    snapshot* get_snapshot();

    std::shared_ptr<snapshot> shared_snapshot();

    log_channel& create_channel(boost::filesystem::path location);

    epoch_id_type last_epoch();

    void switch_epoch(epoch_id_type epoch_id);

    void add_persistent_callback(std::function<void(epoch_id_type)> callback);

    void switch_safe_snapshot(write_version_type write_version, bool inclusive);

    void add_snapshot_callback(std::function<void(write_version_type)> callback);

    std::future<void> shutdown();

    backup& begin_backup();

    tag_repository& epoch_tag_repository();

    void recover(epoch_tag);

protected:
    std::vector<std::unique_ptr<log_channel>> log_channels_;  // place in protectes region for tests
    
private:
    boost::filesystem::path location_{};

    epoch_id_type epoch_id_{};

    std::unique_ptr<backup> backup_{};

    std::unique_ptr<snapshot> snapshot_{};

    std::function<void(epoch_id_type)> persistent_callback_;

    std::function<void(write_version_type)> snapshot_callback_;

    tag_repository tag_repository_{};

    std::atomic_ulong log_channel_id_{};

    std::set<boost::filesystem::path> files_{};

    std::mutex mtx_{};
    std::mutex mtx_set_{};

    void add_file(boost::filesystem::path file);
};

} // namespace limestone::api
