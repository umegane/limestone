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
#include <atomic>

#include <boost/filesystem/path.hpp>

#include <limestone/api/backup.h>
#include <limestone/api/log_channel.h>
#include <limestone/api/configuration.h>
#include <limestone/api/snapshot.h>
#include <limestone/api/epoch_id_type.h>
#include <limestone/api/write_version_type.h>
#include <limestone/api/tag_repository.h>

namespace limestone::api {

/**
 * @brief datastore interface to start/stop the services, store log, create snapshot for recover from log files
 * @details this object is not thread-safe except for create_channel().
 */
class datastore {
    friend class log_channel;

public:
    /**
     * @brief create empty object
     */
    datastore();

    /**
     * @brief create empty object
     * @param conf a reference to a configuration object used in the object construction
     */
    explicit datastore(configuration const& conf);

    /**
     * @brief destruct the object
     */
    ~datastore();

    /**
     * @brief create snapshot from log files stored in location_
     * @details file name of snapshot to be created is snapshot::file_name_ which is stored in location_ / snapshot::subdirectory_name_.
     * If location_ / snapshot::subdirectory_name_ / snapshot::file_name_ is exist, do nothing.
     * @attention this function is not thread-safe.
     */
    void recover();

    /**
     * @brief create snapshot from log files stored in form directory
     * @details file name of snapshot to be created is snapshot::file_name_ which is stored in location_ / snapshot::subdirectory_name_.
     * @param from the location of log files
     * @param overwrite location_ / snapshot::subdirectory_name_ / snapshot::file_name_ is overwritten
     * @attention this function is not thread-safe.
     */
    void recover(std::string_view from, bool overwrite);

    /**
     * @brief transition this object to an operational state
     * @details after this method is called, create_channel() can be invoked.
     * @attention this function is not thread-safe, and the from directory must not contain any files other than log files.
     */
    void ready();

    /**
     * @brief provides a pointer of the snapshot object
     * @details snapshot used is location_ / snapshot::subdirectory_name_ / snapshot::file_name_
     * @return a pointer to the object associated with the latest available snapshot
     */
    snapshot* get_snapshot();

    /**
     * @brief provides a shared pointer of the snapshot object
     * @details snapshot is location_ / snapshot::subdirectory_name_ / snapshot::file_name_
     * @return a shared pointer to the object associated with the latest available snapshot
     */
    std::shared_ptr<snapshot> shared_snapshot();

    /**
     * @brief create a log_channel to write logs to a file
     * @details logs are written to separate files created for each channel
     * @param location specifies the directory of the log files
     * @return the reference of the log_channel
     * @attention this function should be called before the ready() is called.
     */
    log_channel& create_channel(boost::filesystem::path location);

    /**
     * @brief provide the largest epoch ID
     * @return the largest epoch ID that has been successfully persisted
     * @note designed to make epoch ID monotonic across reboots
     */
    epoch_id_type last_epoch();

    /**
     * @brief change the current epoch ID
     * @param new epoch id which must be greater than current epoch ID.
     * @attention this function should be called after the ready() is called.
     */
    void switch_epoch(epoch_id_type epoch_id);

    /**
     * @brief register a callback on successful persistence
     * @param callback a pointer to the callback function
     * @attention this function should be called before the ready() is called.
     */
    void add_persistent_callback(std::function<void(epoch_id_type)> callback);

    /**
     * @brief notify this of the location of available safe snapshots
     * @param write_version specifies the location (write_version) of available safe snapshots,
     * consisting of major and minor versions where major version should be less than or equal to the current epoch ID.
     * @param inclusive specifies the parameter write_version is inclusive or not
     * @attention this function should be called after the ready() is called.
     * @note the actual safe snapshot location can be checked via add_safe_snapshot_callback
     * @note immediately after datastore::ready(), the last_epoch is treated as the maximum write version
     * with last_epoch as the write major version.
     */
    void switch_safe_snapshot(write_version_type write_version, bool inclusive);

    /**
     * @brief register a callback to be called when the safe snapshot location is changed internally
     * @param callback a pointer to the callback function
     * @attention this function should be called before the ready() is called.
     */
    void add_snapshot_callback(std::function<void(write_version_type)> callback);

    /**
     * @brief prohibits new persistent sessions from starting thereafter
     * @detail move to the stop preparation state.
     * @return the future of void, which allows get() after the transition to the stop preparation state.
     */
    std::future<void> shutdown();

    /**
     * @brief start backup operation
     * @detail a backup object is created, which contains a list of log files.
     * @return a reference to the backup object.
     */
    backup& begin_backup();

    /**
     * @brief provide epoch tag repository
     * @return a reference to the epoch tag repository
     * @note available both before and after ready() call
     */
    tag_repository& epoch_tag_repository();

    /**
     * @brief rewinds the state of the data store to the point in time of the specified epoch
     * @detail create a snapshot file for the specified epoch.
     * @attention this function should be called before the ready() is called.
     */
    void recover(epoch_tag);

protected:
    std::vector<std::unique_ptr<log_channel>> log_channels_;  // place in protectes region for tests
    
private:
    boost::filesystem::path location_{};

    std::atomic_uint64_t epoch_id_switched_{};

    std::atomic_uint64_t epoch_id_informed_{};

    std::unique_ptr<backup> backup_{};

    std::shared_ptr<snapshot> snapshot_{};
 
    std::function<void(epoch_id_type)> persistent_callback_;

    std::function<void(write_version_type)> snapshot_callback_;

    tag_repository tag_repository_{};

    std::atomic_uint64_t log_channel_id_{};

    std::set<boost::filesystem::path> files_{};

    std::mutex mtx_channel_{};
    std::mutex mtx_files_{};

    void add_file(boost::filesystem::path file);

    void update_min_epoch_id();
    epoch_id_type search_min_epoch_id();

    bool ready_{};
    
    void check_after_ready(const char* func);

    void check_before_ready(const char* func);
};

} // namespace limestone::api
