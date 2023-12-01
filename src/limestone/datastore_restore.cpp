/*
 * Copyright 2022-2023 Project Tsurugi.
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
#include <boost/filesystem/operations.hpp>
#include <boost/foreach.hpp>

#include <glog/logging.h>
#include <limestone/logging.h>
#include "logging_helper.h"

#include <limestone/api/datastore.h>
#include <limestone/status.h>
#include "internal.h"

namespace limestone::api {

status datastore::restore(std::string_view from, bool keep_backup) const noexcept {
    VLOG_LP(log_debug) << "restore begin, from directory = " << from << " , keep_backup = " << std::boolalpha << keep_backup;
    auto from_dir = boost::filesystem::path(std::string(from));

    // log_dir version check
    boost::filesystem::path manifest_path = from_dir / std::string(internal::manifest_file_name);
    std::string ver_err;
    if (!internal::is_supported_version(manifest_path, ver_err)) {  // notfound or invalid or unsupport
        LOG_LP(ERROR) << ver_err;
        return status::err_broken_data;
    }

    BOOST_FOREACH(const boost::filesystem::path& p, std::make_pair(boost::filesystem::directory_iterator(location_), boost::filesystem::directory_iterator())) {
        if(!boost::filesystem::is_directory(p)) {
            try {
                boost::filesystem::remove(p);
            } catch (boost::filesystem::filesystem_error& ex) {
                LOG_LP(ERROR) << ex.what() << " file = " << p.string();
                return status::err_permission_error;
            }
        }
    }

    BOOST_FOREACH(const boost::filesystem::path& p, std::make_pair(boost::filesystem::directory_iterator(from_dir), boost::filesystem::directory_iterator())) {
        try {
            boost::filesystem::copy_file(p, location_ / p.filename());
        }
        catch (boost::filesystem::filesystem_error& ex) {
            LOG_LP(ERROR) << ex.what() << " file = " << p.string();
            return status::err_permission_error;
        }
    }
    if (!keep_backup) {
        BOOST_FOREACH(const boost::filesystem::path& p, std::make_pair(boost::filesystem::directory_iterator(from_dir), boost::filesystem::directory_iterator())) {
            try {
                boost::filesystem::remove(p);
            }
            catch (boost::filesystem::filesystem_error& ex) {
                LOG_LP(WARNING) << ex.what() << " file = " << p.string();
            }
        }
    }
    return status::ok;
}

// prusik era
status datastore::restore(std::string_view from, std::vector<file_set_entry>& entries) {
    VLOG_LP(log_debug) << "restore (from prusik) begin, from directory = " << from;
    auto from_dir = boost::filesystem::path(std::string(from));

    // log_dir version check
    int manifest_count = 0;
    for (auto & ent : entries) {
        if (ent.destination_path().string() != internal::manifest_file_name) {
            continue;
        }
        boost::filesystem::path src{ent.source_path()};
        if (src.is_absolute()) {
            // use it
        } else {
            src = from_dir / src;
        }
        if (!boost::filesystem::exists(src) || !boost::filesystem::is_regular_file(src)) {
            LOG_LP(ERROR) << "file not found : file = " << src.string();
            return status::err_not_found;
        }
        std::string ver_err;
        if (!internal::is_supported_version(src, ver_err)) {
            LOG_LP(ERROR) << ver_err;
            return status::err_broken_data;
        }
        manifest_count++;
    }
    if (manifest_count < 1) {  // XXX: change to != 1 ??
        LOG_LP(ERROR) << "no manifest file in backup";
        return status::err_broken_data;
    }

    // purge logdir
    // FIXME: copied this code from (old) restore(), fix duplicate
    BOOST_FOREACH(const boost::filesystem::path& p, std::make_pair(boost::filesystem::directory_iterator(location_), boost::filesystem::directory_iterator())) {
        if(!boost::filesystem::is_directory(p)) {
            try {
                boost::filesystem::remove(p);
            } catch (boost::filesystem::filesystem_error& ex) {
                LOG_LP(ERROR) << ex.what() << " file = " << p.string();
                return status::err_permission_error;
            }
        }
    }

    for (auto & ent : entries) {
        boost::filesystem::path src{ent.source_path()};
        boost::filesystem::path dst{ent.destination_path()};
        if (src.is_absolute()) {
            // use it
        } else {
            src = from_dir / src;
        }
        // TODO: location check (for security)
        // TODO: assert dst.is_relative()
        if (!boost::filesystem::exists(src) || !boost::filesystem::is_regular_file(src)) {
            LOG_LP(ERROR) << "file not found : file = " << src.string();
            return status::err_not_found;
        }
        try {
            boost::filesystem::copy_file(src, location_ / dst);
        } catch (boost::filesystem::filesystem_error& ex) {
            LOG_LP(ERROR) << ex.what() << " file = " << src.string();
            return status::err_permission_error;
        }

    }
    return status::ok;
}

} // namespace limestone::api
