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
#include <boost/filesystem/operations.hpp>
#include <boost/foreach.hpp>

#include <glog/logging.h>
#include <limestone/logging.h>

#include <limestone/api/datastore.h>
#include <limestone/status.h>

namespace limestone::api {

status datastore::restore(std::string_view from, bool keep_backup) const noexcept {
    DVLOG_LP(log_debug) << "restore begin, from directory = " << from << " , keep_backup = " << (keep_backup ? "true" : "false");

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

    auto from_dir = boost::filesystem::path(std::string(from));
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

} // namespace limestone::api
