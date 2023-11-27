/*
 * Copyright 2023-2023 Project Tsurugi.
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
#include <nlohmann/json.hpp>

#include <glog/logging.h>
#include <limestone/logging.h>
#include "logging_helper.h"

#include <limestone/api/datastore.h>

#include "internal.h"
#include "log_entry.h"

namespace limestone::internal {
using namespace limestone::api;

// setup log-dir with no data
void setup_initial_logdir(const boost::filesystem::path& logdir) {
    nlohmann::json manifest_v1 = {
        { "format_version", "1.0" },
        { "persistent_format_version", 1 }
    };
    boost::filesystem::path config = logdir / std::string(manifest_file_name);
    FILE* strm = fopen(config.c_str(), "w");  // NOLINT(*-owning-memory)
    if (!strm) {
        LOG_LP(ERROR) << "fopen for write failed, errno = " << errno;
        throw std::runtime_error("I/O error");
    }
    std::string manifest_str = manifest_v1.dump(4);
    auto ret = fwrite(manifest_str.c_str(), manifest_str.length(), 1, strm);
    if (ret != 1) {
        LOG_LP(ERROR) << "fwrite failed, errno = " << errno;
        throw std::runtime_error("I/O error");
    }
    if (fflush(strm) != 0) {
        LOG_LP(ERROR) << "fflush failed, errno = " << errno;
        throw std::runtime_error("I/O error");
    }
    if (fsync(fileno(strm)) != 0) {
        LOG_LP(ERROR) << "fsync failed, errno = " << errno;
        throw std::runtime_error("I/O error");
    }
    if (fclose(strm) != 0) {  // NOLINT(*-owning-memory)
        LOG_LP(ERROR) << "fclose failed, errno = " << errno;
        throw std::runtime_error("I/O error");
    }
}

void check_logdir_format(const boost::filesystem::path& logdir) {
    boost::filesystem::path manifest_path = logdir / std::string(manifest_file_name);
    std::ifstream istrm(manifest_path.string());
    if (!istrm) {
        LOG_LP(ERROR) << "no config file in logdir, maybe v0";
        throw std::runtime_error("logdir version mismatch");
    }
    nlohmann::json manifest;
    istrm >> manifest;
    auto version = manifest["persistent_format_version"];
    if (!version.is_number_integer() || version != 1) {
        LOG_LP(ERROR) << "logdir version mismatch: version = " << version;
        throw std::runtime_error("logdir version mismatch");
    }
}

} // namespace limestone::internal
