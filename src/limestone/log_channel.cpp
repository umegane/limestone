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
#include <iostream>  // FIXME

#include <limestone/api/log_channel.h>

namespace limestone::api {

log_channel::log_channel(boost::filesystem::path location) : location_(location) {
    std::cout << __func__ << ":" << location.string() << std::endl;
}

void log_channel::begin_session() {
    std::stringstream ss;
    ss << std::setw(4) << std::setfill('0') << std::hex << num_;
    file_ = ss.str();

    strm_.open(location_ / file_, std::ios_base::out | std::ios_base::app | std::ios_base::binary );

    std::cout << __func__ << ":" << std::endl;
}

void log_channel::end_session() {
    strm_.close();
}

void log_channel::abort_session([[maybe_unused]] error_code_type error_code, [[maybe_unused]] std::string message) {
}

void log_channel::add_entry([[maybe_unused]] storage_id_type storage_id, [[maybe_unused]] std::string_view key, [[maybe_unused]] std::string_view value, [[maybe_unused]] write_version_type write_version) {
    std::cout << __func__ << ":" << storage_id << ":" << key.length() << ":" << value.length() << std::endl;
}

} // namespace limestone::api
