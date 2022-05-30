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

#include <vector>

#include <boost/filesystem/path.hpp>

#include <limestone/api/log_channel.h>

namespace limestone::api {

class backup {
public:

    explicit backup(std::vector<std::unique_ptr<log_channel>>& log_channels);
    
    ~backup() = default;  // FIXME
    
    bool is_ready();

    bool wait_for_ready(std::size_t duration);

    std::vector<boost::filesystem::path>& files();

private:
    
    std::vector<boost::filesystem::path> files_;

};

} // namespace limestone::api
