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

#include <string_view>
#include <string>
#include <chrono>

#include <limestone/api/epoch_id_type.h>

namespace limestone::api {

class epoch_tag {
public:

    epoch_tag(std::string_view name, std::string_view comments, epoch_id_type epoch_id, std::chrono::system_clock::time_point timestamp)
        : name_(std::string(name)), comments_(std::string(comments)), epoch_id_(epoch_id), timestamp_(timestamp) {
    }
    epoch_tag(std::string_view name, std::string_view comments) : epoch_tag(name, comments, 0, std::chrono::system_clock::now()) {
    }
    
    std::string_view name() const { return name_; }

    std::string_view comments() const { return comments_; }

    epoch_id_type epoch_id() const { return epoch_id_; }

    std::chrono::system_clock::time_point timestamp() const { return timestamp_; }

private:

    const std::string name_{};
    const std::string comments_{};
    const epoch_id_type epoch_id_{};
    const std::chrono::system_clock::time_point timestamp_{};
    
};
    
} // namespace limestone::api
