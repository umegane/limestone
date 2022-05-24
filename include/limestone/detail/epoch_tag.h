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
#include <chrono>

#include <limestone/detail/epoch_id_type.h>

namespace limestone::detail {

class epoch_tag {
public:
    
    std::string_view name();

    std::string_view comments();

    epoch_id_type epoch_id();

    std::chrono::system_clock::time_point timestamp();
};
    
} // namespace limestone::detail
