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

#include <string>
#include <string_view>

#include <limestone/error_code.h>
#include <limestone/detail/storage_id_type.h>
#include <limestone/detail/write_version_type.h>
#include <limestone/detail/large_object_input.h>

namespace limestone::api {

using namespace limestone::detail;

class log_channel {
public:

    void begin_session();

    void end_session();

    void abort_session(error_code_type error_code, std::string message);

    void add_entry(storage_id_type storage_id, std::string_view key, std::string_view value, write_version_type write_version, std::list<large_object_input> large_objects);
    void add_entry(storage_id_type storage_id, std::string_view key, std::string_view value, write_version_type write_version);


};

} // namespace limestone::api
