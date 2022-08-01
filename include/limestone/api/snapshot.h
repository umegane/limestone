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
#include <string_view>

#include <boost/filesystem/path.hpp>
#include <boost/filesystem/fstream.hpp>

#include <limestone/api/cursor.h>

namespace limestone::api {

class snapshot {
public:
    constexpr static const char* subdirectory_name_ = "data";
    constexpr static const char* file_name_ = "snapshot";

    snapshot() = default;
    explicit snapshot(boost::filesystem::path& dir);
    
    cursor& get_cursor();

    cursor& find(storage_id_type storage_id, std::string_view entry_key);

    cursor& scan(storage_id_type storage_id, std::string_view entry_key, bool inclusive);

private:
    std::unique_ptr<cursor> cursor_{};

    boost::filesystem::path dir_{};

    boost::filesystem::path file_path();

    friend class datastore;
};

} // namespace limestone::api
