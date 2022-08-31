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
    /**
     * @brief directory name of a snapshot
     */
    constexpr static const std::string_view subdirectory_name_ = "data";

    /**
     * @brief file name of a snapshot lodated on the directory named subdirectory_name_
     */
    constexpr static const std::string_view file_name_ = "snapshot";

    std::unique_ptr<cursor> get_cursor() const noexcept;

    std::unique_ptr<cursor> find(storage_id_type storage_id, std::string_view entry_key) const noexcept;

    std::unique_ptr<cursor> scan(storage_id_type storage_id, std::string_view entry_key, bool inclusive) const noexcept;

private:
    boost::filesystem::path dir_{};

    boost::filesystem::path file_path() const noexcept;

    snapshot() noexcept = delete;

    explicit snapshot(const boost::filesystem::path& location) noexcept;

    friend class datastore;
};

} // namespace limestone::api
