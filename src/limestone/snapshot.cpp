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
#include <limestone/api/snapshot.h>

#include <iostream> // FIXME remove this
#include <boost/filesystem.hpp>

namespace limestone::api {  // FIXME fill implementation

snapshot::snapshot(boost::filesystem::path dir) : dir_(dir / boost::filesystem::path(subdirectory_name_)) {
    boost::system::error_code error;
    const bool result_check = boost::filesystem::exists(dir_, error);
    if (!result_check || error) {
        const bool result_mkdir = boost::filesystem::create_directory(dir_, error);
        if (!result_mkdir || error) {
            std::cerr << "fail to create directory" << std::endl;  // FIXME use logger
            std::abort();
        }
    }
}

cursor& snapshot::get_cursor() {
    if (!cursor_) {
        cursor_ = std::make_unique<cursor>(open_ifstream());
    }
    return *cursor_;
}

cursor& snapshot::find([[maybe_unused]] storage_id_type storage_id, [[maybe_unused]] std::string_view entry_key) {
    std::abort();  // FIXME should implement
}

cursor& snapshot::scan([[maybe_unused]] storage_id_type storage_id, [[maybe_unused]] std::string_view entry_key, [[maybe_unused]] bool inclusive) {
    std::abort();  // FIXME should implement
}

boost::filesystem::ofstream& snapshot::open_ofstream() {
    ostrm_.open(dir_ / boost::filesystem::path(file_name_), std::ios_base::out | std::ios_base::app | std::ios_base::binary );
    return ostrm_;
}
boost::filesystem::ifstream& snapshot::open_ifstream() {
    istrm_.open(dir_ / boost::filesystem::path(file_name_), std::ios_base::in | std::ios_base::binary );
    return istrm_;
}


} // namespace limestone::api
