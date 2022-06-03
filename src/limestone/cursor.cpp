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
#include <limestone/api/cursor.h>
#include "log_entry.h"

namespace limestone::api {

cursor::cursor(boost::filesystem::path file) : log_entry_(std::make_unique<log_entry>()) {
    istrm_.open(file, std::ios_base::in | std::ios_base::binary );
}
cursor::~cursor() {
    istrm_.close();
}

bool cursor::next() {
    if (!istrm_.good()) {
        return false;
    }
    if (istrm_.eof()) {
        return false;
    }
    log_entry_->read(istrm_);
    return true;
}

storage_id_type cursor::storage() {
    return log_entry_->storage();
}

void cursor::key(std::string& buf) {
    log_entry_->key(buf);
}

void cursor::value(std::string& buf) {
    log_entry_->value(buf);
}

std::vector<large_object_view>& cursor::large_objects() {
    return large_objects_;
}

} // namespace limestone::api
