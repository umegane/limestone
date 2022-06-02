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
#include <vector>

#include <boost/filesystem.hpp>

#include <limestone/api/storage_id_type.h>
#include <limestone/api/large_object_view.h>

namespace limestone::api {

class log_entry;

class cursor {
public:
    cursor(boost::filesystem::ifstream& istrm);
    ~cursor();

    bool next();

    storage_id_type storage();

    void key(std::string& buf);

    void value(std::string& buf);

    std::vector<large_object_view>& large_objects();

private:
    boost::filesystem::ifstream& istrm_;
    std::unique_ptr<log_entry> log_entry_;
    std::vector<large_object_view> large_objects_{};
};

} // namespace limestone::api
