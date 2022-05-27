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

#include <limestone/api/tag_repository.h>

namespace limestone::api {

std::vector<epoch_tag>& tag_repository::list() {
    list_.clear();
    for(auto&& e : map_) {
        list_.emplace_back(e.second);
    }
    return list_;
}

void tag_repository::register_tag(std::string name, std::string comments) {
    map_.emplace(name, epoch_tag(name, comments));
}

std::optional<epoch_tag> tag_repository::find(std::string_view name) {
    if (auto itr = map_.find(std::string(name)); itr != map_.cend()) {
        return itr->second;
    }
    return std::nullopt;
}

void tag_repository::unregister_tag(std::string_view name) {
    if (auto itr = map_.find(std::string(name)); itr != map_.cend()) {
        map_.erase(itr);
    }
}

} // namespace limestone::api
