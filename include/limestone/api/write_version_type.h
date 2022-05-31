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

#include <cstdint>

namespace limestone::api {

using epoch_t = std::int64_t;  // from shirakami/src/concurrency_control/silo/include/epoch.h

class write_version_type {
  public:
    write_version_type();
    write_version_type(epoch_t epoch_number, std::uint64_t minor_write_version);

private:
    /**
     * @brief For PITR and major write version
     * 
     */
    epoch_t epoch_number_;
        
    /**
     * @brief The order in the same epoch.
     * @apis bit layout:
     * 1 bits: 0 - short tx, 1 - long tx.
     * 63 bits: the order between short tx or long tx id.
     */
    std::uint64_t minor_write_version_;

    friend class log_channel;
};

} // namespace limestone::api
