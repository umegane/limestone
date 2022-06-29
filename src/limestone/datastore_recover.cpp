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
#include <boost/filesystem/operations.hpp>
#include <boost/foreach.hpp>

#include <limestone/api/datastore.h>
#include "log_entry.h"

#include "glog/logging.h"

namespace limestone::api {

void datastore::recover(std::string_view from, [[maybe_unused]] bool overwrite) {
    auto from_dir = boost::filesystem::path(std::string(from));

    epoch_id_type ld_epoch = last_durable_epoch(from_dir);

    boost::filesystem::ofstream ostrm{};
    ostrm.open(snapshot_->file_path(), std::ios_base::out | std::ios_base::app | std::ios_base::binary);
    BOOST_FOREACH(const boost::filesystem::path& p, std::make_pair(boost::filesystem::directory_iterator(from_dir), boost::filesystem::directory_iterator())) {
        if (p.filename().string().substr(0, log_channel::prefix.length()).compare(log_channel::prefix) == 0) {
            log_entry e;
            epoch_id_type current_epoch{UINT64_MAX};

            boost::filesystem::ifstream istrm;
            istrm.open(p, std::ios_base::in | std::ios_base::binary);
            while(e.read(istrm)) {
                switch(e.type()) {
                case log_entry::entry_type::marker_begin:
                    current_epoch = e.epoch_id();
                    break;
                case log_entry::entry_type::normal_entry:
                    if (current_epoch <= ld_epoch) {
                        e.write(ostrm);
                    }
                    break;
                default:
                    break;
                }
            }
            istrm.close();
        }
    }
    ostrm.close();
}

epoch_id_type datastore::last_durable_epoch(boost::filesystem::path from_dir) {
    boost::filesystem::ifstream istrm;
    log_entry e;
    istrm.open(from_dir / boost::filesystem::path(std::string(epoch_file_name)), std::ios_base::in | std::ios_base::binary);
    while (e.read(istrm));
    istrm.close();
    return e.epoch_id();
}

} // namespace limestone::api
