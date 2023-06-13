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

#include <byteswap.h>
#include <boost/filesystem/operations.hpp>
#include <boost/foreach.hpp>
#include <cstdlib>
#include <cstring>

#include <glog/logging.h>
#include <limestone/logging.h>
#include "logging_helper.h"

#include <limestone/api/datastore.h>
#include "log_entry.h"
#include "leveldb_wrapper.h"

namespace limestone::api {

constexpr std::size_t write_version_size = sizeof(epoch_id_type) + sizeof(std::uint64_t);
static_assert(write_version_size == 16);

// return max epoch in file.
static std::optional<epoch_id_type> last_durable_epoch(const boost::filesystem::path& file) noexcept {
    std::optional<epoch_id_type> rv;

    boost::filesystem::ifstream istrm;
    log_entry e;
    istrm.open(file, std::ios_base::in | std::ios_base::binary);
    while (e.read(istrm)) {
        if (!rv.has_value() || e.epoch_id() > rv) {
            rv = e.epoch_id();
        }
    }
    istrm.close();
    return rv;
}

epoch_id_type datastore::last_durable_epoch_in_dir() noexcept {
    auto& from_dir = location_;
    // read main epoch file first
    std::optional<epoch_id_type> ld_epoch = last_durable_epoch(from_dir / std::string(log_channel::prefix));
    if (ld_epoch.has_value()) {
        return *ld_epoch;
    }

    // main epoch file is empty,
    // read all rotated-epoch files
    for (const boost::filesystem::path& p : boost::filesystem::directory_iterator(from_dir)) {
        if (p.filename().string().rfind(log_channel::prefix, 0) == 0) {  // starts_with(log_channel::prefix)
            // this is epoch file (main one or rotated)
            std::optional<epoch_id_type> epoch = last_durable_epoch(p);
            if (!epoch.has_value()) {
                continue;  // file is empty
            }
            // ld_epoch = max(ld_epoch, epoch)
            if (!ld_epoch.has_value() || *ld_epoch < *epoch) {
                ld_epoch = epoch;
            }
        }
    }
    return ld_epoch.value_or(0);  // 0 = minimum epoch
}

[[maybe_unused]]
static void store_bswap64_value(void *dest, const void *src) {
    auto* p64_dest = reinterpret_cast<std::uint64_t*>(dest);  // NOLINT
    auto* p64_src = reinterpret_cast<const std::uint64_t*>(src);  // NOLINT
    *p64_dest = __bswap_64(*p64_src);
}

[[maybe_unused]]
static int comp_twisted_key(const std::string_view& a, const std::string_view& b) {
    std::size_t a_strlen = a.size() - write_version_size;
    std::size_t b_strlen = b.size() - write_version_size;
    std::string_view a_str(a.data() + write_version_size, a_strlen);
    std::string_view b_str(b.data() + write_version_size, b_strlen);
    if (int c = a_str.compare(b_str); c != 0) return c;
    return std::memcmp(b.data(), a.data(), write_version_size);
}

void datastore::create_snapshot() noexcept {
    auto& from_dir = location_;
#if defined SORT_METHOD_PUT_ONLY
    auto lvldb = std::make_unique<leveldb_wrapper>(from_dir, comp_twisted_key);
#else
    auto lvldb = std::make_unique<leveldb_wrapper>(from_dir);
#endif

    epoch_id_type ld_epoch = last_durable_epoch_in_dir();
    epoch_id_switched_.store(ld_epoch + 1);

    [[maybe_unused]]
    auto insert_entry_or_update_to_max = [&lvldb](log_entry& e){
        bool need_write = true;

        // skip older entry than already inserted
        std::string value;
        if (lvldb->get(e.key_sid(), &value)) {
            write_version_type write_version;
            e.write_version(write_version);
            if (write_version < write_version_type(value.substr(1))) {
                need_write = false;
            }
        }

        if (need_write) {
            std::string db_value;
            db_value.append(1, static_cast<char>(e.type()));
            db_value.append(e.value_etc());
            lvldb->put(e.key_sid(), db_value);
        }
    };
    [[maybe_unused]]
    auto insert_twisted_entry = [&lvldb](log_entry& e){
        // key_sid: storage_id[8] key[*], value_etc: epoch[8]LE minor_version[8]LE value[*], type: type[1]
        // db_key: epoch[8]BE minor_version[8]BE storage_id[8] key[*], db_value: type[1] value[*]
        std::string db_key(write_version_size + e.key_sid().size(), '\0');
        store_bswap64_value(&db_key[0], &e.value_etc()[0]);
        store_bswap64_value(&db_key[8], &e.value_etc()[8]);
        std::memcpy(&db_key[write_version_size], e.key_sid().data(), e.key_sid().size());
        std::string db_value(1, static_cast<char>(e.type()));
        db_value.append(e.value_etc().substr(write_version_size));
        lvldb->put(db_key, db_value);
    };
#if defined SORT_METHOD_PUT_ONLY
    auto add_entry = insert_twisted_entry;
#else
    auto add_entry = insert_entry_or_update_to_max;
#endif
    BOOST_FOREACH(const boost::filesystem::path& p, std::make_pair(boost::filesystem::directory_iterator(from_dir), boost::filesystem::directory_iterator())) {
        if (p.filename().string().substr(0, log_channel::prefix.length()) == log_channel::prefix) {
            VLOG_LP(log_info) << "processing pwal file: " << p.filename().string();
            log_entry e;
            epoch_id_type current_epoch{UINT64_MAX};

            boost::filesystem::ifstream istrm;
            istrm.open(p, std::ios_base::in | std::ios_base::binary);
            while(e.read(istrm)) {
                switch(e.type()) {
                case log_entry::entry_type::marker_begin:
                {
                    current_epoch = e.epoch_id();
                    break;
                }
                case log_entry::entry_type::normal_entry:
                case log_entry::entry_type::remove_entry:
                {
                    if (current_epoch <= ld_epoch) {
                        add_entry(e);
                    }
                    break;
                }
                default:
                    break;
                }
            }
            istrm.close();
        }
    }

    boost::filesystem::path sub_dir = location_ / boost::filesystem::path(std::string(snapshot::subdirectory_name_));
    boost::system::error_code error;
    const bool result_check = boost::filesystem::exists(sub_dir, error);
    if (!result_check || error) {
        const bool result_mkdir = boost::filesystem::create_directory(sub_dir, error);
        if (!result_mkdir || error) {
            LOG_LP(ERROR) << "fail to create directory";
            std::abort();
        }
    }

    boost::filesystem::ofstream ostrm{};
    boost::filesystem::path snapshot_file = sub_dir / boost::filesystem::path(std::string(snapshot::file_name_));
    VLOG_LP(log_info) << "generating snapshot file: " << snapshot_file;
    ostrm.open(snapshot_file, std::ios_base::out | std::ios_base::trunc | std::ios_base::binary);
    if( ostrm.fail() ){
        LOG_LP(ERROR) << "cannot create snapshot file (" << snapshot_file << ")";
        std::abort();
    }
    static_assert(sizeof(log_entry::entry_type) == 1);
#if defined SORT_METHOD_PUT_ONLY
    lvldb->each([&ostrm, last_key = std::string{}](std::string_view db_key, std::string_view db_value) mutable {
        // using the first entry in GROUP BY (original-)key
        // NB: max versions comes first (by the custom-comparator)
        std::string_view key(db_key.data() + write_version_size, db_key.size() - write_version_size);
        if (key == last_key) {  // same (original-)key with prev
            return; // skip
        }
        last_key.assign(key);

        auto entry_type = static_cast<log_entry::entry_type>(db_value[0]);
        switch (entry_type) {
        case log_entry::entry_type::normal_entry: {
            std::string value(write_version_size + db_value.size() - 1, '\0');
            store_bswap64_value(&value[0], &db_key[0]);
            store_bswap64_value(&value[8], &db_key[8]);
            std::memcpy(&value[write_version_size], &db_value[1], db_value.size() - 1);
            log_entry::write(ostrm, key, value);
            break;
        }
        case log_entry::entry_type::remove_entry:
            break;  // skip
        default:
            LOG(ERROR) << "never reach " << static_cast<int>(entry_type);
            std::abort();
        }
    });
#else
    lvldb->each([&ostrm](std::string_view db_key, std::string_view db_value) {
        auto entry_type = static_cast<log_entry::entry_type>(db_value[0]);
        db_value.remove_prefix(1);
        switch (entry_type) {
        case log_entry::entry_type::normal_entry:
            log_entry::write(ostrm, db_key, db_value);
            break;
        case log_entry::entry_type::remove_entry:
            break;  // skip
        default:
            LOG(ERROR) << "never reach " << static_cast<int>(entry_type);
            std::abort();
        }
    });
#endif
    ostrm.close();
}

} // namespace limestone::api
