/*
 * Copyright 2024-2024 Project Tsurugi.
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

#include <iostream>
#include <glog/logging.h>
#include <limestone/logging.h>
#include "logging_helper.h"

#include "limestone/api/datastore.h"
#include "dblog_scan.h"
#include "internal.h"
#include "log_entry.h"

using namespace limestone::api;
using namespace limestone::internal;

DEFINE_string(epoch, "", "specify valid epoch upper limit");
DEFINE_int32(thread_num, 1, "specify thread num of scanning wal file");
DEFINE_bool(cut, false, "repair by cutting for error-truncate and error-broken");
DEFINE_string(rotate, "all", "rotate files");
DEFINE_string(output_format, "human-readable", "format of output (human-readable/machine-readable)");

enum subcommand { cmd_inspect, cmd_repair };

void log_and_exit(int error) {
    VLOG(10) << "exiting with code " << error;
    exit(error);
}

namespace limestone {

void inspect(dblog_scan &ds, std::optional<epoch_id_type> epoch) {
    std::cout << "persistent-format-version: 1" << std::endl;
    epoch_id_type ld_epoch{};
    try {
        ld_epoch = ds.last_durable_epoch_in_dir();
    } catch (std::runtime_error& ex) {
        LOG(ERROR) << "reading epoch file is failed: " << ex.what();
        log_and_exit(64);
    }
    std::cout << "durable-epoch: " << ld_epoch << std::endl;
    std::atomic_size_t count_normal_entry = 0;
    std::atomic_size_t count_remove_entry = 0;
    ds.set_process_at_nondurable_epoch_snippet(dblog_scan::process_at_nondurable::report);
    ds.set_process_at_truncated_epoch_snippet(dblog_scan::process_at_truncated::report);
    ds.set_process_at_damaged_epoch_snippet(dblog_scan::process_at_damaged::report);
    ds.set_fail_fast(false);
    dblog_scan::parse_error::code max_ec{};
    epoch_id_type max_appeared_epoch = ds.scan_pwal_files(epoch.value_or(ld_epoch), [&](log_entry& e){
        if (e.type() == log_entry::entry_type::normal_entry) {
            VLOG(50) << "normal";
            count_normal_entry++;
        } else if (e.type() == log_entry::entry_type::remove_entry) {
            VLOG(50) << "remove";
            count_remove_entry++;
        } else {
            LOG(ERROR) << static_cast<int>(e.type());
        }
    }, [](log_entry::read_error& ec){
        VLOG(30) << "ERROR " << ec.value() << " : " << ec.message();
        return false;
    }, &max_ec);
    std::cout << "max-appeared-epoch: " << max_appeared_epoch << std::endl;
    std::cout << "count-durable-wal-entries: " << (count_normal_entry + count_remove_entry) << std::endl;
    VLOG(10) << "scan_pwal_files done, max_ec = " << max_ec;
    switch (max_ec) {
    case dblog_scan::parse_error::ok:
        std::cout << "status: OK" << std::endl;
        log_and_exit(0);
    case dblog_scan::parse_error::repaired:
    case dblog_scan::parse_error::broken_after_tobe_cut:
        LOG(FATAL) << "status: unreachable " << max_ec;
    case dblog_scan::parse_error::broken_after:
    case dblog_scan::parse_error::broken_after_marked:
    case dblog_scan::parse_error::nondurable_entries:
        std::cout << "status: auto-repairable" << std::endl;
        log_and_exit(1);
    case dblog_scan::parse_error::unexpected:
        std::cout << "status: unrepairable" << std::endl;
        log_and_exit(2);
    case dblog_scan::parse_error::failed:
        std::cout << "status: cannot-check" << std::endl;
        log_and_exit(64);
    }
}

void repair(dblog_scan &ds, std::optional<epoch_id_type> epoch) {
    epoch_id_type ld_epoch{};
    if (epoch.has_value()) {
        ld_epoch = epoch.value();
    } else {
        try {
            ld_epoch = ds.last_durable_epoch_in_dir();
        } catch (std::runtime_error& ex) {
            LOG(ERROR) << "reading epoch file is failed: " << ex.what();
            log_and_exit(64);
        }
        std::cout << "durable-epoch: " << ld_epoch << std::endl;
    }
    ds.set_process_at_nondurable_epoch_snippet(dblog_scan::process_at_nondurable::repair_by_mark);
    ds.set_process_at_truncated_epoch_snippet(FLAGS_cut ? dblog_scan::process_at_truncated::repair_by_cut : dblog_scan::process_at_truncated::repair_by_mark);
    ds.set_process_at_damaged_epoch_snippet(FLAGS_cut ? dblog_scan::process_at_damaged::repair_by_cut : dblog_scan::process_at_damaged::repair_by_mark);
    ds.set_fail_fast(false);

    VLOG(30) << "detach all pwal files";
    ds.detach_wal_files();
    std::atomic_size_t count_wal_entry = 0;
    dblog_scan::parse_error::code max_ec{};
    ds.scan_pwal_files(ld_epoch, [&count_wal_entry](log_entry&){ count_wal_entry++; }, [](log_entry::read_error& e) -> bool {
        LOG_LP(ERROR) << "this pwal file is broken: " << e.message();
        return false;
    }, &max_ec);
    VLOG(10) << "scan_pwal_files done, max_ec = " << max_ec;
    VLOG(10) << "count-durable-wal-entries: " << count_wal_entry;
    switch (max_ec) {
    case dblog_scan::parse_error::ok:
        std::cout << "status: OK" << std::endl;
        log_and_exit(0);
    case dblog_scan::parse_error::repaired:
    case dblog_scan::parse_error::broken_after_marked:
        std::cout << "status: repaired" << std::endl;
        log_and_exit(0);
    case dblog_scan::parse_error::broken_after_tobe_cut:
        LOG(FATAL) << "status: unreachable " << max_ec;
    case dblog_scan::parse_error::broken_after:
    case dblog_scan::parse_error::nondurable_entries:
    case dblog_scan::parse_error::unexpected:
        std::cout << "status: unrepairable" << std::endl;
        log_and_exit(1);
    case dblog_scan::parse_error::failed:
        std::cout << "status: cannot-check" << std::endl;
        log_and_exit(64);
    }
}

int main(char *dir, subcommand mode) {  // NOLINT
    std::optional<epoch_id_type> opt_epoch;
    if (FLAGS_epoch.empty()) {
        opt_epoch = std::nullopt;
    } else {
        opt_epoch = std::stoul(FLAGS_epoch);
    }
    boost::filesystem::path p(dir);
    std::cout << "dblogdir: " << p << std::endl;
    if (!boost::filesystem::exists(p)) {
        LOG(ERROR) << "dblogdir not exists";
        log_and_exit(64);
    }
    try {
        check_logdir_format(p);
    } catch (std::runtime_error&) {
        log_and_exit(64);
    }
    dblog_scan ds(p);
    ds.set_thread_num(FLAGS_thread_num);
    if (mode == cmd_inspect) inspect(ds, opt_epoch);
    if (mode == cmd_repair) repair(ds, opt_epoch);
    return 0;
}

}

int main(int argc, char *argv[]) {  // NOLINT
    FLAGS_logtostderr = true;
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    const char *arg0 = argv[0];  // NOLINT(*-pointer-arithmetic)
    google::InitGoogleLogging(arg0);
    subcommand mode{};
    auto usage = [&arg0]() {
        //std::cout << "usage: " << arg0 << " {inspect | repair} [options] <dblogdir>" << std::endl;
        std::cout << "usage: " << arg0 << " repair [options] <dblogdir>" << std::endl;
        log_and_exit(100);
    };
    if (argc < 3) {
        LOG(ERROR) << "missing parameters";
        usage();
    }
    const char *arg1 = argv[1];  // NOLINT(*-pointer-arithmetic)
    if (strcmp(arg1, "inspect") == 0) {
        LOG(WARNING) << "WARNING: subcommand 'inspect' is under development";
        mode = cmd_inspect;
    } else if (strcmp(arg1, "repair") == 0) {
        mode = cmd_repair;
    } else {
        LOG(ERROR) << "unknown subcommand: " << arg1;
        usage();
    }
    return limestone::main(argv[2], mode);  // NOLINT(*-pointer-arithmetic)
}
