
#include <algorithm>
#include <sstream>
#include <limestone/logging.h>

#include <boost/filesystem.hpp>

#include "dblog_scan.h"
#include "internal.h"
#include "log_entry.h"

#include "test_root.h"

namespace limestone::testing {

using namespace std::literals;
using namespace limestone::api;
using namespace limestone::internal;

extern void create_file(boost::filesystem::path path, std::string_view content);

class dblog_scan_test : public ::testing::Test {
public:
static constexpr const char* location = "/tmp/dblog_scan_test";

    void SetUp() {
        boost::filesystem::remove_all(location);
        if (!boost::filesystem::create_directory(location)) {
            std::cerr << "cannot make directory" << std::endl;
        }
    }

    void TearDown() {
        boost::filesystem::remove_all(location);
    }

    void set_inspect_mode(dblog_scan& ds) {
        ds.set_process_at_nondurable_epoch_snippet(dblog_scan::process_at_nondurable::report);
        ds.set_process_at_truncated_epoch_snippet(dblog_scan::process_at_truncated::report);
        ds.set_process_at_damaged_epoch_snippet(dblog_scan::process_at_damaged::report);
        ds.set_fail_fast(false);
    }
    void set_repair_by_mark_mode(dblog_scan& ds) {
        ds.set_process_at_nondurable_epoch_snippet(dblog_scan::process_at_nondurable::repair_by_mark);
        ds.set_process_at_truncated_epoch_snippet(dblog_scan::process_at_truncated::repair_by_mark);
        ds.set_process_at_damaged_epoch_snippet(dblog_scan::process_at_damaged::repair_by_mark);
        ds.set_fail_fast(false);
    }
    void set_repair_by_cut_mode(dblog_scan& ds) {
        ds.set_process_at_nondurable_epoch_snippet(dblog_scan::process_at_nondurable::repair_by_mark);
        ds.set_process_at_truncated_epoch_snippet(dblog_scan::process_at_truncated::repair_by_cut);
        ds.set_process_at_damaged_epoch_snippet(dblog_scan::process_at_damaged::repair_by_cut);
        ds.set_fail_fast(false);
    }
    bool starts_with(std::string a, std::string b) { return a.substr(0, b.length()) == b; }

    std::vector<boost::filesystem::path> list_dir() {
        std::vector<boost::filesystem::path> ret;
        for (const boost::filesystem::path& p : boost::filesystem::directory_iterator(boost::filesystem::path(location))) {
            if (dblog_scan::is_wal(p)) {
                ret.emplace_back(p);
            }
        }
        return ret;
    }

};

// combination test
// {inspect-mode, repair(mark)-mode, repair(cut)-mode}
//   x
// {normal, nondurable, zerofill, truncated_normal_entry, truncated_epoch_header, truncated_invalidated_normal_entry, truncated_invalidated_epoch_header}

static std::string_view data_normal =
    "\x02\xff\x00\x00\x00\x00\x00\x00\x00"  // marker_begin 0xff
    // XXX: epoch footer...
    "\x02\x00\x01\x00\x00\x00\x00\x00\x00"  // marker_begin 0x100
    // XXX: epoch footer...
    ""sv;

static std::string_view data_nondurable =
    "\x02\xff\x00\x00\x00\x00\x00\x00\x00"  // marker_begin 0xff
    // XXX: epoch footer...
    "\x02\x01\x01\x00\x00\x00\x00\x00\x00"  // marker_begin 0x101 (nondurable)
    // XXX: epoch footer...
    ""sv;

static std::string_view data_zerofill =
    "\x02\xff\x00\x00\x00\x00\x00\x00\x00"  // marker_begin 0xff
    // XXX: epoch footer...
    "\x02\x01\x01\x00\x00\x00\x00\x00\x00"  // marker_begin 0x101
    // XXX: epoch footer...
    "\x00\x00\x00\x00\x00\x00\x00\x00\x00"  // UNKNOWN_TYPE_entry
    ""sv;

static std::string_view data_truncated_normal_entry =
    "\x02\xff\x00\x00\x00\x00\x00\x00\x00"  // marker_begin 0xff
    // XXX: epoch footer...
    "\x02\x01\x01\x00\x00\x00\x00\x00\x00"  // marker_begin 0x101
    "\x01\x04\x00\x00\x00\x04\x00\x00\x00"  // SHORT_normal_entry
    ""sv;

static std::string_view data_truncated_epoch_header =
    "\x02\xff\x00\x00\x00\x00\x00\x00\x00"  // marker_begin 0xff
    "\x01\x04\x00\x00\x00\x04\x00\x00\x00" "storage1" "1234" "vermajor" "verminor" "1234"  // normal_entry
    // XXX: epoch footer...
    // offset 50
    "\x02\x01\x01\x00\x00\x00\x00\x00"  // SHORT_marker_begin
    ""sv;

static std::string_view data_truncated_invalidated_normal_entry =
    "\x02\xff\x00\x00\x00\x00\x00\x00\x00"  // marker_begin 0xff
    // XXX: epoch footer...
    "\x06\x01\x01\x00\x00\x00\x00\x00\x00"  // marker_invalidated_begin 0x101
    "\x01\x04\x00\x00\x00\x04\x00\x00\x00"  // SHORT_normal_entry
    ""sv;

static std::string_view data_truncated_invalidated_epoch_header =
    "\x02\xff\x00\x00\x00\x00\x00\x00\x00"  // marker_begin 0xff
    "\x01\x04\x00\x00\x00\x04\x00\x00\x00" "storage1" "1234" "vermajor" "verminor" "1234"  // normal_entry
    // XXX: epoch footer...
    // offset 50
    "\x06\x01\x01\x00\x00\x00\x00\x00"  // SHORT_marker_inv_begin
    ""sv;

// unit-test scan_one_pwal_file
// inspect the normal file; returns ok
TEST_F(dblog_scan_test, scan_one_pwal_file_inspect_normal) {
    auto p = boost::filesystem::path(location) / "pwal_0000";
    create_file(p, data_normal);

    dblog_scan ds{boost::filesystem::path(location)};
    ds.set_thread_num(1);
    set_inspect_mode(ds);
    dblog_scan::parse_error pe;
    std::vector<log_entry::read_error> errors;

    EXPECT_EQ(ds.scan_one_pwal_file(p, 0x100, [](const log_entry& e){
        VLOG(30) << static_cast<int>(e.type());
    }, [&errors](log_entry::read_error& re){
        VLOG(30) << re.message();
        errors.emplace_back(re);
        return false;
    }, pe), 0x100);
    EXPECT_EQ(errors.size(), 0);
    EXPECT_EQ(pe.value(), dblog_scan::parse_error::ok);
}

// unit-test scan_one_pwal_file
// inspect the file including nondurable epoch snippet
TEST_F(dblog_scan_test, scan_one_pwal_file_inspect_nondurable) {
    auto p = boost::filesystem::path(location) / "pwal_0000";
    create_file(p, data_nondurable);

    dblog_scan ds{boost::filesystem::path(location)};
    ds.set_thread_num(1);
    set_inspect_mode(ds);
    dblog_scan::parse_error pe;
    std::vector<log_entry::read_error> errors;

    EXPECT_EQ(ds.scan_one_pwal_file(p, 0x100, [](const log_entry& e){
        VLOG(30) << static_cast<int>(e.type());
    }, [&errors](log_entry::read_error& re){
        VLOG(30) << re.message();
        errors.emplace_back(re);
        return false;
    }, pe), 0x101);
    EXPECT_EQ(errors.size(), 1);  // nondurable
    EXPECT_EQ(pe.value(), dblog_scan::parse_error::nondurable_entries);
}

// unit-test scan_one_pwal_file
// inspect the file filled zero
TEST_F(dblog_scan_test, scan_one_pwal_file_inspect_zerofill) {
    auto p = boost::filesystem::path(location) / "pwal_0000";
    create_file(p, data_zerofill);

    dblog_scan ds{boost::filesystem::path(location)};
    ds.set_thread_num(1);
    set_inspect_mode(ds);
    dblog_scan::parse_error pe;
    std::vector<log_entry::read_error> errors;

    EXPECT_EQ(ds.scan_one_pwal_file(p, 0x100, [](const log_entry& e){
        VLOG(30) << static_cast<int>(e.type());
    }, [&errors](log_entry::read_error& re){
        VLOG(30) << re.message();
        errors.emplace_back(re);
        return false;
    }, pe), 0x101);
    EXPECT_EQ(errors.size(), 1);
    EXPECT_EQ(pe.value(), dblog_scan::parse_error::broken_after);
    EXPECT_EQ(pe.fpos(), 9);
}

// unit-test scan_one_pwal_file
// inspect the file truncated on log_entries
TEST_F(dblog_scan_test, scan_one_pwal_file_inspect_truncated_normal_entry) {
    auto p = boost::filesystem::path(location) / "pwal_0000";
    create_file(p, data_truncated_normal_entry);

    dblog_scan ds{boost::filesystem::path(location)};
    ds.set_thread_num(1);
    set_inspect_mode(ds);
    dblog_scan::parse_error pe;
    std::vector<log_entry::read_error> errors;

    EXPECT_EQ(ds.scan_one_pwal_file(p, 0x100, [](const log_entry& e){
        VLOG(30) << static_cast<int>(e.type());
    }, [&errors](log_entry::read_error& re){
        VLOG(30) << re.message();
        errors.emplace_back(re);
        return false;
    }, pe), 0x101);
    //EXPECT_EQ(errors.size(), 1);  // nondurable
    EXPECT_EQ(pe.value(), dblog_scan::parse_error::broken_after);
    EXPECT_EQ(pe.fpos(), 9);
}

// unit-test scan_one_pwal_file
// inspect the file truncated on epoch_snippet_header
TEST_F(dblog_scan_test, scan_one_pwal_file_inspect_truncated_epoch_header) {
    auto p = boost::filesystem::path(location) / "pwal_0000";
    create_file(p, data_truncated_epoch_header);

    dblog_scan ds{boost::filesystem::path(location)};
    ds.set_thread_num(1);
    set_inspect_mode(ds);
    dblog_scan::parse_error pe;
    std::vector<log_entry::read_error> errors;

    EXPECT_EQ(ds.scan_one_pwal_file(p, 0x100, [](const log_entry& e){
        VLOG(30) << static_cast<int>(e.type());
    }, [&errors](log_entry::read_error& re){
        VLOG(30) << re.message();
        errors.emplace_back(re);
        return false;
    }, pe), 0xff);
    EXPECT_EQ(errors.size(), 1);  // truncated
    EXPECT_EQ(pe.value(), dblog_scan::parse_error::broken_after);
    EXPECT_EQ(pe.fpos(), 50);  // after correct epoch snippet
}

// unit-test scan_one_pwal_file
// inspect the file truncated on log_entries in invalidated epoch snippet
TEST_F(dblog_scan_test, scan_one_pwal_file_inspect_truncated_invalidated_normal_entry) {
    auto p = boost::filesystem::path(location) / "pwal_0000";
    create_file(p, data_truncated_invalidated_normal_entry);

    dblog_scan ds{boost::filesystem::path(location)};
    ds.set_thread_num(1);
    set_inspect_mode(ds);
    dblog_scan::parse_error pe;
    std::vector<log_entry::read_error> errors;

    EXPECT_EQ(ds.scan_one_pwal_file(p, 0x100, [](const log_entry& e){
        VLOG(30) << static_cast<int>(e.type());
    }, [&errors](log_entry::read_error& re){
        VLOG(30) << re.message();
        errors.emplace_back(re);
        return false;
    }, pe), 0x101);
    // EXPECT_EQ(errors.size(), 1);  0 or 1 // ??
    EXPECT_EQ(pe.value(), dblog_scan::parse_error::broken_after);
    EXPECT_EQ(pe.fpos(), 9);
}

// unit-test scan_one_pwal_file
// inspect the file truncated on invalidated epoch_snippet_header
TEST_F(dblog_scan_test, scan_one_pwal_file_inspect_truncated_invalidated_epoch_header) {
    auto p = boost::filesystem::path(location) / "pwal_0000";
    create_file(p, data_truncated_invalidated_epoch_header);

    dblog_scan ds{boost::filesystem::path(location)};
    ds.set_thread_num(1);
    set_inspect_mode(ds);
    dblog_scan::parse_error pe;
    std::vector<log_entry::read_error> errors;

    EXPECT_EQ(ds.scan_one_pwal_file(p, 0x100, [](const log_entry& e){
        VLOG(30) << static_cast<int>(e.type());
    }, [&errors](log_entry::read_error& re){
        VLOG(30) << re.message();
        errors.emplace_back(re);
        return false;
    }, pe), 0xff);
    //EXPECT_EQ(errors.size(), 1);  // ?
    EXPECT_EQ(pe.value(), dblog_scan::parse_error::broken_after);
    EXPECT_EQ(pe.fpos(), 50);  // after correct epoch snippet
}

// unit-test scan_one_pwal_file
// repair(mark) the normal file; returns ok
TEST_F(dblog_scan_test, scan_one_pwal_file_repairm_normal) {
    auto p = boost::filesystem::path(location) / "pwal_0000";
    create_file(p, data_normal);

    dblog_scan ds{boost::filesystem::path(location)};
    ds.set_thread_num(1);
    set_repair_by_mark_mode(ds);
    dblog_scan::parse_error pe;
    std::vector<log_entry::read_error> errors;

    EXPECT_EQ(ds.scan_one_pwal_file(p, 0x100, [](const log_entry& e){
        VLOG(30) << static_cast<int>(e.type());
    }, [&errors](log_entry::read_error& re){
        VLOG(30) << re.message();
        errors.emplace_back(re);
        return false;
    }, pe), 0x100);
    EXPECT_EQ(pe.value(), dblog_scan::parse_error::ok);
}

// unit-test scan_one_pwal_file
// repair(mark) the file including nondurable epoch snippet
TEST_F(dblog_scan_test, scan_one_pwal_file_repairm_nondurable) {
    auto p = boost::filesystem::path(location) / "pwal_0000";
    create_file(p, data_nondurable);

    dblog_scan ds{boost::filesystem::path(location)};
    ds.set_thread_num(1);
    set_repair_by_mark_mode(ds);
    dblog_scan::parse_error pe;
    std::vector<log_entry::read_error> errors;

    EXPECT_EQ(ds.scan_one_pwal_file(p, 0x100, [](const log_entry& e){
        VLOG(30) << static_cast<int>(e.type());
    }, [&errors](log_entry::read_error& re){
        VLOG(30) << re.message();
        errors.emplace_back(re);
        return false;
    }, pe), 0x101);
    EXPECT_EQ(pe.value(), dblog_scan::parse_error::repaired);
}

// unit-test scan_one_pwal_file
// repair(mark) the file filled zero
TEST_F(dblog_scan_test, scan_one_pwal_file_repairm_zerofill) {
    auto p = boost::filesystem::path(location) / "pwal_0000";
    create_file(p, data_zerofill);

    dblog_scan ds{boost::filesystem::path(location)};
    ds.set_thread_num(1);
    set_repair_by_mark_mode(ds);
    dblog_scan::parse_error pe;
    std::vector<log_entry::read_error> errors;

    EXPECT_EQ(ds.scan_one_pwal_file(p, 0x100, [](const log_entry& e){
        VLOG(30) << static_cast<int>(e.type());
    }, [&errors](log_entry::read_error& re){
        VLOG(30) << re.message();
        errors.emplace_back(re);
        return false;
    }, pe), 0x101);
    EXPECT_EQ(pe.value(), dblog_scan::parse_error::broken_after_marked);
    EXPECT_EQ(pe.fpos(), 9);
}

// unit-test scan_one_pwal_file
// repair(mark) the file truncated on log_entries
TEST_F(dblog_scan_test, scan_one_pwal_file_repairm_truncated_normal_entry) {
    auto p = boost::filesystem::path(location) / "pwal_0000";
    create_file(p, data_truncated_normal_entry);

    dblog_scan ds{boost::filesystem::path(location)};
    ds.set_thread_num(1);
    set_repair_by_mark_mode(ds);
    dblog_scan::parse_error pe;
    std::vector<log_entry::read_error> errors;

    EXPECT_EQ(ds.scan_one_pwal_file(p, 0x100, [](const log_entry& e){
        VLOG(30) << static_cast<int>(e.type());
    }, [&errors](log_entry::read_error& re){
        VLOG(30) << re.message();
        errors.emplace_back(re);
        return false;
    }, pe), 0x101);
    EXPECT_EQ(pe.value(), dblog_scan::parse_error::broken_after_marked);
    EXPECT_EQ(pe.fpos(), 9);
}

// unit-test scan_one_pwal_file
// repair(mark) the file truncated on epoch_snippet_header
TEST_F(dblog_scan_test, scan_one_pwal_file_repairm_truncated_epoch_header) {
    auto p = boost::filesystem::path(location) / "pwal_0000";
    create_file(p, data_truncated_epoch_header);

    dblog_scan ds{boost::filesystem::path(location)};
    ds.set_thread_num(1);
    set_repair_by_mark_mode(ds);
    dblog_scan::parse_error pe;
    std::vector<log_entry::read_error> errors;

    EXPECT_EQ(ds.scan_one_pwal_file(p, 0x100, [](const log_entry& e){
        VLOG(30) << static_cast<int>(e.type());
    }, [&errors](log_entry::read_error& re){
        VLOG(30) << re.message();
        errors.emplace_back(re);
        return false;
    }, pe), 0xff);
    EXPECT_EQ(pe.value(), dblog_scan::parse_error::broken_after_marked);
    EXPECT_EQ(pe.fpos(), 50);  // after correct epoch snippet
}

// unit-test scan_one_pwal_file
// repair(mark) the file truncated on log_entries in invalidated epoch snippet
TEST_F(dblog_scan_test, scan_one_pwal_file_repairm_truncated_invalidated_normal_entry) {
    auto p = boost::filesystem::path(location) / "pwal_0000";
    create_file(p, data_truncated_invalidated_normal_entry);

    dblog_scan ds{boost::filesystem::path(location)};
    ds.set_thread_num(1);
    set_repair_by_mark_mode(ds);
    dblog_scan::parse_error pe;
    std::vector<log_entry::read_error> errors;

    EXPECT_EQ(ds.scan_one_pwal_file(p, 0x100, [](const log_entry& e){
        VLOG(30) << static_cast<int>(e.type());
    }, [&errors](log_entry::read_error& re){
        VLOG(30) << re.message();
        errors.emplace_back(re);
        return false;
    }, pe), 0x101);
    EXPECT_EQ(pe.value(), dblog_scan::parse_error::broken_after_marked);
    EXPECT_EQ(pe.fpos(), 9);
}

// unit-test scan_one_pwal_file
// repair(mark) the file truncated on invalidated epoch_snippet_header
TEST_F(dblog_scan_test, scan_one_pwal_file_repairm_truncated_invalidated_epoch_header) {
    auto p = boost::filesystem::path(location) / "pwal_0000";
    create_file(p, data_truncated_epoch_header);

    dblog_scan ds{boost::filesystem::path(location)};
    ds.set_thread_num(1);
    set_repair_by_mark_mode(ds);
    dblog_scan::parse_error pe;
    std::vector<log_entry::read_error> errors;

    EXPECT_EQ(ds.scan_one_pwal_file(p, 0x100, [](const log_entry& e){
        VLOG(30) << static_cast<int>(e.type());
    }, [&errors](log_entry::read_error& re){
        VLOG(30) << re.message();
        errors.emplace_back(re);
        return false;
    }, pe), 0xff);
    EXPECT_EQ(pe.value(), dblog_scan::parse_error::broken_after_marked);
    EXPECT_EQ(pe.fpos(), 50);  // after correct epoch snippet
}

// unit-test scan_one_pwal_file
// repair(cut) the normal file; returns ok
// same as avobe

// unit-test scan_one_pwal_file
// repair(cut) is not supported

// unit-test scan_one_pwal_file
// repair(mark) the file filled zero
TEST_F(dblog_scan_test, scan_one_pwal_file_repairc_zerofill) {
    auto p = boost::filesystem::path(location) / "pwal_0000";
    create_file(p, data_zerofill);

    dblog_scan ds{boost::filesystem::path(location)};
    ds.set_thread_num(1);
    set_repair_by_cut_mode(ds);
    dblog_scan::parse_error pe;
    std::vector<log_entry::read_error> errors;

    EXPECT_EQ(ds.scan_one_pwal_file(p, 0x100, [](const log_entry& e){
        VLOG(30) << static_cast<int>(e.type());
    }, [&errors](log_entry::read_error& re){
        VLOG(30) << re.message();
        errors.emplace_back(re);
        return false;
    }, pe), 0x101);
    EXPECT_EQ(pe.value(), dblog_scan::parse_error::repaired);
    EXPECT_EQ(pe.fpos(), 9);
    EXPECT_EQ(boost::filesystem::file_size(p), 9);
}

// unit-test scan_one_pwal_file
// repair(cut) the file truncated on log_entries
TEST_F(dblog_scan_test, scan_one_pwal_file_repairc_truncated_normal_entry) {
    auto p = boost::filesystem::path(location) / "pwal_0000";
    create_file(p, data_truncated_normal_entry);

    dblog_scan ds{boost::filesystem::path(location)};
    ds.set_thread_num(1);
    set_repair_by_cut_mode(ds);
    dblog_scan::parse_error pe;
    std::vector<log_entry::read_error> errors;

    EXPECT_EQ(ds.scan_one_pwal_file(p, 0x100, [](const log_entry& e){
        VLOG(30) << static_cast<int>(e.type());
    }, [&errors](log_entry::read_error& re){
        VLOG(30) << re.message();
        errors.emplace_back(re);
        return false;
    }, pe), 0x101);
    EXPECT_EQ(pe.value(), dblog_scan::parse_error::repaired);
    EXPECT_EQ(pe.fpos(), 9);
    EXPECT_EQ(boost::filesystem::file_size(p), 9);
}

// unit-test scan_one_pwal_file
// repair(cut) the file truncated on epoch_snippet_header
TEST_F(dblog_scan_test, scan_one_pwal_file_repairc_truncated_epoch_header) {
    auto p = boost::filesystem::path(location) / "pwal_0000";
    create_file(p, data_truncated_epoch_header);

    dblog_scan ds{boost::filesystem::path(location)};
    ds.set_thread_num(1);
    set_repair_by_cut_mode(ds);
    dblog_scan::parse_error pe;
    std::vector<log_entry::read_error> errors;

    EXPECT_EQ(ds.scan_one_pwal_file(p, 0x100, [](const log_entry& e){
        VLOG(30) << static_cast<int>(e.type());
    }, [&errors](log_entry::read_error& re){
        VLOG(30) << re.message();
        errors.emplace_back(re);
        return false;
    }, pe), 0xff);
    EXPECT_EQ(pe.value(), dblog_scan::parse_error::repaired);
    EXPECT_EQ(pe.fpos(), 50);
    EXPECT_EQ(boost::filesystem::file_size(p), 50);
}

// unit-test scan_one_pwal_file
// repair(cut) the file truncated on log_entries in invalidated epoch snippet
TEST_F(dblog_scan_test, scan_one_pwal_file_repairc_truncated_invalidated_normal_entry) {
    auto p = boost::filesystem::path(location) / "pwal_0000";
    create_file(p, data_truncated_invalidated_normal_entry);

    dblog_scan ds{boost::filesystem::path(location)};
    ds.set_thread_num(1);
    set_repair_by_cut_mode(ds);
    dblog_scan::parse_error pe;
    std::vector<log_entry::read_error> errors;

    EXPECT_EQ(ds.scan_one_pwal_file(p, 0x100, [](const log_entry& e){
        VLOG(30) << static_cast<int>(e.type());
    }, [&errors](log_entry::read_error& re){
        VLOG(30) << re.message();
        errors.emplace_back(re);
        return false;
    }, pe), 0x101);
    EXPECT_EQ(pe.value(), dblog_scan::parse_error::repaired);
    EXPECT_EQ(pe.fpos(), 9);
}

// unit-test scan_one_pwal_file
// repair(cut) the file truncated on invalidated epoch_snippet_header
TEST_F(dblog_scan_test, scan_one_pwal_file_repairc_truncated_invalidated_epoch_header) {
    auto p = boost::filesystem::path(location) / "pwal_0000";
    create_file(p, data_truncated_epoch_header);

    dblog_scan ds{boost::filesystem::path(location)};
    ds.set_thread_num(1);
    set_repair_by_cut_mode(ds);
    dblog_scan::parse_error pe;
    std::vector<log_entry::read_error> errors;

    EXPECT_EQ(ds.scan_one_pwal_file(p, 0x100, [](const log_entry& e){
        VLOG(30) << static_cast<int>(e.type());
    }, [&errors](log_entry::read_error& re){
        VLOG(30) << re.message();
        errors.emplace_back(re);
        return false;
    }, pe), 0xff);
    EXPECT_EQ(pe.value(), dblog_scan::parse_error::repaired);
    EXPECT_EQ(pe.fpos(), 50);  // after correct epoch snippet
}

// unit-test detach_wal_files; normal non-detached pwal files are renamed (rotated)
TEST_F(dblog_scan_test, detach_wal_files_renamne_pwal_0000) {
    auto p0_attached = boost::filesystem::path(location) / "pwal_0000";
    create_file(p0_attached,
                "\x02\xff\x00\x00\x00\x00\x00\x00\x00"  // marger_begin 0xff
                // XXX: epoch footer...
                ""sv);
    {
        auto wal_files = list_dir();
        ASSERT_EQ(wal_files.size(), 1);
        ASSERT_EQ(wal_files.at(0), p0_attached);
    }
    dblog_scan ds{boost::filesystem::path(location)};
    ds.detach_wal_files();
    {  // rotated
        auto wal_files = list_dir();
        EXPECT_EQ(wal_files.size(), 1);
        EXPECT_NE(wal_files.at(0), p0_attached);
        EXPECT_GT(wal_files.at(0).filename().string().length(), 10);
    }
}

// unit-test detach_wal_files; empty pwal files are skipped
TEST_F(dblog_scan_test, detach_wal_files_skip_rename_empty_pwal) {
    auto p0_attached_empty = boost::filesystem::path(location) / "pwal_0000";
    create_file(p0_attached_empty, ""sv);
    {
        auto wal_files = list_dir();
        ASSERT_EQ(wal_files.size(), 1);
        ASSERT_EQ(wal_files.at(0), p0_attached_empty);
    }
    dblog_scan ds{boost::filesystem::path(location)};
    ds.detach_wal_files();
    {  // no change
        auto wal_files = list_dir();
        EXPECT_EQ(wal_files.size(), 1);
        EXPECT_EQ(wal_files.at(0), p0_attached_empty);
    }
}

// unit-test detach_wal_files; detached (rotated) pwal files are skipped
TEST_F(dblog_scan_test, detach_wal_files_skip_rename_pwal_0000_somewhat) {
    auto p0_detached = boost::filesystem::path(location) / "pwal_0000.somewhat";
    create_file(p0_detached,
                "\x02\xff\x00\x00\x00\x00\x00\x00\x00"  // marger_begin 0xff
                // XXX: epoch footer...
                ""sv);
    {
        auto wal_files = list_dir();
        ASSERT_EQ(wal_files.size(), 1);
        ASSERT_EQ(wal_files.at(0), p0_detached);
    }
    dblog_scan ds{boost::filesystem::path(location)};
    ds.detach_wal_files();
    {  // no change
        auto wal_files = list_dir();
        EXPECT_EQ(wal_files.size(), 1);
        EXPECT_EQ(wal_files.at(0), p0_detached);
    }
}

}  // namespace limestone::testing
