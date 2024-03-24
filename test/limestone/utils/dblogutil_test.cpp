
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

extern void create_file(const boost::filesystem::path& path, std::string_view content);
extern std::string read_entire_file(const boost::filesystem::path& path);
extern std::string data_manifest(int persistent_format_version = 1);

extern const std::string_view epoch_0x100_str;

extern const std::string_view data_normal;
extern const std::string_view data_normal2;
extern const std::string_view data_nondurable;
extern const std::string_view data_repaired_nondurable;
extern const std::string_view data_zerofill;
extern const std::string_view data_truncated_normal_entry;
extern const std::string_view data_truncated_epoch_header;
extern const std::string_view data_truncated_invalidated_normal_entry;
extern const std::string_view data_truncated_invalidated_epoch_header;
extern const std::string_view data_allzero;

#define UTIL_COMMAND "../src/dblogutil"

int invoke(const std::string& command, std::string& out) {
    FILE* fp;
    fp = popen(command.c_str(), "r");
    char buf[4096];
    std::ostringstream ss;
    std::size_t rc;
    while ((rc = fread(buf, 1, 4095, fp)) > 0) {
        ss.write(buf, rc);
    }
    out.assign(ss.str());
    LOG(INFO) << "\n" << out;
    return pclose(fp);
}

class dblogutil_test : public ::testing::Test {
public:
static constexpr const char* location = "/tmp/dblogutil_test";

    void SetUp() {
        boost::filesystem::remove_all(location);
        if (!boost::filesystem::create_directory(location)) {
            std::cerr << "cannot make directory" << std::endl;
        }
    }

    void TearDown() {
        boost::filesystem::remove_all(location);
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

    std::pair<int, std::string> inspect(std::string pwal_fname, std::string_view data) {
        boost::filesystem::path dir{location};
        create_file(dir / "epoch", epoch_0x100_str);
        create_file(dir / std::string(manifest_file_name), data_manifest());
        auto pwal = dir / pwal_fname;
        create_file(pwal, data);
        std::string command;
        command = UTIL_COMMAND " inspect " + dir.string() + " 2>&1";
        std::string out;
        int rc = invoke(command, out);
        return make_pair(rc, out);
    }

    std::pair<int, std::string> repairm(std::string pwal_fname, std::string_view data) {
        boost::filesystem::path dir{location};
        create_file(dir / "epoch", epoch_0x100_str);
        create_file(dir / std::string(manifest_file_name), data_manifest());
        auto pwal = dir / pwal_fname;
        create_file(pwal, data);
        std::string command;
        command = UTIL_COMMAND " repair --cut=false " + dir.string() + " 2>&1";
        std::string out;
        int rc = invoke(command, out);
        auto files = list_dir();
        assert(files.size() == 1);
        assert(dblog_scan::is_detached_wal(files.at(0)));
        return make_pair(rc, out);
    }

    std::tuple<int, std::string, int, std::string> repairm_twice(std::string pwal_fname, std::string_view data) {
        boost::filesystem::path dir{location};
        create_file(dir / "epoch", epoch_0x100_str);
        create_file(dir / std::string(manifest_file_name), data_manifest());
        auto pwal = dir / pwal_fname;
        create_file(pwal, data);
        std::string command;
        command = UTIL_COMMAND " repair --cut=false " + dir.string() + " 2>&1";
        std::string out;
        int rc = invoke(command, out);
        auto files = list_dir();
        assert(files.size() == 1);
        assert(dblog_scan::is_detached_wal(files.at(0)));
        auto data1 = read_entire_file(list_dir()[0]);

        std::string out2;
        int rc2 = invoke(command, out2);
        auto files2 = list_dir();
        assert(files2.size() == 1);
        assert(dblog_scan::is_detached_wal(files2.at(0)));
        auto data2 = read_entire_file(list_dir()[0]);

        assert(data1.size() == data.size());
        assert(data1 == data2);
        return std::make_tuple(rc, out, rc2, out2);
    }

    std::pair<int, std::string> repairc(std::string pwal_fname, std::string_view data) {
        boost::filesystem::path dir{location};
        create_file(dir / "epoch", epoch_0x100_str);
        create_file(dir / std::string(manifest_file_name), data_manifest());
        auto pwal = dir / pwal_fname;
        create_file(pwal, data);
        std::string command;
        command = UTIL_COMMAND " repair --cut=true " + dir.string() + " 2>&1";
        std::string out;
        int rc = invoke(command, out);
        auto files = list_dir();
        assert(files.size() == 1);
        assert(dblog_scan::is_detached_wal(files.at(0)));
        return make_pair(rc, out);
    }

    std::tuple<int, std::string, int, std::string> repairc_twice(std::string pwal_fname, std::string_view data) {
        boost::filesystem::path dir{location};
        create_file(dir / "epoch", epoch_0x100_str);
        create_file(dir / std::string(manifest_file_name), data_manifest());
        auto pwal = dir / pwal_fname;
        create_file(pwal, data);
        std::string command;
        command = UTIL_COMMAND " repair --cut=true " + dir.string() + " 2>&1";
        std::string out;
        int rc = invoke(command, out);
        auto files = list_dir();
        assert(files.size() == 1);
        assert(dblog_scan::is_detached_wal(files.at(0)));
        auto data1 = read_entire_file(list_dir()[0]);

        std::string out2;
        int rc2 = invoke(command, out2);
        auto files2 = list_dir();
        assert(files2.size() == 1);
        assert(dblog_scan::is_detached_wal(files2.at(0)));
        auto data2 = read_entire_file(list_dir()[0]);

        assert(data1.size() <= data.size());
        assert(data1 == data2);
        return std::make_tuple(rc, out, rc2, out2);
    }

    void expect_no_change(std::string_view& from) {
        auto to = read_entire_file(list_dir()[0]);
        EXPECT_EQ(from, to);  // no change
    }

    void expect_mark_at(std::size_t offset, std::string_view& from) {
        auto to = read_entire_file(list_dir()[0]);
        ASSERT_EQ(from.at(offset), '\x02');
        EXPECT_EQ(to.at(offset), '\x06');  // marked
        EXPECT_EQ(from.substr(0, offset), to.substr(0, offset));  // no change before mark
        EXPECT_EQ(from.substr(offset + 1), to.substr(offset + 1));
    }

    void expect_cut_at(std::size_t offset, std::string_view& from) {
        auto to = read_entire_file(list_dir()[0]);
        ASSERT_TRUE(from.at(offset) == '\x02' || from.at(offset) == '\x06');
        EXPECT_EQ(to.size(), offset);  // cut
        EXPECT_EQ(from.substr(0, offset), to.substr(0, offset));  // no change before cut
    }

};

TEST_F(dblogutil_test, inspect_normal) {
    auto [rc, out] = inspect("pwal_0000", data_normal);
    EXPECT_EQ(rc, 0);
    EXPECT_NE(out.find("\n" "status: OK"), out.npos);
}

TEST_F(dblogutil_test, inspect_normal2) {
    auto [rc, out] = inspect("pwal_0000", data_normal2);
    EXPECT_EQ(rc, 0 << 8);
    EXPECT_NE(out.find("\n" "status: OK"), out.npos);
    EXPECT_NE(out.find("\n" "count-durable-wal-entries: 3"), out.npos);
}

TEST_F(dblogutil_test, inspect_nondurable) {
    auto [rc, out] = inspect("pwal_0000", data_nondurable);
    EXPECT_EQ(rc, 1 << 8);
    EXPECT_NE(out.find("\n" "status: auto-repairable"), out.npos);
}

TEST_F(dblogutil_test, inspect_repaired_nondurable) {
    auto [rc, out] = inspect("pwal_0000", data_repaired_nondurable);
    EXPECT_EQ(rc, 0 << 8);
    EXPECT_NE(out.find("\n" "status: OK"), out.npos);
    EXPECT_NE(out.find("\n" "count-durable-wal-entries: 2"), out.npos);
}

TEST_F(dblogutil_test, inspect_zerofill) {
    auto [rc, out] = inspect("pwal_0000", data_zerofill);
    EXPECT_EQ(rc, 1 << 8);
    EXPECT_NE(out.find("\n" "status: auto-repairable"), out.npos);
}

TEST_F(dblogutil_test, inspect_truncated_normal_entry) {
    auto [rc, out] = inspect("pwal_0000", data_truncated_normal_entry);
    EXPECT_EQ(rc, 1 << 8);
    EXPECT_NE(out.find("\n" "status: auto-repairable"), out.npos);
}

TEST_F(dblogutil_test, inspect_truncated_epoch_header) {
    auto [rc, out] = inspect("pwal_0000", data_truncated_epoch_header);
    EXPECT_EQ(rc, 1 << 8);
    EXPECT_NE(out.find("\n" "status: auto-repairable"), out.npos);
}

TEST_F(dblogutil_test, inspect_truncated_invalidated_normal_entry) {
    auto [rc, out] = inspect("pwal_0000", data_truncated_invalidated_normal_entry);
    EXPECT_EQ(rc, 1 << 8);
    EXPECT_NE(out.find("\n" "status: auto-repairable"), out.npos);
}

TEST_F(dblogutil_test, inspect_truncated_invalidated_epoch_header) {
    auto [rc, out] = inspect("pwal_0000", data_truncated_invalidated_epoch_header);
    EXPECT_EQ(rc, 1 << 8);
    EXPECT_NE(out.find("\n" "status: auto-repairable"), out.npos);
}

TEST_F(dblogutil_test, inspect_allzero) {
    auto [rc, out] = inspect("pwal_0000", data_allzero);
    EXPECT_EQ(rc, 2 << 8);
    EXPECT_NE(out.find("\n" "status: unrepairable"), out.npos);
}

TEST_F(dblogutil_test, repairm_normal) {
    auto orig_data = data_normal;
    auto [rc, out] = repairm("pwal_0000", orig_data);
    EXPECT_EQ(rc, 0);
    EXPECT_NE(out.find("\n" "status: OK"), out.npos);
    expect_no_change(orig_data);
}

TEST_F(dblogutil_test, repairm_nondurable) {
    auto orig_data = data_nondurable;
    auto [rc, out, rc2, out2] = repairm_twice("pwal_0000", orig_data);
    EXPECT_EQ(rc, 0);
    EXPECT_NE(out.find("\n" "status: repaired"), out.npos);
    EXPECT_EQ(rc2, 0);
    EXPECT_NE(out2.find("\n" "status: OK"), out2.npos);
    expect_mark_at(9, orig_data);
}

TEST_F(dblogutil_test, repairm_nondurable_detached) {
    auto orig_data = data_nondurable;
    auto [rc, out] = repairm("pwal_0000.rotated", orig_data);
    EXPECT_EQ(rc, 0);
    EXPECT_NE(out.find("\n" "status: repaired"), out.npos);
    expect_mark_at(9, orig_data);
}

TEST_F(dblogutil_test, repairm_repaired_nondurable) {
    auto orig_data = data_repaired_nondurable;
    auto [rc, out] = repairm("pwal_0000", orig_data);
    EXPECT_EQ(rc, 0);
    EXPECT_NE(out.find("\n" "status: OK"), out.npos);
    expect_no_change(orig_data);
}

TEST_F(dblogutil_test, repairm_zerofill) {
    auto orig_data = data_zerofill;
    auto [rc, out, rc2, out2] = repairm_twice("pwal_0000", orig_data);
    EXPECT_EQ(rc, 0);
    EXPECT_NE(out.find("\n" "status: repaired"), out.npos);
    EXPECT_EQ(rc2, 0);
    EXPECT_NE(out2.find("\n" "status: OK"), out2.npos);
    expect_mark_at(9, orig_data);
}

TEST_F(dblogutil_test, repairm_zerofill_detached) {
    auto orig_data = data_zerofill;
    auto [rc, out] = repairm("pwal_0000.rotated", orig_data);
    EXPECT_EQ(rc, 0);
    EXPECT_NE(out.find("\n" "status: repaired"), out.npos);
    expect_mark_at(9, orig_data);
}

TEST_F(dblogutil_test, repairm_truncated_normal_entry) {
    auto orig_data = data_truncated_normal_entry;
    auto [rc, out, rc2, out2] = repairm_twice("pwal_0000", orig_data);
    EXPECT_EQ(rc, 0);
    EXPECT_NE(out.find("\n" "status: repaired"), out.npos);
    EXPECT_EQ(rc2, 0);
    EXPECT_NE(out2.find("\n" "status: OK"), out2.npos);
    expect_mark_at(9, orig_data);
}

TEST_F(dblogutil_test, repairm_truncated_normal_entry_detached) {
    auto orig_data = data_truncated_normal_entry;
    auto [rc, out] = repairm("pwal_0000.rotated", orig_data);
    EXPECT_EQ(rc, 0);
    EXPECT_NE(out.find("\n" "status: repaired"), out.npos);
    expect_mark_at(9, orig_data);
}

TEST_F(dblogutil_test, repairm_truncated_epoch_header) {
    auto orig_data = data_truncated_epoch_header;
    auto [rc, out, rc2, out2] = repairm_twice("pwal_0000", orig_data);
    EXPECT_EQ(rc, 0);
    EXPECT_NE(out.find("\n" "status: repaired"), out.npos);
    EXPECT_EQ(rc2, 0);
    EXPECT_NE(out2.find("\n" "status: OK"), out2.npos);
    expect_mark_at(50, orig_data);
}

TEST_F(dblogutil_test, repairm_truncated_epoch_header_detached) {
    auto orig_data = data_truncated_epoch_header;
    auto [rc, out] = repairm("pwal_0000.rotated", orig_data);
    EXPECT_EQ(rc, 0);
    EXPECT_NE(out.find("\n" "status: repaired"), out.npos);
    expect_mark_at(50, orig_data);
}

TEST_F(dblogutil_test, repairm_truncated_invalidated_normal_entry) {
    auto orig_data = data_truncated_invalidated_normal_entry;
    auto [rc, out] = repairm("pwal_0000", orig_data);
    EXPECT_EQ(rc, 0);
    EXPECT_NE(out.find("\n" "status: OK"), out.npos);
    expect_no_change(orig_data);
}

TEST_F(dblogutil_test, repairm_truncated_invalidated_normal_entry_detached) {
    auto orig_data = data_truncated_invalidated_normal_entry;
    auto [rc, out] = repairm("pwal_0000.rotated", orig_data);
    EXPECT_EQ(rc, 0);
    EXPECT_NE(out.find("\n" "status: OK"), out.npos);
    expect_no_change(orig_data);
}

TEST_F(dblogutil_test, repairm_truncated_invalidated_epoch_header) {
    auto orig_data = data_truncated_invalidated_epoch_header;
    auto [rc, out] = repairm("pwal_0000", orig_data);
    EXPECT_EQ(rc, 0);
    EXPECT_NE(out.find("\n" "status: OK"), out.npos);
    expect_no_change(orig_data);
}

TEST_F(dblogutil_test, repairm_truncated_invalidated_epoch_header_detached) {
    auto orig_data = data_truncated_invalidated_epoch_header;
    auto [rc, out] = repairm("pwal_0000.rotated", orig_data);
    EXPECT_EQ(rc, 0);
    EXPECT_NE(out.find("\n" "status: OK"), out.npos);
    expect_no_change(orig_data);
}

TEST_F(dblogutil_test, repairm_allzero) {
    auto orig_data = data_allzero;
    auto [rc, out] = repairm("pwal_0000", orig_data);
    EXPECT_EQ(rc, 1 << 8);
    EXPECT_NE(out.find("\n" "status: unrepairable"), out.npos);
    expect_no_change(orig_data);
}

TEST_F(dblogutil_test, repairc_zerofill) {
    auto orig_data = data_zerofill;
    auto [rc, out, rc2, out2] = repairc_twice("pwal_0000", orig_data);
    EXPECT_EQ(rc, 0);
    EXPECT_NE(out.find("\n" "status: repaired"), out.npos);
    EXPECT_EQ(rc2, 0);
    EXPECT_NE(out2.find("\n" "status: OK"), out2.npos);
    expect_cut_at(9, orig_data);
}

TEST_F(dblogutil_test, repairc_truncated_normal_entry) {
    auto orig_data = data_truncated_normal_entry;
    auto [rc, out, rc2, out2] = repairc_twice("pwal_0000", orig_data);
    EXPECT_EQ(rc, 0);
    EXPECT_NE(out.find("\n" "status: repaired"), out.npos);
    EXPECT_EQ(rc2, 0);
    EXPECT_NE(out2.find("\n" "status: OK"), out2.npos);
    expect_cut_at(9, orig_data);
}

TEST_F(dblogutil_test, repairc_truncated_epoch_header) {
    auto orig_data = data_truncated_epoch_header;
    auto [rc, out, rc2, out2] = repairc_twice("pwal_0000", orig_data);
    EXPECT_EQ(rc, 0);
    EXPECT_NE(out.find("\n" "status: repaired"), out.npos);
    EXPECT_EQ(rc2, 0);
    EXPECT_NE(out2.find("\n" "status: OK"), out2.npos);
    expect_cut_at(50, orig_data);
}

TEST_F(dblogutil_test, repairc_truncated_invalidated_normal_entry) {
    auto orig_data = data_truncated_invalidated_normal_entry;
    auto [rc, out, rc2, out2] = repairc_twice("pwal_0000", orig_data);
    EXPECT_EQ(rc, 0);
    EXPECT_NE(out.find("\n" "status: repaired"), out.npos);
    EXPECT_EQ(rc2, 0);
    EXPECT_NE(out2.find("\n" "status: OK"), out2.npos);
    expect_cut_at(9, orig_data);
}

TEST_F(dblogutil_test, repairc_truncated_invalidated_epoch_header) {
    auto orig_data = data_truncated_invalidated_epoch_header;
    auto [rc, out, rc2, out2] = repairc_twice("pwal_0000", orig_data);
    EXPECT_EQ(rc, 0);
    EXPECT_NE(out.find("\n" "status: repaired"), out.npos);
    EXPECT_EQ(rc2, 0);
    EXPECT_NE(out2.find("\n" "status: OK"), out2.npos);
    expect_cut_at(50, orig_data);
}

TEST_F(dblogutil_test, repairc_allzero) {
    auto orig_data = data_allzero;
    auto [rc, out] = repairc("pwal_0000", orig_data);
    EXPECT_EQ(rc, 1 << 8);
    EXPECT_NE(out.find("\n" "status: unrepairable"), out.npos);
    expect_no_change(orig_data);
}

}  // namespace limestone::testing
