
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
extern const std::string_view data_nondurable;
extern const std::string_view data_zerofill;
extern const std::string_view data_truncated_normal_entry;
extern const std::string_view data_truncated_epoch_header;
extern const std::string_view data_truncated_invalidated_normal_entry;
extern const std::string_view data_truncated_invalidated_epoch_header;


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

};

std::pair<int, std::string> inspect(const boost::filesystem::path& dir, std::string pwal_fname, std::string_view data) {
    create_file(dir / "epoch", epoch_0x100_str);
    create_file(dir / std::string(manifest_file_name), data_manifest());
    auto pwal = dir / pwal_fname;
    create_file(pwal, data);
    std::string command;
    command = "../src/dblogutil inspect " + dir.string() + " 2>&1";
    std::string out;
    int rc = invoke(command, out);
    return make_pair(rc, out);
}

TEST_F(dblogutil_test, inspect_normal) {
    auto [rc, out] = inspect(location, "pwal_0000", data_normal);
    EXPECT_EQ(rc, 0);
    EXPECT_NE(out.find("\n" "status: OK"), out.npos);
}

TEST_F(dblogutil_test, inspect_nondurable) {
    auto [rc, out] = inspect(location, "pwal_0000", data_nondurable);
    EXPECT_EQ(rc, 1 << 8);
    EXPECT_NE(out.find("\n" "status: auto-repairable"), out.npos);
}

TEST_F(dblogutil_test, inspect_zerofill) {
    auto [rc, out] = inspect(location, "pwal_0000", data_zerofill);
    EXPECT_EQ(rc, 1 << 8);
    EXPECT_NE(out.find("\n" "status: auto-repairable"), out.npos);
}

TEST_F(dblogutil_test, inspect_truncated_normal_entry) {
    auto [rc, out] = inspect(location, "pwal_0000", data_truncated_normal_entry);
    EXPECT_EQ(rc, 1 << 8);
    EXPECT_NE(out.find("\n" "status: auto-repairable"), out.npos);
}

TEST_F(dblogutil_test, inspect_truncated_epoch_header) {
    auto [rc, out] = inspect(location, "pwal_0000", data_truncated_epoch_header);
    EXPECT_EQ(rc, 1 << 8);
    EXPECT_NE(out.find("\n" "status: auto-repairable"), out.npos);
}

TEST_F(dblogutil_test, inspect_truncated_invalidated_normal_entry) {
    auto [rc, out] = inspect(location, "pwal_0000", data_truncated_invalidated_normal_entry);
    EXPECT_EQ(rc, 1 << 8);
    EXPECT_NE(out.find("\n" "status: auto-repairable"), out.npos);
}

TEST_F(dblogutil_test, inspect_truncated_invalidated_epoch_header) {
    auto [rc, out] = inspect(location, "pwal_0000", data_truncated_invalidated_epoch_header);
    EXPECT_EQ(rc, 1 << 8);
    EXPECT_NE(out.find("\n" "status: auto-repairable"), out.npos);
}

}  // namespace limestone::testing
