
#include <boost/filesystem.hpp>

#include <limestone/logging.h>

#include "internal.h"
#include "log_entry.h"

#include "test_root.h"

using namespace std::literals;

namespace limestone::testing {

class log_dir_test : public ::testing::Test {
public:
static constexpr const char* location = "/tmp/log_dir_test";
const boost::filesystem::path manifest_path = boost::filesystem::path(location) / std::string(limestone::internal::manifest_file_name);
static constexpr const std::string_view epoch_0_str = "\x04\x00\x00\x00\x00\x00\x00\x00\x00"sv;
static_assert(epoch_0_str.length() == 9);

    void SetUp() {
        boost::filesystem::remove_all(location);
        if (!boost::filesystem::create_directory(location)) {
            std::cerr << "cannot make directory" << std::endl;
        }
    }

    void gen_datastore() {
        std::vector<boost::filesystem::path> data_locations{};
        data_locations.emplace_back(location);
        boost::filesystem::path metadata_location{location};
        limestone::api::configuration conf(data_locations, metadata_location);

        datastore_ = std::make_unique<limestone::api::datastore_test>(conf);
    }

    void TearDown() {
        datastore_ = nullptr;
        boost::filesystem::remove_all(location);
    }

    static bool starts_with(std::string a, std::string b) { return a.substr(0, b.length()) == b; }
    static bool is_pwal(const boost::filesystem::path& p) { return starts_with(p.filename().string(), "pwal"); }
    static void ignore_entry(limestone::api::log_entry&) {}

protected:
    std::unique_ptr<limestone::api::datastore_test> datastore_{};
};

extern void create_file(boost::filesystem::path path, std::string_view content);

TEST_F(log_dir_test, newly_created_directory_contains_manifest_file) {
    gen_datastore();
    limestone::internal::check_logdir_format(location);

    EXPECT_TRUE(boost::filesystem::exists(manifest_path));
}

TEST_F(log_dir_test, reject_directory_without_manifest_file) {
    create_file(boost::filesystem::path(location) / "epoch", epoch_0_str);

    gen_datastore();
    EXPECT_THROW({ limestone::internal::check_logdir_format(location); }, std::exception);
}

TEST_F(log_dir_test, reject_directory_with_broken_manifest_file) {
    create_file(boost::filesystem::path(location) / "epoch", epoch_0_str);
    create_file(manifest_path, "broken");

    gen_datastore();
    EXPECT_THROW({ limestone::internal::check_logdir_format(location); }, std::exception);
}

TEST_F(log_dir_test, reject_directory_only_broken_manifest_file) {
    create_file(manifest_path, "broken");

    gen_datastore();
    EXPECT_THROW({ limestone::internal::check_logdir_format(location); }, std::exception);
}

TEST_F(log_dir_test, reject_directory_only_broken_manifest_file2) {
    create_file(manifest_path, "{ \"answer\": 42 }");

    gen_datastore();
    EXPECT_THROW({ limestone::internal::check_logdir_format(location); }, std::exception);
}

TEST_F(log_dir_test, accept_directory_with_correct_manifest_file) {
    create_file(boost::filesystem::path(location) / "epoch", epoch_0_str);
    create_file(manifest_path, "{ \"format_version\": \"1.0\", \"persistent_format_version\": 1 }");

    gen_datastore();
    limestone::internal::check_logdir_format(location);  // success
}

TEST_F(log_dir_test, accept_directory_only_correct_manifest_file) {
    create_file(manifest_path, "{ \"format_version\": \"1.0\", \"persistent_format_version\": 1 }");

    gen_datastore();
    limestone::internal::check_logdir_format(location);  // success
}

TEST_F(log_dir_test, reject_directory_of_different_version) {
    create_file(manifest_path, "{ \"format_version\": \"1.0\", \"persistent_format_version\": 222 }");

    gen_datastore();
    EXPECT_THROW({ limestone::internal::check_logdir_format(location); }, std::exception);
}

TEST_F(log_dir_test, rotate_old_ok_v1_dir) {
    // setup backups
    boost::filesystem::path bk_path = boost::filesystem::path(location) / "bk";
    if (!boost::filesystem::create_directory(bk_path)) {
        LOG(FATAL) << "cannot make directory";
    }
    create_file(bk_path / "epoch", epoch_0_str);
    create_file(bk_path / std::string(limestone::internal::manifest_file_name),
                "{ \"format_version\": \"1.0\", \"persistent_format_version\": 1 }");

    gen_datastore();

    EXPECT_EQ(datastore_->restore(bk_path.string(), true), limestone::status::ok);
}

TEST_F(log_dir_test, rotate_old_rejects_unsupported_data) {
    // setup backups
    boost::filesystem::path bk_path = boost::filesystem::path(location) / "bk";
    if (!boost::filesystem::create_directory(bk_path)) {
        LOG(FATAL) << "cannot make directory";
    }
    create_file(bk_path / "epoch", epoch_0_str);
    create_file(bk_path / std::string(limestone::internal::manifest_file_name),
                "{ \"format_version\": \"1.0\", \"persistent_format_version\": 2 }");

    gen_datastore();

    EXPECT_EQ(datastore_->restore(bk_path.string(), true), limestone::status::err_broken_data);
}

TEST_F(log_dir_test, rotate_old_rejects_v0_logdir_missing_manifest) {
    // setup backups
    boost::filesystem::path bk_path = boost::filesystem::path(location) / "bk";
    if (!boost::filesystem::create_directory(bk_path)) {
        LOG(FATAL) << "cannot make directory";
    }
    create_file(bk_path / "epoch", epoch_0_str);

    gen_datastore();

    EXPECT_EQ(datastore_->restore(bk_path.string(), true), limestone::status::err_broken_data);
}

TEST_F(log_dir_test, rotate_old_rejects_corrupted_dir) {
    // setup backups
    boost::filesystem::path bk_path = boost::filesystem::path(location) / "bk";
    if (!boost::filesystem::create_directory(bk_path)) {
        LOG(FATAL) << "cannot make directory";
    }
    create_file(bk_path / "epoch", epoch_0_str);
    create_file(bk_path / std::string(limestone::internal::manifest_file_name),
                "{ \"answer\": 42 }");

    gen_datastore();

    EXPECT_EQ(datastore_->restore(bk_path.string(), true), limestone::status::err_broken_data);
}

TEST_F(log_dir_test, rotate_prusik_ok_v1_dir) {
    // setup backups
    boost::filesystem::path bk_path = boost::filesystem::path(location) / "bk";
    if (!boost::filesystem::create_directory(bk_path)) {
        LOG(FATAL) << "cannot make directory";
    }
    create_file(bk_path / "epoch", epoch_0_str);
    create_file(bk_path / std::string(limestone::internal::manifest_file_name),
                "{ \"format_version\": \"1.0\", \"persistent_format_version\": 1 }");
    // setup entries
    std::vector<limestone::api::file_set_entry> entries;
    entries.emplace_back("epoch", "epoch", false);
    entries.emplace_back(std::string(limestone::internal::manifest_file_name), std::string(limestone::internal::manifest_file_name), false);

    gen_datastore();

    EXPECT_EQ(datastore_->restore(bk_path.string(), entries), limestone::status::ok);
}

TEST_F(log_dir_test, rotate_prusik_rejects_unsupported_data) {
    // setup backups
    boost::filesystem::path bk_path = boost::filesystem::path(location) / "bk";
    if (!boost::filesystem::create_directory(bk_path)) {
        LOG(FATAL) << "cannot make directory";
    }
    create_file(bk_path / "epoch", epoch_0_str);
    create_file(bk_path / std::string(limestone::internal::manifest_file_name),
                "{ \"format_version\": \"1.0\", \"persistent_format_version\": 2 }");
    // setup entries
    std::vector<limestone::api::file_set_entry> entries;
    entries.emplace_back("epoch", "epoch", false);
    entries.emplace_back(std::string(limestone::internal::manifest_file_name), std::string(limestone::internal::manifest_file_name), false);

    gen_datastore();

    EXPECT_EQ(datastore_->restore(bk_path.string(), entries), limestone::status::err_broken_data);
}

TEST_F(log_dir_test, rotate_prusik_rejects_v0_logdir_missing_manifest) {
    // setup backups
    boost::filesystem::path bk_path = boost::filesystem::path(location) / "bk";
    if (!boost::filesystem::create_directory(bk_path)) {
        LOG(FATAL) << "cannot make directory";
    }
    create_file(bk_path / "epoch", epoch_0_str);
    // setup entries
    std::vector<limestone::api::file_set_entry> entries;
    entries.emplace_back("epoch", "epoch", false);

    gen_datastore();

    EXPECT_EQ(datastore_->restore(bk_path.string(), entries), limestone::status::err_broken_data);
}

TEST_F(log_dir_test, rotate_prusik_rejects_corrupted_dir) {
    // setup backups
    boost::filesystem::path bk_path = boost::filesystem::path(location) / "bk";
    if (!boost::filesystem::create_directory(bk_path)) {
        LOG(FATAL) << "cannot make directory";
    }
    create_file(bk_path / "epoch", epoch_0_str);
    create_file(bk_path / std::string(limestone::internal::manifest_file_name),
                "{ \"answer\": 42 }");
    // setup entries
    std::vector<limestone::api::file_set_entry> entries;
    entries.emplace_back("epoch", "epoch", false);
    entries.emplace_back(std::string(limestone::internal::manifest_file_name), std::string(limestone::internal::manifest_file_name), false);

    gen_datastore();

    EXPECT_EQ(datastore_->restore(bk_path.string(), entries), limestone::status::err_broken_data);
}

TEST_F(log_dir_test, scan_pwal_files_in_dir_returns_max_epoch_normal) {
    create_file(manifest_path, "{ \"format_version\": \"1.0\", \"persistent_format_version\": 2 }");
    create_file(boost::filesystem::path(location) / "epoch", "\x04\x00\x01\x00\x00\x00\x00\x00\x00"sv);  // not used
    create_file(boost::filesystem::path(location) / "pwal_0000",
                "\x02\xff\x00\x00\x00\x00\x00\x00\x00"
                // XXX: epoch footer...
                "\x02\x00\x01\x00\x00\x00\x00\x00\x00"
                // XXX: epoch footer...
                ""sv);

    gen_datastore();
    EXPECT_EQ(limestone::internal::scan_pwal_files_in_dir(location, 2, is_pwal, 0x100, ignore_entry), 0x100);
}

TEST_F(log_dir_test, scan_pwal_files_in_dir_returns_max_epoch_nondurable) {
    create_file(manifest_path, "{ \"format_version\": \"1.0\", \"persistent_format_version\": 2 }");
    create_file(boost::filesystem::path(location) / "epoch", "\x04\x00\x01\x00\x00\x00\x00\x00\x00"sv);  // not used
    create_file(boost::filesystem::path(location) / "pwal_0000",
                "\x02\xff\x00\x00\x00\x00\x00\x00\x00"
                // XXX: epoch footer...
                "\x02\x01\x01\x00\x00\x00\x00\x00\x00"
                // XXX: epoch footer...
                ""sv);

    gen_datastore();
    EXPECT_EQ(limestone::internal::scan_pwal_files_in_dir(location, 2, is_pwal, 0x100, ignore_entry), 0x101);
}

TEST_F(log_dir_test, scan_pwal_files_in_dir_rejects_unexpected_EOF) {
    create_file(manifest_path, "{ \"format_version\": \"1.0\", \"persistent_format_version\": 2 }");
    create_file(boost::filesystem::path(location) / "epoch", "\x04\x00\x01\x00\x00\x00\x00\x00\x00"sv);  // not used
    create_file(boost::filesystem::path(location) / "pwal_0000",
                "\x02\xff\x00\x00\x00\x00\x00\x00\x00"
                // XXX: epoch footer...
                "\x02\x01\x01\x00\x00\x00"
                ""sv);

    gen_datastore();
    EXPECT_THROW({
        limestone::internal::scan_pwal_files_in_dir(location, 2, is_pwal, 0x100, ignore_entry);
    }, std::exception);
}

TEST_F(log_dir_test, scan_pwal_files_in_dir_rejects_unexpeced_zeros) {
    create_file(manifest_path, "{ \"format_version\": \"1.0\", \"persistent_format_version\": 2 }");
    create_file(boost::filesystem::path(location) / "epoch", "\x04\x00\x01\x00\x00\x00\x00\x00\x00"sv);  // not used
    create_file(boost::filesystem::path(location) / "pwal_0000",
                "\x02\xff\x00\x00\x00\x00\x00\x00\x00"
                // XXX: epoch footer...
                "\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"
                ""sv);

    gen_datastore();
    EXPECT_THROW({
        limestone::internal::scan_pwal_files_in_dir(location, 2, is_pwal, 0x100, ignore_entry);
    }, std::exception);
}

TEST_F(log_dir_test, ut_purge_dir_ok_file1) {
    create_file(manifest_path, "{ \"format_version\": \"1.0\", \"persistent_format_version\": 2 }");
    ASSERT_FALSE(boost::filesystem::is_empty(location));

    ASSERT_EQ(internal::purge_dir(location), status::ok);
    ASSERT_TRUE(boost::filesystem::is_empty(location));
}

/* check purge_dir returns err_permission_error: unimplemented.
   because creating the file that cannnot be deleted by test user requires super-user privileges or similar */
//TEST_F(log_dir_test, ut_purge_dir_err_file1) {}

}  // namespace limestone::testing
