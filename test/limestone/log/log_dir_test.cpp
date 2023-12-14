
#include <boost/filesystem/path.hpp>
#include <boost/filesystem/fstream.hpp>

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

    bool starts_with(std::string a, std::string b) { return a.substr(0, b.length()) == b; }

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

}  // namespace limestone::testing
