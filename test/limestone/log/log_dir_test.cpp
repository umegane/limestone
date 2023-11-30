
#include <boost/filesystem/path.hpp>
#include <boost/filesystem/fstream.hpp>

#include <limestone/logging.h>

#include "internal.h"
#include "log_entry.h"

#include "test_root.h"

namespace limestone::testing {

class log_dir_test : public ::testing::Test {
public:
static constexpr const char* location = "/tmp/log_dir_test";
const boost::filesystem::path manifest_path = boost::filesystem::path(location) / std::string(limestone::internal::manifest_file_name);

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

    bool starts_with(std::string a, std::string b) { return a.substr(0, b.length()) == b; };

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
    create_file(boost::filesystem::path(location) / "epoch", "\x04\x00\x00\x00\x00\x00\x00\x00\x00");

    gen_datastore();
    EXPECT_THROW({ limestone::internal::check_logdir_format(location); }, std::exception);
}

TEST_F(log_dir_test, reject_directory_with_broken_manifest_file) {
    create_file(boost::filesystem::path(location) / "epoch", "\x04\x00\x00\x00\x00\x00\x00\x00\x00");
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
    create_file(boost::filesystem::path(location) / "epoch", "\x04\x00\x00\x00\x00\x00\x00\x00\x00");
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
    create_file(boost::filesystem::path(location) / std::string(limestone::internal::manifest_file_name),
                "{ \"format_version\": \"1.0\", \"persistent_format_version\": 222 }");

    gen_datastore();
    EXPECT_THROW({ limestone::internal::check_logdir_format(location); }, std::exception);
}

}  // namespace limestone::testing
