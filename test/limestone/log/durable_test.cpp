
#include <algorithm>
#include <sstream>
#include <limestone/logging.h>

#include <boost/filesystem/path.hpp>
#include <boost/filesystem/fstream.hpp>

#include "test_root.h"

namespace limestone::testing {

class durable_test : public ::testing::Test {
public:
static constexpr const char* location = "/tmp/durable_test";

    void SetUp() {
        boost::filesystem::remove_all(location);
        if (!boost::filesystem::create_directory(location)) {
            std::cerr << "cannot make directory" << std::endl;
        }

        regen_datastore();
    }

    void regen_datastore() {
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

TEST_F(durable_test, last_record_will_ignored) {
    using namespace limestone::api;

    datastore_->ready();
    log_channel& channel = datastore_->create_channel(boost::filesystem::path(location));
    log_channel& channel2 = datastore_->create_channel(boost::filesystem::path(location));
    datastore_->switch_epoch(42);
    channel.begin_session();
    channel.add_entry(3, "k1", "v1", {42, 4});
    channel.end_session();
    datastore_->switch_epoch(43);
    channel2.begin_session();  // another channel running
    channel.begin_session();
    channel.add_entry(3, "k2", "v2", {43, 5});
    channel.end_session();
    //datastore_->switch_epoch is not called

    datastore_->shutdown();
    regen_datastore();
    // setup done

    datastore_->recover();
    datastore_->ready();
    auto snapshot = datastore_->get_snapshot();
    auto cursor = snapshot->get_cursor();
    std::string buf;

    ASSERT_TRUE(cursor->next());
    EXPECT_EQ(cursor->storage(), 3);
    EXPECT_EQ((cursor->key(buf), buf), "k1");
    EXPECT_EQ((cursor->value(buf), buf), "v1");
    EXPECT_FALSE(cursor->next());  // only 1 entry
    datastore_->shutdown();
}

}  // namespace limestone::testing
