
#include <atomic>

#include <unistd.h>
#include <stdlib.h>
#include <xmmintrin.h>
#include "TestRoot.h"

namespace limestone::testing {

class LogAndRecoverTest : public ::testing::Test {
public:
    const char* data_location = "/tmp/data_location";

    virtual void SetUp() {
        if (system("rm -rf /tmp/data_location /tmp/metadata_location") != 0) {
            std::cerr << "cannot remove directory" << std::endl;
        }
        if (system("rm -rf /tmp/data_location /tmp/metadata_location") != 0) {
            std::cerr << "cannot make directory" << std::endl;
        }

        std::vector<boost::filesystem::path> data_locations{};
        data_locations.emplace_back(data_location);
        boost::filesystem::path metadata_location{"/tmp/metadata_location"};
        limestone::api::configuration conf(data_locations, metadata_location);

        datastore_ = std::make_unique<limestone::api::datastore_test>(conf);

        limestone::api::log_channel& channel = datastore_->create_channel(boost::filesystem::path(data_location));

        // prepare durable epoch
        std::atomic<std::size_t> durable_epoch{0};
        auto set_durable_epoch = [&durable_epoch](std::size_t n) {
                                     durable_epoch.store(n, std::memory_order_release);
                                 };
        auto get_durable_epoch = [&durable_epoch]() {
                                     return durable_epoch.load(std::memory_order_acquire);
                                 };

        // register persistent callback
        datastore_->add_persistent_callback(set_durable_epoch);

        // epoch 1
        datastore_->switch_epoch(1);

        // ready
        datastore_->ready();

        // log 1 entry
        channel.begin_session();
        std::string k{"k"};
        std::string v{"v"};
        limestone::api::storage_id_type st{2};
        channel.add_entry(st, k, v, {1, 0});
        channel.end_session();

        // epoch 2
        datastore_->switch_epoch(2);

        // wait epoch 1's durable
        for (;;) {
            if (get_durable_epoch() >= 1) {
                break;
            }
            _mm_pause();
        }

        // cleanup
        datastore_->shutdown();
    }

    virtual void TearDown() {
        datastore_ = nullptr;
    }

protected:
    std::unique_ptr<limestone::api::datastore_test> datastore_{};
};

TEST_F(LogAndRecoverTest, Recovery) {
    // recover and ready
    datastore_->recover();
    datastore_->ready();

    // create snapshot
    limestone::api::snapshot* ss{datastore_->get_snapshot()};
    ASSERT_TRUE(ss->get_cursor().next()); // point first
    std::string buf{};
    ss->get_cursor().key(buf);
    ASSERT_EQ(buf, "k");
    ss->get_cursor().value(buf);
    ASSERT_EQ(buf, "v");
    ASSERT_EQ(ss->get_cursor().storage(), 2);
    ASSERT_FALSE(ss->get_cursor().next()); // nothing

    // cleanup
    datastore_->shutdown();
}

}  // namespace limestone::testing
