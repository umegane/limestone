
#include <atomic>

#include <unistd.h>
#include <stdlib.h>
#include <xmmintrin.h>
#include "TestRoot.h"

namespace limestone::testing {

class DatastoreTest : public ::testing::Test {
public:
    const char* data_location = "/tmp/data_location";
    static inline std::atomic<std::size_t> durable_epoch_{0};

    virtual void SetUp() {
        // initialize
        set_durable_epoch(0);
    }

    virtual void TearDown() {
        datastore_ = nullptr;
    }

    static std::size_t get_durable_epoch() {
         return durable_epoch_.load(std::memory_order_acquire);
    }

    static void set_durable_epoch(std::size_t n) {
        durable_epoch_.store(n, std::memory_order_release);
    }

protected:
    std::unique_ptr<limestone::api::datastore_test> datastore_{};
};

TEST_F(DatastoreTest, add_persistent_callback_test) { // NOLINT
    FLAGS_stderrthreshold = 0;

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

    // register persistent callback
    datastore_->add_persistent_callback(set_durable_epoch);

    // epoch 1
    datastore_->switch_epoch(1);

    // ready
    datastore_->ready();

    // epoch 2
    datastore_->switch_epoch(2);

    for (;;) {
        if (get_durable_epoch() >= 1) {
            break;
        }
        _mm_pause();
#if 1
        // todo remove this block. now, infinite loop at this.
        LOG(INFO);
        sleep(1);
#endif
    }

    // epoch 3
    datastore_->switch_epoch(3);

    for (;;) {
        if (get_durable_epoch() >= 2) {
            break;
        }
        _mm_pause();
    }

}

}  // namespace limestone::testing
