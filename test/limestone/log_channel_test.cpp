/*
 * Copyright 2022-2022 tsurugi project.
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
#include <unistd.h>
#include "test_root.h"

namespace limestone::testing {

class log_channel_test : public ::testing::Test {
public:
    virtual void SetUp() {
        if (system("rm -f /tmp/pwal_000?") != 0) {
            std::cerr << "cannot remove directory" << std::endl;
        }

        std::vector<boost::filesystem::path> data_locations{};
        data_locations.emplace_back("/tmp");
        boost::filesystem::path metadata_location{"/tmp"};
        limestone::api::configuration conf(data_locations, metadata_location);

        datastore_ = std::make_unique<limestone::api::datastore_test>(conf);
    }

    virtual void TearDown() {
        datastore_ = nullptr;
    }

protected:
    std::unique_ptr<limestone::api::datastore_test> datastore_{};
};

TEST_F(log_channel_test, name) {
    limestone::api::log_channel& channel = datastore_->create_channel(boost::filesystem::path("/tmp"));
    EXPECT_EQ(channel.file_path().string(), "/tmp/pwal_0000");
}

TEST_F(log_channel_test, number_and_backup) {
    limestone::api::log_channel& channel1 = datastore_->create_channel(boost::filesystem::path("/tmp"));
    limestone::api::log_channel& channel2 = datastore_->create_channel(boost::filesystem::path("/tmp"));
    limestone::api::log_channel& channel3 = datastore_->create_channel(boost::filesystem::path("/tmp"));
    limestone::api::log_channel& channel4 = datastore_->create_channel(boost::filesystem::path("/tmp"));

    channel1.begin_session();
    channel2.begin_session();
    channel3.begin_session();
    channel4.begin_session();

    EXPECT_EQ(datastore_->log_channels().size(), 4);

    channel1.end_session();
    channel2.end_session();
    channel3.end_session();
    channel4.end_session();

    EXPECT_EQ(datastore_->log_channels().size(), 4);

    auto backup = datastore_->begin_backup();
    auto files = backup.files();

    EXPECT_EQ(files.size(), 4);
    EXPECT_EQ(files.at(0).string(), "/tmp/pwal_0000");
    EXPECT_EQ(files.at(1).string(), "/tmp/pwal_0001");
    EXPECT_EQ(files.at(2).string(), "/tmp/pwal_0002");
    EXPECT_EQ(files.at(3).string(), "/tmp/pwal_0003");
}

}  // namespace limestone::testing
