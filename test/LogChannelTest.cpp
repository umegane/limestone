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
#include "TestRoot.h"

namespace limestone::testing {

class LogChannelTest : public ::testing::Test {
    virtual void SetUp() {
        datastore_ = std::make_unique<limestone::api::datastore>();
    }

    virtual void TearDown() {
        datastore_ = nullptr;
    }

protected:
    std::unique_ptr<limestone::api::datastore> datastore_{};
};

TEST_F(LogChannelTest, number) {
    limestone::api::log_channel& channel1 = datastore_->create_channel(boost::filesystem::path("1"));
    limestone::api::log_channel& channel2 = datastore_->create_channel(boost::filesystem::path("2"));
    limestone::api::log_channel& channel3 = datastore_->create_channel(boost::filesystem::path("3"));
    limestone::api::log_channel& channel4 = datastore_->create_channel(boost::filesystem::path("4"));

    channel1.begin_session();
    channel2.begin_session();
    channel3.begin_session();
    channel4.begin_session();

    EXPECT_EQ(datastore_->log_channels().size(), 4);

    channel1.end_session();
    channel2.end_session();
    channel3.end_session();
    channel4.end_session();

    EXPECT_TRUE(datastore_->log_channels().empty());
}

}  // namespace limestone::testing
