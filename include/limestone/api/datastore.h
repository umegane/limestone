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
#pragma once

#include <memory>
#include <future>
#include <set>

#include <boost/filesystem/path.hpp>

#include <limestone/api/backup.h>
#include <limestone/api/log_channel.h>
#include <limestone/api/configuration.h>
#include <limestone/api/snapshot.h>
#include <limestone/api/epoch_id_type.h>
#include <limestone/api/write_version_type.h>
#include <limestone/api/tag_repository.h>

namespace limestone::api {

template<class T>
struct pointer_comp {
    using is_transparent = std::true_type;
    // helper does some magic in order to reduce the number of
    // pairs of types we need to know how to compare: it turns
    // everything into a pointer, and then uses `std::less<T*>`
    // to do the comparison:
    class helper {
        T* ptr;
    public:
        helper():ptr(nullptr) {}
        helper(helper const&) = default;
        helper(helper&&) noexcept = default;
        helper& operator = (helper const&) = default;
        helper& operator = (helper&&) noexcept = default;
        helper(T* const p):ptr(p) {}  //NOLINT
        template<class U>
        helper( std::shared_ptr<U> const& sp ):ptr(sp.get()) {}  //NOLINT
        template<class U, class...Ts>
        helper( std::unique_ptr<U, Ts...> const& up ):ptr(up.get()) {}  //NOLINT
        ~helper() = default;
        // && optional: enforces rvalue use only
        bool operator<( helper const o ) const {
            return std::less<T*>()( ptr, o.ptr );
        }
    };
    // without helper, we would need 2^n different overloads, where
    // n is the number of types we want to support (so, 8 with
    // raw pointers, unique pointers, and shared pointers).  That
    // seems silly:
    // && helps enforce rvalue use only
    bool operator()( helper const&& lhs, helper const&& rhs ) const {
        return lhs < rhs;
    }
};

class datastore {
public:
    
    datastore() = default;  // FIXME
    explicit datastore(configuration conf);

    ~datastore();

    void recover();

    void ready();

    snapshot& get_snapshot();

    std::shared_ptr<snapshot> shared_snapshot();

    log_channel& create_channel(boost::filesystem::path location);

    epoch_id_type last_epoch();

    void switch_epoch(epoch_id_type epoch_id);

    void add_persistent_callback(std::function<void(epoch_id_type)> callback);

    void switch_safe_snapshot(write_version_type write_version, bool inclusive);

    void add_snapshot_callback(std::function<void(write_version_type)> callback);

    std::future<void> shutdown();

    backup& begin_backup();

    tag_repository& epoch_tag_repository();

    void recover(epoch_tag);

    auto& log_channels() { return channels_; }  // for test purpose
    
private:

    std::set<std::unique_ptr<log_channel>, pointer_comp<log_channel>> channels_{};

    std::unique_ptr<backup> backup_{};

    std::function<void(epoch_id_type)> persistent_callback_;

    std::function<void(write_version_type)> snapshot_callback_;

    tag_repository tag_repository_{};

    void erase_log_channel(log_channel* lc);

    friend class log_channel;

};

} // namespace limestone::api
