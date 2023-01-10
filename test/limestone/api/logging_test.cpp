
#include <sstream>
#include <limestone/logging.h>

#include "test_root.h"

namespace limestone::testing {

class logging_test : public ::testing::Test {
public:
    void SetUp() {
    }

    void TearDown() {
    }

};

TEST_F(logging_test, find_fullname) { // NOLINT
    // check constexpr-ness (if fail, then compile error)
    constexpr std::string_view a = find_fullname(__PRETTY_FUNCTION__, __FUNCTION__);

    ASSERT_EQ(find_fullname("int foo(int)", "foo"), "foo");
    ASSERT_EQ(find_fullname("limestone::api::datastore::recover()", "recover"), "limestone::api::datastore::recover");
    ASSERT_EQ(find_fullname("myclass::myclass()", "myclass"), "myclass::myclass");

    // TODO: closure
    // g++-9
    // ASSERT_EQ(find_fullname("main(int, char**)::<lambda()>", "operator()"), ???);
    // clang++-11
    // ASSERT_EQ(find_fullname("auto main(int, char **)::(anonymous class)::operator()()", "operator()"), ???);
}

TEST_F(logging_test, location_prefix_sv) { // NOLINT
    // check constexpr-ness (if fail, then compile error)
    constexpr auto a = location_prefix<2>(std::string_view("a"));

    // TODO: operators, eg. operator==
    ASSERT_EQ(std::string(location_prefix<50>("limestone::api::datastore::recover").data()), "/:limestone:api:datastore:recover ");
}

TEST_F(logging_test, location_prefix_constchar) { // NOLINT
    // check constexpr-ness (if fail, then compile error)
    constexpr auto a = location_prefix(__PRETTY_FUNCTION__, __FUNCTION__);

    ASSERT_EQ(std::string(location_prefix("limestone::api::datastore::recover()", "recover").data()), "/:limestone:api:datastore:recover ");
    ASSERT_EQ(std::string(location_prefix("foo<myclass>::func(int)", "func").data()), "/:foo:func ");
}

// redirect LOG to local-scope lbuf
#ifdef LOG
#undef LOG
#endif

#define LOG(_ignored) lbuf

class logging_test_foo1 {
public:
    int foo(int& p) {
        std::ostringstream lbuf;
        lbuf.str("");
        LOG_LP(0) << "TEST";
        assert(lbuf.str() == "/:limestone:testing:logging_test_foo1:foo TEST");
        return 0;
    }
};
template<class T, int n>
class logging_test_foo2 {
public:
    T foo(int& p, int &p2) {
        std::ostringstream lbuf;
        // limestone::logging_test_foo2<T, n>::foo
        lbuf.str("");
        LOG_LP(0) << "TEST";
        assert(lbuf.str() == "/:limestone:testing:logging_test_foo2:foo TEST");
        auto lambda1 = [&lbuf](int u){
            // g++-9:      limestone::testing::logging_test_foo2<T, n>::foo<long unsigned int, 99>::<lambda(int)>
            // clang++-11: auto limestone::testing::logging_test_foo2<unsigned long, 99>::foo(int &, int &)::(anonymous class)::operator()(int)
            lbuf.str("");
            LOG_LP(0) << "TEST";
            //assert(lbuf.str() == ???);
        };
        lambda1(1);
        std::function<long double(int)> lambda2 = [&lbuf](int u){
            // g++-9:      limestone::testing::logging_test_foo2<T, n>::foo<long unsigned int, 99>::<lambda(int)>
            // clang++-11: auto limestone::testing::logging_test_foo2<unsigned long, 99>::foo(int &, int &)::(anonymous class)::operator()(int)
            lbuf.str("");
            LOG_LP(0) << "TEST";
            //assert(lbuf.str() == ???);
            return 0.0;
        };
        lambda2(1);
        return 0;
    }
    long double operator() (const char *p) {
        std::ostringstream lbuf;
        LOG_LP(0) << "TEST";
        assert(lbuf.str() == "/:limestone:testing:logging_test_foo2:operator() TEST");
        auto lambda1 = [&lbuf](int u){
            //std::cout << "PF:" << __PRETTY_FUNCTION__ << " F:" << __FUNCTION__ << std::endl;
            // g++-9:      limestone::testing::logging_test_foo2<T, n>::operator()(const char*) [with T = long unsigned int; int n = 99]::<lambda(int)>
            // clang++-11: auto limestone::testing::logging_test_foo2<unsigned long, 99>::operator()(const char *)::(anonymous class)::operator()(int) const [T = unsigned long, n = 99]
            lbuf.str("");
            LOG_LP(0) << "TEST";
            //assert(lbuf.str() == ???);
        };
        lambda1(1);
        return 0.0;
    }
};

TEST_F(logging_test, assert_in_other_methods) { // NOLINT
    int dummy = 1234;
    logging_test_foo1 foo1;
    foo1.foo(dummy);
    logging_test_foo2<unsigned long, 99> foo2;
    foo2.foo(dummy, dummy);
    foo2("a");
}

}  // namespace limestone::testing
