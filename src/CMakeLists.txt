file(GLOB SOURCES
        "limestone/*.cpp"
)

add_library(${package_name}
        ${SOURCES}
)

set_target_properties(${package_name} PROPERTIES
        OUTPUT_NAME ${export_name}
        )

target_link_libraries(${package_name}
        PUBLIC limestone-api
        PRIVATE Boost::boost
        PRIVATE Boost::filesystem
        PRIVATE Boost::thread
        PRIVATE Boost::container
        PRIVATE glog::glog
        PRIVATE leveldb
)

set_compile_options(${package_name})

install_custom(${package_name} ${export_name})

# for tests
add_library(limestone-impl INTERFACE)

target_include_directories(limestone-impl
        INTERFACE .
        )

target_link_libraries(limestone-impl
        INTERFACE ${package_name}
        INTERFACE Boost::boost
        INTERFACE Boost::filesystem
        INTERFACE Boost::thread
        INTERFACE Boost::container
        INTERFACE glog::glog
        INTERFACE leveldb
)
