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
)

set_compile_options(${package_name})

install_custom(${package_name} ${export_name})
