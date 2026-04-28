cmake_minimum_required(VERSION 3.28)

project(warehouse-backend VERSION 1.1.0 LANGUAGES CXX)

# -- version boilerplate -------------------------------------------------------

# Individual components of the version number.
set(WB_VERSION_MAJOR ${PROJECT_VERSION_MAJOR}
    CACHE INTERNAL "The major version of warehouse-backend" FORCE)
set(WB_VERSION_MINOR ${PROJECT_VERSION_MINOR}
    CACHE INTERNAL "The minor version of warehouse-backend" FORCE)
set(WB_VERSION_PATCH ${PROJECT_VERSION_PATCH}
    CACHE INTERNAL "The patch version of warehouse-backend" FORCE)

# The full version number as human-readable string.
set(WB_VERSION "${WB_VERSION_MAJOR}.${WB_VERSION_MINOR}.${WB_VERSION_PATCH}"
    CACHE INTERNAL "The full warehouse-backend version string" FORCE)

# 在顶层 CMakeLists.txt 中设置
set(CMAKE_CXX_STANDARD 20)       # 设置 C++ 20
set(CMAKE_CXX_STANDARD_REQUIRED ON)  # 必须支持该标准
set(CMAKE_CXX_EXTENSIONS OFF)    # 禁用编译器扩展（如 GNU 扩展）

# 设置所有 target 的输出目录为 bin
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin) # 可执行文件
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin) # 动态库
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib) # 静态库放在 lib 目录


if(MSVC)
    add_compile_options(/utf-8)
endif()

# -- includes 
include(FetchContent)

# -- options
option(BUILD_SHARED_LIBS "Build shared library targets" ON)
set(CAF_TAG "1.1.0" CACHE STRING "The tag of the CAF version to use")
set(CAF_ENABLE_PROTOBUF_EXAMPLES ON CACHE BOOL "Whether to build CAF protobuf examples")
# -- get our dependencies
find_package(SQLite3 REQUIRED)

# -- embed CAF
function(get_caf)
    set(CAF_CXX_VERSION ${CMAKE_CXX_STANDARD} CACHE STRING "Set the C++ version to use for CAF")
    set(CAF_ENABLE_EXAMPLES OFF CACHE BOOL "" FORCE)
    # set(CAF_ENABLE_IO_MODULE ON CACHE BOOL "" FORCE)
    # set(CAF_ENABLE_NET_MODULE ON CACHE BOOL "" FORCE)
    # set(CAF_ENABLE_TESTING ON CACHE BOOL "" FORCE)
    set(CAF_ENABLE_TOOLS ON CACHE BOOL "" FORCE)

    FetchContent_Declare(
        caf 
        GIT_REPOSITORY https://github.com/actor-framework/actor-framework.git
        GIT_TAG ${CAF_TAG}
    )

    FetchContent_MakeAvailable(caf)
    # 设置 CAF 相关 target 在 VS 的同一目录
    get_property(caf_targets DIRECTORY ${caf_SOURCE_DIR} PROPERTY BUILDSYSTEM_TARGETS)
    
    foreach(tgt ${caf_targets})
        set_target_properties(${tgt} PROPERTIES FOLDER "ThirdParty/CAF")
        message(STATUS "Set target ${tgt} to folder ThirdParty/CAF")
    endforeach()
    set_target_properties(
        caf-net-autobahn-driver
        caf-test-test
        core_enums
        io_enums
        libcaf_core caf-core-test 
        libcaf_io caf-io-test
        libcaf_net caf-net-test
        libcaf_openssl caf-openssl-test 
        net_enums
        test_enums
        PROPERTIES FOLDER "ThirdParty/CAF")
    
    # CAF的动态库搞过来
    set_target_properties(libcaf_core libcaf_io libcaf_net libcaf_openssl PROPERTIES
        LIBRARY_OUTPUT_DIRECTORY ${CMAKE_LIBRARY_OUTPUT_DIRECTORY}
    )

endfunction(get_caf)
get_caf()

add_subdirectory(distributed-actors)
add_subdirectory(distributed-nodes)
add_subdirectory(warehouse-backend)
