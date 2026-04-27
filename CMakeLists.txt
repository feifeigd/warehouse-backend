cmake_minimum_required(VERSION 3.28)

project(warehouse-backend CXX)

# 在顶层 CMakeLists.txt 中设置
set(CMAKE_CXX_STANDARD 20)       # 设置 C++ 20
set(CMAKE_CXX_STANDARD_REQUIRED ON)  # 必须支持该标准
set(CMAKE_CXX_EXTENSIONS OFF)    # 禁用编译器扩展（如 GNU 扩展）

if(MSVC)
    add_compile_options(/utf-8)
endif()

# -- includes 
include(FetchContent)

# -- options
option(BUILD_SHARED_LIBS "Build shared library targets" OFF)
set(CAF_TAG "1.1.0" CACHE STRING "The tag of the CAF version to use")

# -- get our dependencies
find_package(SQLite3 REQUIRED)

# -- embed CAF
function(get_caf)
    set(CAF_CXX_VERSION ${CMAKE_CXX_STANDARD} CACHE STRING "Set the C++ version to use for CAF")
    set(CAF_ENABLE_EXAMPLES OFF CACHE BOOL "" FORCE)
    set(CAF_ENABLE_IO_MODULE ON CACHE BOOL "" FORCE)
    set(CAF_ENABLE_NET_MODULE ON CACHE BOOL "" FORCE)
    set(CAF_ENABLE_TESTING ON CACHE BOOL "" FORCE)
    set(CAF_ENABLE_TOOLS OFF CACHE BOOL "" FORCE)

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
    

endfunction(get_caf)
get_caf()

add_subdirectory(distributed-actors)
add_subdirectory(distributed-nodes)
add_subdirectory(warehouse-backend)
