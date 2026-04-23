cmake_minimum_required(VERSION 3.28)

project(warehouse-backend CXX)

# 在顶层 CMakeLists.txt 中设置
set(CMAKE_CXX_STANDARD 20)       # 设置 C++ 20
set(CMAKE_CXX_STANDARD_REQUIRED ON)  # 必须支持该标准
set(CMAKE_CXX_EXTENSIONS OFF)    # 禁用编译器扩展（如 GNU 扩展）

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
    set(CAF_ENABLE_TESTING OFF CACHE BOOL "" FORCE)
    set(CAF_ENABLE_TOOLS OFF CACHE BOOL "" FORCE)

    FetchContent_Declare(
        caf 
        GIT_REPOSITORY https://github.com/actor-framework/actor-framework.git
        GIT_TAG ${CAF_TAG}
    )

    FetchContent_MakeAvailable(caf)

endfunction(get_caf)
get_caf()

file(GLOB SOURCES *.c* *.h*)
add_executable(warehouse-backend ${SOURCES} work/caf-application.conf)
target_link_libraries(warehouse-backend PRIVATE CAF::net SQLite::SQLite3)
set_target_properties(warehouse-backend PROPERTIES
  VS_DEBUGGER_WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/work"
)
