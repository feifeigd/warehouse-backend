# Core library (common infrastructure)
add_library(core
    lib.cpp
    remote_actor_manager.hpp
    # 其它 core 源文件
)
target_include_directories(core PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
