# Core library (common infrastructure)

add_library(d7kj.core
    lib.cpp
    remote_actor_manager.hpp
    # 其它 core 源文件
)
target_include_directories(d7kj.core PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

# 插件 SDK（提供给插件开发者的接口和类型定义）
add_subdirectory(plugin_sdk)
