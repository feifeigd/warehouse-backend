
add_library(plugin_sdk
    lib.cpp
    IPlugin.h
    plugin_types.hpp
    PluginManager.hpp
    PluginManager.cpp

)
target_include_directories(plugin_sdk PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(plugin_sdk PUBLIC CAF::core)

add_subdirectory(hello_main)
add_subdirectory(hello_plugin)
