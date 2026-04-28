add_executable(plugin_demo main.cpp)
target_include_directories(plugin_demo PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
set_target_properties(plugin_demo PROPERTIES FOLDER "core/plugin")
target_link_libraries(plugin_demo PRIVATE plugin_sdk)
