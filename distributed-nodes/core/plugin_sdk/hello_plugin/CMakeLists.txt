add_library(hello_plugin SHARED HelloPlugin.cpp)
target_include_directories(hello_plugin PRIVATE ${CMAKE_SOURCE_DIR}/core/plugin)
set_target_properties(hello_plugin PROPERTIES PREFIX "" OUTPUT_NAME "hello_plugin")
