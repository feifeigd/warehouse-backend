add_library(storage_service STATIC
    lib.cpp
    # ...其它 storage_service 相关文件
)
# target_link_libraries(storage_service PUBLIC core)
target_include_directories(storage_service PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
set_target_properties(storage_service PROPERTIES FOLDER "services")
