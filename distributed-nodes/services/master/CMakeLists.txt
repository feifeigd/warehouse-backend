add_library(master_service STATIC
    lib.cpp
    # ...其它 master_service 相关文件
)
target_link_libraries(master_service PUBLIC core)
target_include_directories(master_service PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
set_target_properties(master_service PROPERTIES FOLDER "services")
