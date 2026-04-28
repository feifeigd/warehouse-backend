add_library(compute_service STATIC
    lib.cpp
    # ...其它 compoute_service 相关文件
)
# target_link_libraries(compute_service PUBLIC core)
target_include_directories(compute_service PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
set_target_properties(compute_service PROPERTIES FOLDER "services")
