add_library(region_service STATIC
    lib.cpp
    # ...其它 region 相关文件
)
target_link_libraries(region_service PUBLIC core)
target_include_directories(region_service PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
