
file(GLOB SOURCES *.c* *.h*)
add_executable(warehouse-backend ${SOURCES} work/caf-application.conf)
target_link_libraries(warehouse-backend PRIVATE CAF::net CAF::io SQLite::SQLite3)
set_target_properties(warehouse-backend PROPERTIES
  VS_DEBUGGER_WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/work"
)
