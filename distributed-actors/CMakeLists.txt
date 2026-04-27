set(DISTRIBUTED_ACTORS_CODEX_CONFIG_FILES
  caf-application.conf
  seed.conf
  worker-a.conf
  worker-b.conf
  client.conf
)

add_executable(distributed-actors
  main.cpp
  ${DISTRIBUTED_ACTORS_CODEX_CONFIG_FILES}
)

target_link_libraries(distributed-actors PRIVATE CAF::io)

source_group("Config Files" FILES ${DISTRIBUTED_ACTORS_CODEX_CONFIG_FILES})

set_target_properties(distributed-actors PROPERTIES
  VS_DEBUGGER_WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}"
)
