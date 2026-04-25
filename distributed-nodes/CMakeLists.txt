set(DISTRIBUTED_NODES_CONFIG_FILES
  caf-application.conf
  master.conf
  region-a.conf
  compute-a1.conf
  storage-a1.conf
  client.conf
)

add_executable(distributed-nodes
  main.cpp
  ${DISTRIBUTED_NODES_CONFIG_FILES}
)

target_link_libraries(distributed-nodes PRIVATE CAF::io)

source_group("Config Files" FILES ${DISTRIBUTED_NODES_CONFIG_FILES})

set_target_properties(distributed-nodes PROPERTIES
  VS_DEBUGGER_WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}"
)
