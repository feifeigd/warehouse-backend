set(DISTRIBUTED_NODES_SHARED_FILES
  app.hpp
)

set(DISTRIBUTED_NODES_CONFIG_FILES
  caf-application.conf
  master.conf
  region-a.conf
  compute-a1.conf
  storage-a1.conf
  client.conf
)

function(add_distributed_nodes_executable target_name entry_file)
  add_executable(${target_name}
    ${entry_file}
    ${DISTRIBUTED_NODES_SHARED_FILES}
    ${DISTRIBUTED_NODES_CONFIG_FILES}
  )
  target_link_libraries(${target_name} PRIVATE CAF::io)
  source_group("Shared Files" FILES ${DISTRIBUTED_NODES_SHARED_FILES})
  source_group("Config Files" FILES ${DISTRIBUTED_NODES_CONFIG_FILES})
  set_target_properties(${target_name} PROPERTIES
    VS_DEBUGGER_WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}"
  )
endfunction()

add_distributed_nodes_executable(distributed-nodes-master master_main.cpp)
add_distributed_nodes_executable(distributed-nodes-region region_main.cpp)
add_distributed_nodes_executable(distributed-nodes-compute compute_main.cpp)
add_distributed_nodes_executable(distributed-nodes-storage storage_main.cpp)
add_distributed_nodes_executable(distributed-nodes-client client_main.cpp)

add_custom_target(distributed-nodes)
add_dependencies(distributed-nodes
  distributed-nodes-master
  distributed-nodes-region
  distributed-nodes-compute
  distributed-nodes-storage
  distributed-nodes-client
)
