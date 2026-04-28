

add_subdirectory(core)
add_subdirectory(services)

set(DISTRIBUTED_NODES_CONFIG_FILES
  config/caf-application.conf
  config/master.conf
  config/region-a.conf
  config/compute-a1.conf
  config/storage-a1.conf
  config/client.conf
)

function(add_distributed_nodes_executable target_name service_name entry_file)
  add_executable(${target_name}
    app/${entry_file}
    
    ${DISTRIBUTED_NODES_CONFIG_FILES}
  )
  target_link_libraries(${target_name} PRIVATE CAF::core CAF::io CAF::net d7kj.core ${service_name})
  
  source_group("Config Files" FILES ${DISTRIBUTED_NODES_CONFIG_FILES})
  set_target_properties(${target_name} PROPERTIES
    VS_DEBUGGER_WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}"
    FOLDER "app-nodes"
  )
  
endfunction()

add_distributed_nodes_executable(distributed-nodes-master master_service master_main.cpp)
add_distributed_nodes_executable(distributed-nodes-region region_service region_main.cpp)
add_distributed_nodes_executable(distributed-nodes-compute compute_service compute_main.cpp)
add_distributed_nodes_executable(distributed-nodes-storage storage_service storage_main.cpp)
add_distributed_nodes_executable(distributed-nodes-client  "" client_main.cpp)

add_custom_target(distributed-nodes)
add_dependencies(distributed-nodes
  distributed-nodes-master
  distributed-nodes-region
  distributed-nodes-compute
  distributed-nodes-storage
  distributed-nodes-client
)

set_target_properties(distributed-nodes PROPERTIES FOLDER "distributed-nodes")

add_subdirectory(tests)
