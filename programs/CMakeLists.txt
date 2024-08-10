add_headers_and_sources(shdb ${CMAKE_CURRENT_SOURCE_DIR})
add_executable(shdb ${shdb_sources})
target_link_libraries(shdb PRIVATE shdb_core)
target_include_directories(shdb PRIVATE "${PROJECT_SOURCE_DIR}/src")
