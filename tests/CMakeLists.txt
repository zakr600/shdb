add_headers_and_sources(shdb_unit_tests ${CMAKE_CURRENT_SOURCE_DIR})

macro(add_test test_name)
    add_executable(${test_name} ${test_name}.cpp)
    target_link_libraries(${test_name} PRIVATE shdb_core gtest gtest_main)
    target_include_directories(${test_name} PRIVATE "${PROJECT_SOURCE_DIR}/src")
endmacro()

add_test(bp_1_clock_test)
add_test(bp_2_scan_test)
add_test(bp_3_schema_test)
add_test(bp_4_meta_test)

add_test(btree_1_layout)
add_test(btree_2_insert_lookup)
add_test(btree_3_remove)
add_test(btree_4_scan)

add_test(sql_1_ddl)
add_test(sql_2_expr)
add_test(sql_3_insert)
add_test(sql_4_select)

add_test(sql_5_sort)
add_test(sql_6_join)
add_test(sql_7_group_by)
