# 下个最简单的测试
add_executable(test_sample test_sample.test.cpp)
target_link_libraries(test_sample PRIVATE CAF::core CAF::io CAF::test)
add_test(NAME test_sample COMMAND test_sample)
set_target_properties(test_sample PROPERTIES FOLDER "tests")
