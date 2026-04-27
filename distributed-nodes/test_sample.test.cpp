
#include <caf/test/test.hpp>
#include <caf/test/caf_test_main.hpp>

// 文档参考 https://actor-framework.readthedocs.io/en/stable/test/UnitTesting.html
// https://www.interance.io/learning/cpp/guide/unit-testing-part-1


TEST("sample_test") {
  check_eq(1 + 1, 2);
}

CAF_TEST_MAIN()
