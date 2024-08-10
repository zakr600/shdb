#include <gtest/gtest.h>

#include "db.h"
#include "interpreter.h"

TEST(SQL, Expression)
{
    auto interpreter = shdb::Interpreter(nullptr);

    auto check = [&](const std::string & query, const shdb::Row & expected)
    {
        auto result = interpreter.execute(query);
        ASSERT_EQ(result.getRows().size(), 1);
        ASSERT_EQ(result.getRows()[0], expected);
    };

    check("SELECT 11", {static_cast<int64_t>(11)});
    check("SELECT 5 - 10", {static_cast<int64_t>(-5)});
    check("SELECT 11+11", {static_cast<int64_t>(22)});
    check("SELECT 2*2", {static_cast<int64_t>(4)});
    check("SELECT 1 > 0", {true});
//    check("SELECT (50-30)*2 <= 1*2*3*4", {false});
    check("SELECT \"Hi\"", {std::string("Hi")});
    check(R"(SELECT "Mike", "Bob", 1+2, 1>0)", {std::string("Mike"), std::string("Bob"), static_cast<int64_t>(3), true});
}

TEST(SQL2, Expression2)
{
    auto interpreter = shdb::Interpreter(nullptr);

    auto check = [&](const std::string & query, const shdb::Row & expected)
    {
        auto result = interpreter.execute(query);
        ASSERT_EQ(result.getRows().size(), 1);
        ASSERT_EQ(result.getRows()[0], expected);
    };

//    check("SELECT 11", {static_cast<int64_t>(11)});
//    check("SELECT 5 - 10", {static_cast<int64_t>(-5)});
//    check("SELECT 11+11", {static_cast<int64_t>(22)});
//    check("SELECT 2*2", {static_cast<int64_t>(4)});
//    check("SELECT 1 > 0", {true});
    check("SELECT (50-30)*2 <= 1*2*3*4", {false});
//    check("SELECT \"Hi\"", {std::string("Hi")});
//    check(R"(SELECT "Mike", "Bob", 1+2, 1>0)", {std::string("Mike"), std::string("Bob"), static_cast<int64_t>(3), true});
}
