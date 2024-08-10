#include <gtest/gtest.h>

#include "db.h"
#include "interpreter.h"

namespace
{

std::shared_ptr<shdb::Database> createDatabase(int frame_count)
{
    auto db = shdb::connect("./mydb", frame_count);
    return db;
}

void populate(shdb::Interpreter & interpreter)
{
    interpreter.execute("DROP TABLE test_table");
    interpreter.execute("CREATE TABLE test_table (id int64, age int64, name string, girl boolean)");
    interpreter.execute("INSERT test_table VALUES (0, 10+10, \"Ann\", 1>0)");
    interpreter.execute("INSERT test_table VALUES (1, 10+10+1, \"Bob\", 1<0)");
    interpreter.execute("INSERT test_table VALUES (2, 10+9, \"Sara\", 1>0)");
}

void assertRowsEqual(const std::vector<shdb::Row> & rows, const shdb::RowSet & rowset)
{
    ASSERT_EQ(rows.size(), rowset.getRows().size());
    for (size_t index = 0; index < rows.size(); ++index)
        ASSERT_EQ(rows[index], rowset.getRows()[index]);
}

}

TEST(SQL, Sort)
{
    auto db = createDatabase(1);
    auto interpreter = shdb::Interpreter(db);
    populate(interpreter);

    auto rows1 = std::vector<shdb::Row>{
        {static_cast<int64_t>(2), static_cast<int64_t>(19), std::string("Sara"), true},
        {static_cast<int64_t>(0), static_cast<int64_t>(20), std::string("Ann"), true},
        {static_cast<int64_t>(1), static_cast<int64_t>(21), std::string("Bob"), false}};

    auto result1 = interpreter.execute("SELECT * FROM test_table ORDER BY age");
    assertRowsEqual(rows1, result1);

    auto rows2 = std::vector<shdb::Row>{
        {static_cast<int64_t>(1), static_cast<int64_t>(21), std::string("Bob"), false},
        {static_cast<int64_t>(0), static_cast<int64_t>(20), std::string("Ann"), true},
        {static_cast<int64_t>(2), static_cast<int64_t>(19), std::string("Sara"), true}};

    auto result2 = interpreter.execute("SELECT * FROM test_table ORDER BY age DESC");
    assertRowsEqual(rows2, result2);

    auto rows3 = std::vector<shdb::Row>{
        {static_cast<int64_t>(0), static_cast<int64_t>(20), std::string("Ann"), true},
        {static_cast<int64_t>(1), static_cast<int64_t>(21), std::string("Bob"), false},
        {static_cast<int64_t>(2), static_cast<int64_t>(19), std::string("Sara"), true}};

    auto result3 = interpreter.execute("SELECT * FROM test_table ORDER BY name");
    assertRowsEqual(rows3, result3);

    auto rows4 = std::vector<shdb::Row>{
        {static_cast<int64_t>(2), static_cast<int64_t>(19), std::string("Sara"), true},
        {static_cast<int64_t>(1), static_cast<int64_t>(21), std::string("Bob"), false},
        {static_cast<int64_t>(0), static_cast<int64_t>(20), std::string("Ann"), true}};

    auto result4 = interpreter.execute("SELECT * FROM test_table ORDER BY name DESC");
    assertRowsEqual(rows4, result4);

    auto rows5 = std::vector<shdb::Row>{
        {static_cast<int64_t>(0), static_cast<int64_t>(20), std::string("Ann"), true},
        {static_cast<int64_t>(1), static_cast<int64_t>(21), std::string("Bob"), false},
        {static_cast<int64_t>(2), static_cast<int64_t>(19), std::string("Sara"), true}};

    auto result5 = interpreter.execute("SELECT * FROM test_table ORDER BY name, age");
    assertRowsEqual(rows5, result5);

    auto rows6 = std::vector<shdb::Row>{
        {static_cast<int64_t>(2), static_cast<int64_t>(19), std::string("Sara"), true},
        {static_cast<int64_t>(1), static_cast<int64_t>(21), std::string("Bob"), false},
        {static_cast<int64_t>(0), static_cast<int64_t>(20), std::string("Ann"), true}};

    auto result6 = interpreter.execute("SELECT * FROM test_table ORDER BY name DESC, age");
    assertRowsEqual(rows6, result6);

    auto rows7 = std::vector<shdb::Row>{
        {static_cast<int64_t>(0), static_cast<int64_t>(20), std::string("Ann"), true},
        {static_cast<int64_t>(1), static_cast<int64_t>(21), std::string("Bob"), false},
        {static_cast<int64_t>(2), static_cast<int64_t>(19), std::string("Sara"), true}};

    auto result7 = interpreter.execute("SELECT * FROM test_table ORDER BY name, age DESC");
    assertRowsEqual(rows7, result7);

    auto rows8 = std::vector<shdb::Row>{
        {static_cast<int64_t>(2), static_cast<int64_t>(19), std::string("Sara"), true},
        {static_cast<int64_t>(1), static_cast<int64_t>(21), std::string("Bob"), false},
        {static_cast<int64_t>(0), static_cast<int64_t>(20), std::string("Ann"), true}};

    auto result8 = interpreter.execute("SELECT * FROM test_table ORDER BY age - id * 2");
    assertRowsEqual(rows8, result8);
}
