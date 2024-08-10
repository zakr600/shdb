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
    interpreter.execute("INSERT test_table VALUES (0, 20, \"Ann\", 1>0)");
    interpreter.execute("INSERT test_table VALUES (1, 21, \"Bob\", 1<0)");
    interpreter.execute("INSERT test_table VALUES (2, 19, \"Sara\", 1>0)");
    interpreter.execute("INSERT test_table VALUES (3, 20, \"Ann\", 1>0)");
    interpreter.execute("INSERT test_table VALUES (4, 15, \"Bob\", 1<0)");
    interpreter.execute("INSERT test_table VALUES (5, 15, \"Bob\", 1<0)");
}

void assertRowsEqual(const std::vector<shdb::Row> & rows, const shdb::RowSet & rowset)
{
    ASSERT_EQ(rows.size(), rowset.getRows().size());
    for (size_t index = 0; index < rows.size(); ++index)
        ASSERT_EQ(rows[index], rowset.getRows()[index]);
}

}

TEST(SQL, GROUPBY)
{
    auto db = createDatabase(1);
    auto interpreter = shdb::Interpreter(db);
    populate(interpreter);

    auto rows1 = std::vector<shdb::Row>{
        {static_cast<int64_t>(15), static_cast<int64_t>(2)},
        {static_cast<int64_t>(19), static_cast<int64_t>(1)},
        {static_cast<int64_t>(20), static_cast<int64_t>(2)},
        {static_cast<int64_t>(21), static_cast<int64_t>(1)}};

    auto result1 = interpreter.execute("SELECT age, sum(1) FROM test_table GROUP BY age ORDER BY age");
    assertRowsEqual(rows1, result1);

    auto rows2 = std::vector<shdb::Row>{
        {std::string("Ann"), static_cast<int64_t>(40)},
        {std::string("Bob"), static_cast<int64_t>(51)},
        {std::string("Sara"), static_cast<int64_t>(19)}};

    auto result2 = interpreter.execute("SELECT name, sum(age) FROM test_table GROUP BY name ORDER BY name");
    assertRowsEqual(rows2, result2);

    auto rows3 = std::vector<shdb::Row>{
        {std::string("Ann"), static_cast<int64_t>(20), static_cast<int64_t>(2)},
        {std::string("Bob"), static_cast<int64_t>(15), static_cast<int64_t>(2)},
        {std::string("Bob"), static_cast<int64_t>(21), static_cast<int64_t>(1)},
        {std::string("Sara"), static_cast<int64_t>(19), static_cast<int64_t>(1)}};

    auto result3 = interpreter.execute("SELECT name, age, sum(1) FROM test_table GROUP BY name, age ORDER BY name, age");
    assertRowsEqual(rows3, result3);

    auto rows4 = std::vector<shdb::Row>{
        {std::string("Ann"), static_cast<int64_t>(20)},
        {std::string("Bob"), static_cast<int64_t>(15)},
        {std::string("Bob"), static_cast<int64_t>(21)},
        {std::string("Sara"), static_cast<int64_t>(19)}};

    auto result4 = interpreter.execute("SELECT * FROM test_table GROUP BY name, age ORDER BY name, age");
    assertRowsEqual(rows4, result4);

    auto rows5 = std::vector<shdb::Row>{
        {std::string("Ann"), static_cast<int64_t>(20), static_cast<int64_t>(20), static_cast<int64_t>(20)},
        {std::string("Bob"), static_cast<int64_t>(15), static_cast<int64_t>(21), static_cast<int64_t>(17)},
        {std::string("Sara"), static_cast<int64_t>(19), static_cast<int64_t>(19), static_cast<int64_t>(19)}};

    auto result5 = interpreter.execute("SELECT name, min(age), max(age), avg(age) FROM test_table GROUP BY name ORDER BY name");
    assertRowsEqual(rows5, result5);

    auto rows6 = std::vector<shdb::Row>{
        {std::string("Ann"), static_cast<int64_t>(20), static_cast<int64_t>(2)},
        {std::string("Bob"), static_cast<int64_t>(21), static_cast<int64_t>(1)}};

    auto result6 = interpreter.execute("SELECT name, age, sum(1) FROM test_table WHERE age >=20 GROUP BY name, age ORDER BY name, age");
    assertRowsEqual(rows6, result6);

    auto rows7 = std::vector<shdb::Row>{
        {std::string("Ann"), static_cast<int64_t>(20), static_cast<int64_t>(2)},
        {std::string("Bob"), static_cast<int64_t>(15), static_cast<int64_t>(2)}};

    auto result7 = interpreter.execute("SELECT name, age, sum(1) FROM test_table GROUP BY name, age HAVING sum(1) > 1 ORDER BY name, age");
    assertRowsEqual(rows7, result7);

    auto rows8 = std::vector<shdb::Row>{{std::string("Bob"), static_cast<int64_t>(66)}};

    auto result8 = interpreter.execute("SELECT name, min(age) + sum(age) FROM test_table GROUP BY name HAVING max(age) - avg(age) > 0");
    assertRowsEqual(rows8, result8);

    auto rows9 = std::vector<shdb::Row>{
        {std::string("Bob"), static_cast<int64_t>(15)},
        {std::string("Sara"), static_cast<int64_t>(19)},
        {std::string("Ann"), static_cast<int64_t>(20)}};

    auto result9 = interpreter.execute("SELECT name, min(age) FROM test_table GROUP BY name ORDER BY min(age)");
    assertRowsEqual(rows9, result9);

    auto rows10 = std::vector<shdb::Row>{
        {static_cast<int64_t>(19), static_cast<int64_t>(15), static_cast<int64_t>(15)},
        {static_cast<int64_t>(20), static_cast<int64_t>(15), static_cast<int64_t>(20)},
        {static_cast<int64_t>(21), static_cast<int64_t>(19), static_cast<int64_t>(19)},
        {static_cast<int64_t>(22), static_cast<int64_t>(21), static_cast<int64_t>(21)},
        {static_cast<int64_t>(23), static_cast<int64_t>(20), static_cast<int64_t>(20)}};

    auto result10 = interpreter.execute("SELECT age + id, min(age), max(age) FROM test_table GROUP BY age + id ORDER BY age + id");
    assertRowsEqual(rows10, result10);

    auto rows11 = std::vector<shdb::Row>{
        {std::string("Ann"), static_cast<int64_t>(23)},
        {std::string("Bob"), static_cast<int64_t>(22)},
        {std::string("Sara"), static_cast<int64_t>(21)}};

    auto result11 = interpreter.execute("SELECT name, max(age + id) FROM test_table GROUP BY name ORDER BY name");
    assertRowsEqual(rows11, result11);
}
