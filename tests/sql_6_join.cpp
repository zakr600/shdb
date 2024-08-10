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

    interpreter.execute("DROP TABLE test_orders");
    interpreter.execute("CREATE TABLE test_orders (id int64, order string, price int64)");
    interpreter.execute("INSERT test_orders VALUES (0, \"pizza\", 99)");
    interpreter.execute("INSERT test_orders VALUES (0, \"cola\", 49)");
    interpreter.execute("INSERT test_orders VALUES (2, \"burger\", 599)");
}

void assertRowsEqual(const std::vector<shdb::Row> & rows, const shdb::RowSet & rowset)
{
    ASSERT_EQ(rows.size(), rowset.getRows().size());
    for (size_t index = 0; index < rows.size(); ++index)
        ASSERT_EQ(rows[index], rowset.getRows()[index]);
}

}

TEST(SQL, Join)
{
    auto db = createDatabase(1);
    auto interpreter = shdb::Interpreter(db);
    populate(interpreter);

    auto rows1 = std::vector<shdb::Row>{
        {static_cast<int64_t>(0), static_cast<int64_t>(20), std::string("Ann"), true, std::string("pizza"), static_cast<int64_t>(99)},
        {static_cast<int64_t>(0), static_cast<int64_t>(20), std::string("Ann"), true, std::string("cola"), static_cast<int64_t>(49)},
        {static_cast<int64_t>(2), static_cast<int64_t>(19), std::string("Sara"), true, std::string("burger"), static_cast<int64_t>(599)}};

    auto result1 = interpreter.execute("SELECT * FROM test_table, test_orders");
    assertRowsEqual(rows1, result1);

    auto rows2 = std::vector<shdb::Row>{
        {static_cast<int64_t>(0), static_cast<int64_t>(20), std::string("Ann"), true, std::string("pizza"), static_cast<int64_t>(99)},
        {static_cast<int64_t>(2), static_cast<int64_t>(19), std::string("Sara"), true, std::string("burger"), static_cast<int64_t>(599)}};

    auto result2 = interpreter.execute("SELECT * FROM test_table, test_orders WHERE price > 50");
    assertRowsEqual(rows2, result2);

    auto rows3 = std::vector<shdb::Row>{
        {std::string("Ann"), static_cast<int64_t>(99)},
        {std::string("Ann"), static_cast<int64_t>(49)},
        {std::string("Sara"), static_cast<int64_t>(599)}};

    auto result3 = interpreter.execute("SELECT name, price FROM test_table, test_orders");
    assertRowsEqual(rows3, result3);

    auto rows4 = std::vector<shdb::Row>{{std::string("Ann"), static_cast<int64_t>(99)}, {std::string("Sara"), static_cast<int64_t>(599)}};

    auto result4 = interpreter.execute("SELECT name, price FROM test_table, test_orders WHERE price > 50");
    assertRowsEqual(rows4, result4);

    auto rows5 = std::vector<shdb::Row>{{std::string("Sara"), static_cast<int64_t>(599)}, {std::string("Ann"), static_cast<int64_t>(99)}};

    auto result5 = interpreter.execute("SELECT name, price FROM test_table, test_orders WHERE price > 50 ORDER BY name DESC");
    assertRowsEqual(rows5, result5);

    auto rows6 = std::vector<shdb::Row>{
        {std::string("Ann"), static_cast<int64_t>(49)},
        {std::string("Ann"), static_cast<int64_t>(99)},
        {std::string("Sara"), static_cast<int64_t>(599)}};

    auto result6 = interpreter.execute("SELECT name, price FROM test_table, test_orders ORDER BY price");
    assertRowsEqual(rows6, result6);

    auto rows7 = std::vector<shdb::Row>{
        {static_cast<int64_t>(2), static_cast<int64_t>(19), std::string("Sara"), true, std::string("burger"), static_cast<int64_t>(599)},
        {static_cast<int64_t>(0), static_cast<int64_t>(20), std::string("Ann"), true, std::string("cola"), static_cast<int64_t>(49)},
        {static_cast<int64_t>(0), static_cast<int64_t>(20), std::string("Ann"), true, std::string("pizza"), static_cast<int64_t>(99)}};

    auto result7 = interpreter.execute("SELECT * FROM test_table, test_orders ORDER BY name DESC, price");
    assertRowsEqual(rows7, result7);
}
