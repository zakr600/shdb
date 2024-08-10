#include <gtest/gtest.h>

#include "db.h"
#include "interpreter.h"

namespace
{

auto fixed_schema = std::make_shared<shdb::Schema>(shdb::Schema{
    {"id", shdb::Type::int64}, {"name", shdb::Type::varchar, 1024}, {"age", shdb::Type::int64}, {"graduated", shdb::Type::boolean}});

std::shared_ptr<shdb::Database> createDatabase(int frame_count)
{
    auto db = shdb::connect("./mydb", frame_count);
    if (db->checkTableExists("test_table"))
        db->dropTable("test_table");
    db->createTable("test_table", fixed_schema);
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

}

TEST(SQL, Select)
{
    auto db = createDatabase(1);
    auto interpreter = shdb::Interpreter(db);
    populate(interpreter);

    auto check = [&](const std::vector<shdb::Row> & rows, const shdb::RowSet & rowset)
    {
        ASSERT_EQ(rows.size(), rowset.getRows().size());

        for (size_t index = 0; index < rows.size(); ++index)
            ASSERT_EQ(rows[index], rowset.getRows()[index]);
    };

    auto rows1 = std::vector<shdb::Row>{
        {static_cast<int64_t>(0), static_cast<int64_t>(20), std::string("Ann"), true},
        {static_cast<int64_t>(1), static_cast<int64_t>(21), std::string("Bob"), false},
        {static_cast<int64_t>(2), static_cast<int64_t>(19), std::string("Sara"), true}};
    auto result1 = interpreter.execute("SELECT * FROM test_table");
    check(rows1, result1);

    auto rows2 = std::vector<shdb::Row>{
        {static_cast<int64_t>(0), static_cast<int64_t>(20), std::string("Ann"), true},
        {static_cast<int64_t>(2), static_cast<int64_t>(19), std::string("Sara"), true}};
    auto result2 = interpreter.execute("SELECT * FROM test_table WHERE age <= 20");
    check(rows2, result2);

    auto rows3 = std::vector<shdb::Row>{
        {static_cast<int64_t>(1), std::string("Ann"), true},
        {static_cast<int64_t>(2), std::string("Bob"), true},
        {static_cast<int64_t>(3), std::string("Sara"), false}};
    auto result3 = interpreter.execute("SELECT id+1, name, id < 2 FROM test_table");
    check(rows3, result3);

    auto rows4 = std::vector<shdb::Row>{{std::string("Bob"), static_cast<int64_t>(2)}, {std::string("Sara"), static_cast<int64_t>(4)}};
    auto result4 = interpreter.execute("SELECT name, id*2 FROM test_table WHERE id > 0");
    check(rows4, result4);
}
