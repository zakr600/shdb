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
    return db;
}

void populate(shdb::Interpreter & interpreter)
{
    interpreter.execute("DROP TABLE test_table");
    interpreter.execute("CREATE TABLE test_table (id int64, age int64, name string, girl boolean)");
    interpreter.execute("INSERT test_table VALUES (0, 10+10, \"Ann\", 1>0)");
    interpreter.execute("INSERT test_table VALUES (1, 10+10+1, \"Bob\", 1<0)");
    interpreter.execute("INSERT test_table VALUES (2, 10+9, \"Sara\", 1>0)");
    interpreter.execute("INSERT test_table VALUES (-2, 10+9, \"Sara\", 1>0)");

    ASSERT_ANY_THROW(interpreter.execute(R"(INSERT test_table VALUES ("2", 10+9, "Sara", 1>0))"));
    ASSERT_ANY_THROW(interpreter.execute(R"(INSERT test_table VALUES (1>0, 10+9, "Sara", 1>0))"));
}

}

TEST(SQL, Insert)
{
    auto db = createDatabase(1);
    auto interpreter = shdb::Interpreter(db);
    populate(interpreter);

    auto rows = std::vector<shdb::Row>{
        {static_cast<int64_t>(0), static_cast<int64_t>(20), std::string("Ann"), true},
        {static_cast<int64_t>(1), static_cast<int64_t>(21), std::string("Bob"), false},
        {static_cast<int64_t>(2), static_cast<int64_t>(19), std::string("Sara"), true},
        {static_cast<int64_t>(-2), static_cast<int64_t>(19), std::string("Sara"), true}};

    size_t index = 0;
    auto table = db->getTable("test_table");
    auto scan = shdb::Scan(table);
    for (auto it = scan.begin(), end = scan.end(); it != end; ++it)
    {
        auto row = it.getRow();
        if (!row.empty())
        {
            ASSERT_EQ(row, rows[index]);
            ++index;
        }
    }

    ASSERT_EQ(index, rows.size());
}
