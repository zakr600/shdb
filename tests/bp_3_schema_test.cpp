#include <cassert>
#include <iostream>
#include <sstream>

#include <gtest/gtest.h>

#include "db.h"

namespace
{

auto fixed_schema = std::make_shared<shdb::Schema>(shdb::Schema{
    {"id", shdb::Type::uint64}, {"name", shdb::Type::varchar, 1024}, {"age", shdb::Type::uint64}, {"graduated", shdb::Type::boolean}});
auto flexible_schema = std::make_shared<shdb::Schema>(shdb::Schema{
    {"id", shdb::Type::uint64}, {"name", shdb::Type::string}, {"age", shdb::Type::uint64}, {"graduated", shdb::Type::boolean}});

std::shared_ptr<shdb::Database> createDatabase(int frame_count)
{
    auto db = shdb::connect("./mydb", frame_count);
    if (db->checkTableExists("test_table"))
        db->dropTable("test_table");
    db->createTable("test_table", fixed_schema);
    return db;
}

}

void testDelete(std::shared_ptr<shdb::Schema> schema)
{
    shdb::PageIndex pool_size = 5;
    auto db = createDatabase(pool_size);
    auto table = db->getTable("test_table", schema);

    std::vector<std::pair<shdb::RowId, shdb::Row>> rows;
    shdb::PageIndex page_count = 0;
    uint64_t row_count = 0;

    while (page_count < 2 * pool_size)
    {
        std::stringstream stream;
        stream << "clone" << row_count;
        auto row = shdb::Row{row_count, stream.str(), 20UL + row_count % 10, row_count % 10 > 5};
        auto row_id = table->insertRow(row);
        rows.emplace_back(row_id, std::move(row));
        page_count = std::max(page_count, row_id.page_index + 1);
        ++row_count;
    }

    auto validate = [&](auto rows)
    {
        for (auto & [row_id, row] : rows)
        {
            auto found_row = table->getRow(row_id);
            ASSERT_TRUE(found_row == row);
        }

        size_t index = 0;
        auto scan = shdb::Scan(table);
        for (auto it = scan.begin(), end = scan.end(); it != end; ++it)
        {
            auto row = it.getRow();
            if (!row.empty())
            {
                ASSERT_TRUE(row == rows[index].second);
                ASSERT_TRUE(it.getRowId() == rows[index].first);
                ++index;
            }
        }
        ASSERT_TRUE(index == rows.size());
    };

    validate(rows);

    std::vector<std::pair<shdb::RowId, shdb::Row>> remained;
    for (auto & [row_id, row] : rows)
    {
        if (std::get<uint64_t>(row[0]) % 3 == 1)
        {
            auto deleted_row = table->getRow(row_id);
            ASSERT_TRUE(deleted_row == row);
            table->deleteRow(row_id);
            deleted_row = table->getRow(row_id);
            ASSERT_TRUE(deleted_row.empty());
        }
        else
        {
            remained.emplace_back(row_id, row);
        }
    }
    ASSERT_TRUE(remained.size() <= rows.size() * 2 / 3 + 1);

    validate(remained);

    for (auto & [row_id, row] : rows)
    {
        if (std::get<uint64_t>(row[0]) % 3 == 1)
        {
            auto deleted_row = table->getRow(row_id);
            assert(deleted_row.empty());
        }
    }
}

TEST(BufferPool, Delete)
{
    std::cout << "Validate deletes with fixed schema" << std::endl;
    testDelete(fixed_schema);
    std::cout << "Validate deletes with flexible schema" << std::endl;
    testDelete(flexible_schema);
    std::cout << "Test flexible schema passed" << std::endl;
}
