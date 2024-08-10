#include <stdio.h>
#include <unistd.h>

#include <sys/types.h>
#include <sys/wait.h>

#include <cassert>
#include <iostream>
#include <sstream>

#include <gtest/gtest.h>

#include "db.h"

namespace
{

auto fixed_schema = std::make_shared<shdb::Schema>(shdb::Schema{
    {"id", shdb::Type::uint64}, {"name", shdb::Type::varchar, 1024}, {"age", shdb::Type::uint64}, {"graduated", shdb::Type::boolean}});

std::shared_ptr<shdb::Database> createDatabase(int frame_count)
{
    auto db = shdb::connect("./mydb", frame_count);
    if (db->checkTableExists("test_table"))
        db->dropTable("test_table");
    db->createTable("test_table", fixed_schema);
    return db;
}

}

void testMetadataPrepare()
{
    shdb::PageIndex pool_size = 5;
    auto db = createDatabase(pool_size);
    auto table = db->getTable("test_table", fixed_schema);

    std::vector<std::pair<shdb::RowId, shdb::Row>> rows;
    shdb::PageIndex page_count = 0;
    uint64_t row_count = 0;

    while (row_count < 10)
    {
        std::stringstream stream;
        stream << "clone" << row_count;
        auto row = shdb::Row{row_count, stream.str(), 20UL + row_count % 10, row_count % 10 > 5};
        auto row_id = table->insertRow(row);
        rows.emplace_back(row_id, std::move(row));
        page_count = std::max(page_count, row_id.page_index + 1);
        ++row_count;
    }
}

void testMetadataValidate()
{
    auto db = shdb::connect("./mydb", 1);
    auto table = db->getTable("test_table");
    uint64_t row_count = 0;
    for (auto row : shdb::Scan(table))
    {
        if (!row.empty())
        {
            std::stringstream stream;
            stream << "clone" << row_count;
            auto expected = shdb::Row{row_count, stream.str(), 20UL + row_count % 10, row_count % 10 > 5};
            std::cout << shdb::toString(row) << std::endl;
            ASSERT_TRUE(row == expected);
            ++row_count;
        }
    }
    ASSERT_TRUE(row_count == 10);
}

TEST(BufferPool, Metadata)
{
    testMetadataPrepare();
    std::cout << "Validating metadata in the same process:" << std::endl;
    testMetadataValidate();
}
