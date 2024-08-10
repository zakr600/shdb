#include "db.h"

#include <gtest/gtest.h>

#include <cassert>
#include <chrono>
#include <iostream>
#include <sstream>

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

TEST(BufferPool, Clock)
{
    shdb::PageIndex pool_size = 5;
    auto db = createDatabase(pool_size);
    auto table = db->getTable("test_table", fixed_schema);

    std::vector<std::pair<shdb::RowId, shdb::Row>> rows;
    shdb::PageIndex page_count = 0;
    uint64_t row_count = 0;

    while (page_count < pool_size + 1)
    {
        std::stringstream stream;
        stream << "clone" << row_count;
        auto row = shdb::Row{row_count, stream.str(), 20UL + row_count % 10, row_count % 10 > 5};
        auto row_id = table->insertRow(row);
        rows.emplace_back(row_id, std::move(row));
        page_count = std::max(page_count, row_id.page_index + 1);
        ++row_count;
    }

    auto statistics = db->getStatistics();
    int rounds = 100;
    auto run = [&](auto filter)
    {
        auto start_page_read = statistics->page_read;
        auto start = std::chrono::steady_clock::now();
        for (int round = 0; round < rounds; ++round)
        {
            for (auto & [row_id, row] : rows)
            {
                if (filter(row_id.page_index))
                {
                    auto found_row = table->getRow(row_id);
                    assert(found_row == row);
                }
            }
        }
        auto end = std::chrono::steady_clock::now();
        auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
        auto page_read = statistics->page_read - start_page_read;
        std::cout << "required " << page_read << " page fetches and finished in " << elapsed.count() << "ms" << std::endl;
        return page_read;
    };

    std::cout << "Lookups with working set fit into cache... ";
    auto page_read = run([&](auto page_index) { return page_index < pool_size; });
    ASSERT_TRUE(page_read <= static_cast<uint64_t>(pool_size));

    std::cout << "Lookups with working set fit into cache... ";
    page_read = run([&](auto page_index) { return page_index > 1; });
    assert(page_read <= static_cast<uint64_t>(pool_size));
    ASSERT_TRUE(page_read <= static_cast<uint64_t>(pool_size));

    std::cout << "Lookups with working set unfit into cache... ";
    page_read = run([&](auto) { return true; });
    ASSERT_TRUE(page_read >= static_cast<uint64_t>(pool_size));

    std::cout << "Test bufferpool passed" << std::endl;
}
