#include <random>

#include <unistd.h>

#include <sys/types.h>
#include <sys/wait.h>

#include <gtest/gtest.h>

#include "btree.h"
#include "btree_page.h"
#include "db.h"

namespace
{

template <typename It>
void randomShuffle(It begin, It end)
{
    std::random_device random_device;
    std::mt19937 generator(random_device());
    std::shuffle(begin, end, generator);
}

std::shared_ptr<shdb::Database> createDatabase(int frame_count)
{
    auto db = shdb::connect("./mydb", frame_count);
    return db;
}

void validateBTree(shdb::BTree & btree, const std::vector<std::pair<shdb::Row, shdb::RowId>> & inserted_entries)
{
    auto inserted_entries_copy = inserted_entries;
    std::sort(
        inserted_entries_copy.begin(),
        inserted_entries_copy.end(),
        [](auto & lhs, auto & rhs) { return shdb::compareRows(lhs.first, rhs.first) < 0; });

    auto & index_table = btree.getIndexTable();
    auto page_count = index_table.getPageCount();
    ASSERT_GE(page_count, 1);

    auto metadata_page = index_table.getMetadataPage(shdb::BTree::MetadataPageIndex);
    auto root_page_index = metadata_page.getRootPageIndex();
    auto current_page = index_table.getPage(root_page_index);
    while (!current_page->isLeafPage())
    {
        ASSERT_TRUE(current_page->isInternalPage());
        shdb::BTreeInternalPage internal_page(current_page);
        current_page = index_table.getPage(internal_page.getValue(0));
    }

    std::vector<std::pair<shdb::Row, shdb::RowId>> entries;

    shdb::BTreeLeafPage leaf_page(current_page);
    size_t entry_offset = 0;

    while (true)
    {
        if (entry_offset == leaf_page.getSize())
        {
            auto next_page_index = leaf_page.getNextPageIndex();
            if (next_page_index == shdb::InvalidPageIndex)
                break;

            leaf_page = index_table.getLeafPage(leaf_page.getNextPageIndex());
            entry_offset = 0;
            continue;
        }

        auto key = leaf_page.getKey(entry_offset);
        auto value = leaf_page.getValue(entry_offset);
        entries.emplace_back(std::move(key), std::move(value));
        ++entry_offset;
    }

    ASSERT_EQ(entries, inserted_entries_copy);
}
}

void testBTreeSimpleKey(std::function<size_t(size_t)> max_page_size_to_insert_keys_size, std::optional<size_t> max_keys_size = {})
{
    auto db = createDatabase(1024);
    auto schema = shdb::createSchema({shdb::Type::uint64});

    shdb::IndexMetadata metadata("test_index", std::move(schema));
    shdb::BTree::removeIndexIfExists(metadata.getIndexName(), *db->getStore());

    std::shared_ptr<shdb::BTree> btree_index;

    if (max_keys_size)
        btree_index = shdb::BTree::createIndex(metadata, *max_keys_size, *db->getStore());
    else
        btree_index = shdb::BTree::createIndex(metadata, *db->getStore());

    size_t insert_keys_size = max_page_size_to_insert_keys_size(btree_index->getMaxPageSize());

    std::vector<std::pair<shdb::Row, shdb::RowId>> entries;

    for (size_t i = 0; i < insert_keys_size; ++i)
        entries.emplace_back(shdb::Row{shdb::Value{static_cast<uint64_t>(i)}}, shdb::RowId{0, static_cast<shdb::RowIndex>(i)});

    randomShuffle(entries.begin(), entries.end());

    for (size_t i = 0; i < insert_keys_size; ++i)
    {
        const auto & entry = entries[i];
        btree_index->insert(entry.first, entry.second);
        ASSERT_EQ(btree_index->lookupUniqueKey(entry.first), entry.second);
    }

    validateBTree(*btree_index, entries);

    for (auto & entry : entries)
        ASSERT_EQ(btree_index->lookupUniqueKey(entry.first), entry.second);

    for (size_t i = 0; i < insert_keys_size; ++i)
    {
        const auto & entry = entries[i];
        btree_index->remove(entry.first, entry.second);
        ASSERT_EQ(btree_index->lookupUniqueKey(entry.first), std::optional<shdb::RowId>{});
    }

    for (size_t i = 0; i < insert_keys_size; ++i)
    {
        const auto & entry = entries[i];
        btree_index->insert(entry.first, entry.second);
        ASSERT_EQ(btree_index->lookupUniqueKey(entry.first), entry.second);
    }

    validateBTree(*btree_index, entries);

    for (size_t i = 0; i < insert_keys_size; ++i)
    {
        const auto & entry = entries[i];
        btree_index->remove(entry.first, entry.second);
        ASSERT_EQ(btree_index->lookupUniqueKey(entry.first), std::optional<shdb::RowId>{});
    }

    shdb::BTree::removeIndex(metadata.getIndexName(), *db->getStore());
}

void testBTreeComplexKey(std::function<size_t(size_t)> max_page_size_to_insert_keys_size, std::optional<size_t> max_keys_size = {})
{
    auto db = createDatabase(1024);
    auto schema = shdb::createSchema({shdb::Type::uint64, {shdb::Type::varchar, 16}});

    shdb::IndexMetadata metadata("test_index", std::move(schema));
    shdb::BTree::removeIndexIfExists(metadata.getIndexName(), *db->getStore());

    std::shared_ptr<shdb::BTree> btree_index;

    if (max_keys_size)
        btree_index = shdb::BTree::createIndex(metadata, *max_keys_size, *db->getStore());
    else
        btree_index = shdb::BTree::createIndex(metadata, *db->getStore());

    size_t insert_keys_size = max_page_size_to_insert_keys_size(btree_index->getMaxPageSize());

    std::vector<std::pair<shdb::Row, shdb::RowId>> entries;

    for (size_t i = 0; i < insert_keys_size; ++i)
    {
        auto key = shdb::Row{shdb::Value{static_cast<uint64_t>(i)}, shdb::Value{"Key_" + std::to_string(i)}};
        auto value = shdb::RowId{0, static_cast<shdb::RowIndex>(i)};
        entries.emplace_back(std::move(key), std::move(value));
    }

    randomShuffle(entries.begin(), entries.end());

    for (size_t i = 0; i < insert_keys_size; ++i)
    {
        const auto & entry = entries[i];
        btree_index->insert(entry.first, entry.second);
        ASSERT_EQ(btree_index->lookupUniqueKey(entry.first), entry.second);
    }

    validateBTree(*btree_index, entries);

    for (auto & entry : entries)
        ASSERT_EQ(btree_index->lookupUniqueKey(entry.first), entry.second);

    for (size_t i = 0; i < insert_keys_size; ++i)
    {
        const auto & entry = entries[i];
        btree_index->remove(entry.first, entry.second);
        ASSERT_EQ(btree_index->lookupUniqueKey(entry.first), std::optional<shdb::RowId>{});
    }

    for (size_t i = 0; i < insert_keys_size; ++i)
    {
        const auto & entry = entries[i];
        btree_index->insert(entry.first, entry.second);
        ASSERT_EQ(btree_index->lookupUniqueKey(entry.first), entry.second);
    }

    validateBTree(*btree_index, entries);

    for (size_t i = 0; i < insert_keys_size; ++i)
    {
        const auto & entry = entries[i];
        btree_index->remove(entry.first, entry.second);
        ASSERT_EQ(btree_index->lookupUniqueKey(entry.first), std::optional<shdb::RowId>{});
    }

    shdb::BTree::removeIndex(metadata.getIndexName(), *db->getStore());
}

TEST(BTree, BTreeRemoveSimpleKey)
{
    static size_t iterations = 5;

    {
        for (size_t i = 0; i < iterations; ++i)
            testBTreeSimpleKey([](size_t) { return 10; });
    }
    {
        for (size_t i = 0; i < iterations; ++i)
            testBTreeSimpleKey([](size_t max_page_size) { return max_page_size * 16; });
    }
    {
        for (size_t i = 0; i < iterations; ++i)
            testBTreeSimpleKey([](size_t max_page_size) { return max_page_size * 100; }, 16);
    }
}

TEST(BTree, BTreeRemoveComplexKey)
{
    static size_t iterations = 5;

    {
        for (size_t i = 0; i < iterations; ++i)
            testBTreeComplexKey([](size_t) { return 10; });
    }
    {
        for (size_t i = 0; i < iterations; ++i)
            testBTreeComplexKey([](size_t max_page_size) { return max_page_size * 16; });
    }
    {
        for (size_t i = 0; i < iterations; ++i)
            testBTreeComplexKey([](size_t max_page_size) { return max_page_size * 100; }, 16);
    }
}
