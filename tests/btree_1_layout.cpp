#include <random>

#include <unistd.h>

#include <sys/types.h>
#include <sys/wait.h>

#include <gtest/gtest.h>

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

template <shdb::BTreePageType PageType>
class BTreePageHolder
{
public:
    static_assert(PageType == shdb::BTreePageType::internal || PageType == shdb::BTreePageType::leaf);

    explicit BTreePageHolder(std::shared_ptr<shdb::Schema> key_schema_)
        : key_schema(std::move(key_schema_))
        , key_marshal(std::make_shared<shdb::Marshal>(key_schema))
        , frame_memory(std::make_unique<uint8_t[]>(shdb::PageSize))
        , frame(std::make_shared<shdb::Frame>(nullptr, 0, frame_memory.get()))
        , max_keys_size(calculateMaxKeySize(key_marshal->getFixedRowSpace()))
        , btree_page(std::make_shared<shdb::BTreePage>(frame, key_marshal, key_marshal->getFixedRowSpace(), max_keys_size))
    {
        btree_page->setPageType(PageType);
    }

    size_t getMaxKeysSize() const { return max_keys_size; }

    const std::shared_ptr<shdb::BTreePage> & getBTreePage() const { return btree_page; }

    std::shared_ptr<shdb::BTreePage> & getBTreePage() { return btree_page; }

private:
    static size_t calculateMaxKeySize(size_t key_size)
    {
        if constexpr (PageType == shdb::BTreePageType::leaf)
            return shdb::BTreeLeafPage::calculateMaxKeysSize(key_size);
        else if constexpr (PageType == shdb::BTreePageType::internal)
            return shdb::BTreeInternalPage::calculateMaxKeysSize(key_size);
    }

    std::shared_ptr<shdb::Schema> key_schema;
    std::shared_ptr<shdb::Marshal> key_marshal;
    std::unique_ptr<uint8_t[]> frame_memory;
    std::shared_ptr<shdb::Frame> frame;
    size_t max_keys_size = 0;
    std::shared_ptr<shdb::BTreePage> btree_page;
};

}

TEST(BTree, InternalPageLayoutSimpleKey)
{
    static constexpr size_t iterations = 5;

    {
        for (size_t iteration = 0; iteration < iterations; ++iteration)
        {
            auto schema = shdb::createSchema({shdb::Type::uint64});
            BTreePageHolder<shdb::BTreePageType::internal> page_holder(schema);

            size_t insert_keys_size = 10;
            ASSERT_TRUE(insert_keys_size <= page_holder.getMaxKeysSize());

            auto btree_internal_page = shdb::BTreeInternalPage(page_holder.getBTreePage());

            std::vector<std::pair<shdb::Row, shdb::PageIndex>> entries;

            for (size_t i = 0; i < insert_keys_size; ++i)
                entries.emplace_back(shdb::Row{shdb::Value{static_cast<uint64_t>(i)}}, static_cast<shdb::PageIndex>(i));

            btree_internal_page.insertFirstEntry(entries[0].second);
            ASSERT_EQ(btree_internal_page.getSize(), 1);

            for (size_t i = 1; i < insert_keys_size; ++i)
            {
                const auto & entry = entries[i];
                btree_internal_page.insertEntry(i, entry.first, entry.second);
                ASSERT_EQ(btree_internal_page.getSize(), i + 1);
            }

            for (const auto & entry : entries)
            {
                if (entry != entries[0])
                {
                    auto result = btree_internal_page.lookup(entry.first);
                    ASSERT_EQ(btree_internal_page.lookup(entry.first), entry.second);
                }
            }
        }
    }
    {
        for (size_t iteration = 0; iteration < iterations; ++iteration)
        {
            auto schema = shdb::createSchema({shdb::Type::uint64});
            BTreePageHolder<shdb::BTreePageType::internal> page_holder(schema);

            size_t insert_keys_size = page_holder.getMaxKeysSize();

            auto btree_internal_page = shdb::BTreeInternalPage(page_holder.getBTreePage());

            std::vector<std::pair<shdb::Row, shdb::PageIndex>> entries;

            for (size_t i = 0; i < insert_keys_size; ++i)
                entries.emplace_back(shdb::Row{shdb::Value{static_cast<uint64_t>(i)}}, static_cast<shdb::PageIndex>(i));

            btree_internal_page.insertFirstEntry(entries[0].second);
            ASSERT_EQ(btree_internal_page.getSize(), 1);

            for (size_t i = 1; i < insert_keys_size; ++i)
            {
                const auto & entry = entries[i];
                btree_internal_page.insertEntry(i, entry.first, entry.second);
                ASSERT_EQ(btree_internal_page.getSize(), i + 1);
            }

            for (auto & entry : entries)
                if (entry != entries[0])
                {
                    ASSERT_EQ(btree_internal_page.lookup(entry.first), entry.second);
                }
        }
    }
}

TEST(BTree, InternalPageLayoutComplexKey)
{
    static constexpr size_t iterations = 5;

    {
        for (size_t iteration = 0; iteration < iterations; ++iteration)
        {
            auto schema = shdb::createSchema({shdb::Type::uint64, {shdb::Type::varchar, 16}});
            BTreePageHolder<shdb::BTreePageType::internal> page_holder(schema);

            size_t insert_keys_size = 10;
            ASSERT_TRUE(insert_keys_size <= page_holder.getMaxKeysSize());

            auto btree_internal_page = shdb::BTreeInternalPage(page_holder.getBTreePage());

            std::vector<std::pair<shdb::Row, shdb::PageIndex>> entries;

            for (size_t i = 0; i < insert_keys_size; ++i)
            {
                auto key = shdb::Row{shdb::Value{static_cast<uint64_t>(i)}, shdb::Value{"Key_" + std::to_string(i)}};
                auto value = static_cast<shdb::PageIndex>(i);
                entries.emplace_back(std::move(key), std::move(value));
            }

            btree_internal_page.insertFirstEntry(entries[0].second);
            ASSERT_EQ(btree_internal_page.getSize(), 1);

            for (size_t i = 1; i < insert_keys_size; ++i)
            {
                const auto & entry = entries[i];
                btree_internal_page.insertEntry(i, entry.first, entry.second);
                ASSERT_EQ(btree_internal_page.getSize(), i + 1);
            }

            for (const auto & entry : entries)
            {
                auto result = btree_internal_page.lookup(entry.first);
                ASSERT_EQ(btree_internal_page.lookup(entry.first), entry.second);
            }
        }
    }
    {
        for (size_t iteration = 0; iteration < iterations; ++iteration)
        {
            auto schema = shdb::createSchema({shdb::Type::uint64, {shdb::Type::varchar, 16}});
            BTreePageHolder<shdb::BTreePageType::internal> page_holder(schema);

            size_t insert_keys_size = page_holder.getMaxKeysSize();

            auto btree_internal_page = shdb::BTreeInternalPage(page_holder.getBTreePage());

            std::vector<std::pair<shdb::Row, shdb::PageIndex>> entries;

            for (size_t i = 0; i < insert_keys_size; ++i)
            {
                auto key = shdb::Row{shdb::Value{static_cast<uint64_t>(i)}, shdb::Value{"Key_" + std::to_string(i)}};
                auto value = static_cast<shdb::PageIndex>(i);
                entries.emplace_back(std::move(key), std::move(value));
            }

            btree_internal_page.insertFirstEntry(entries[0].second);
            ASSERT_EQ(btree_internal_page.getSize(), 1);

            for (size_t i = 1; i < insert_keys_size; ++i)
            {
                const auto & entry = entries[i];
                btree_internal_page.insertEntry(i, entry.first, entry.second);
                ASSERT_EQ(btree_internal_page.getSize(), i + 1);
            }

            for (auto & entry : entries)
                ASSERT_EQ(btree_internal_page.lookup(entry.first), entry.second);
        }
    }
}

TEST(BTree, LeafPageLayoutSimpleKey)
{
    static constexpr size_t iterations = 5;

    {
        for (size_t iteration = 0; iteration < iterations; ++iteration)
        {
            auto schema = shdb::createSchema({shdb::Type::uint64});
            BTreePageHolder<shdb::BTreePageType::leaf> page_holder(schema);

            size_t insert_keys_size = 10;
            ASSERT_TRUE(insert_keys_size <= page_holder.getMaxKeysSize());

            auto btree_leaf_page = shdb::BTreeLeafPage(page_holder.getBTreePage());

            std::vector<std::pair<shdb::Row, shdb::RowId>> entries;

            for (size_t i = 0; i < insert_keys_size; ++i)
                entries.emplace_back(shdb::Row{shdb::Value{static_cast<uint64_t>(i)}}, shdb::RowId{0, static_cast<shdb::RowIndex>(i)});

            randomShuffle(entries.begin(), entries.end());

            for (size_t i = 0; i < insert_keys_size; ++i)
            {
                const auto & entry = entries[i];
                ASSERT_TRUE(btree_leaf_page.insert(entry.first, entry.second));
                ASSERT_EQ(btree_leaf_page.getSize(), i + 1);
            }

            for (auto & entry : entries)
                ASSERT_EQ(btree_leaf_page.lookup(entry.first), entry.second);

            for (auto & entry : entries)
            {
                ASSERT_TRUE(btree_leaf_page.remove(entry.first));
                ASSERT_EQ(btree_leaf_page.lookup(entry.first), std::nullopt);
            }

            ASSERT_EQ(btree_leaf_page.getSize(), 0);
        }
    }
    {
        for (size_t iteration = 0; iteration < iterations; ++iteration)
        {
            auto schema = shdb::createSchema({shdb::Type::uint64});
            BTreePageHolder<shdb::BTreePageType::leaf> page_holder(schema);

            size_t insert_keys_size = page_holder.getMaxKeysSize();

            auto btree_leaf_page = shdb::BTreeLeafPage(page_holder.getBTreePage());

            std::vector<std::pair<shdb::Row, shdb::RowId>> entries;

            for (size_t i = 0; i < insert_keys_size; ++i)
                entries.emplace_back(shdb::Row{shdb::Value{static_cast<uint64_t>(i)}}, shdb::RowId{0, static_cast<shdb::RowIndex>(i)});

            randomShuffle(entries.begin(), entries.end());

            for (size_t i = 0; i < insert_keys_size; ++i)
            {
                const auto & entry = entries[i];
                ASSERT_TRUE(btree_leaf_page.insert(entry.first, entry.second));
                ASSERT_EQ(btree_leaf_page.getSize(), i + 1);
            }

            for (auto & entry : entries)
                ASSERT_EQ(btree_leaf_page.lookup(entry.first), entry.second);

            for (auto & entry : entries)
            {
                ASSERT_TRUE(btree_leaf_page.remove(entry.first));
                ASSERT_EQ(btree_leaf_page.lookup(entry.first), std::nullopt);
            }

            ASSERT_EQ(btree_leaf_page.getSize(), 0);
        }
    }
    {
        /// Insert duplicate key

        auto schema = shdb::createSchema({shdb::Type::uint64});
        BTreePageHolder<shdb::BTreePageType::leaf> page_holder(schema);

        auto btree_leaf_page = shdb::BTreeLeafPage(page_holder.getBTreePage());

        auto key = shdb::Row{shdb::Value{static_cast<uint64_t>(0)}};
        auto value = shdb::RowId{0, static_cast<shdb::RowIndex>(0)};

        btree_leaf_page.insert(key, value);
        EXPECT_ANY_THROW(btree_leaf_page.insert(key, value));
    }
}

TEST(BTree, LeafPageLayoutComplexKey)
{
    static constexpr size_t iterations = 5;

    {
        for (size_t iteration = 0; iteration < iterations; ++iteration)
        {
            auto schema = shdb::createSchema({shdb::Type::uint64, {shdb::Type::varchar, 16}});
            BTreePageHolder<shdb::BTreePageType::leaf> page_holder(schema);

            size_t insert_keys_size = 10;
            ASSERT_TRUE(insert_keys_size <= page_holder.getMaxKeysSize());

            auto btree_leaf_page = shdb::BTreeLeafPage(page_holder.getBTreePage());

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
                ASSERT_TRUE(btree_leaf_page.insert(entry.first, entry.second));
                ASSERT_EQ(btree_leaf_page.getSize(), i + 1);
            }

            for (auto & entry : entries)
                ASSERT_EQ(btree_leaf_page.lookup(entry.first), entry.second);

            for (auto & entry : entries)
            {
                ASSERT_TRUE(btree_leaf_page.remove(entry.first));
                ASSERT_EQ(btree_leaf_page.lookup(entry.first), std::nullopt);
            }

            ASSERT_EQ(btree_leaf_page.getSize(), 0);
        }
    }
    {
        for (size_t iteration = 0; iteration < iterations; ++iteration)
        {
            auto schema = shdb::createSchema({shdb::Type::uint64, {shdb::Type::varchar, 16}});
            BTreePageHolder<shdb::BTreePageType::leaf> page_holder(schema);

            size_t insert_keys_size = page_holder.getMaxKeysSize();

            auto btree_leaf_page = shdb::BTreeLeafPage(page_holder.getBTreePage());

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
                ASSERT_TRUE(btree_leaf_page.insert(entry.first, entry.second));
                ASSERT_EQ(btree_leaf_page.getSize(), i + 1);
            }

            for (auto & entry : entries)
                ASSERT_EQ(btree_leaf_page.lookup(entry.first), entry.second);

            for (auto & entry : entries)
            {
                ASSERT_TRUE(btree_leaf_page.remove(entry.first));
                ASSERT_EQ(btree_leaf_page.lookup(entry.first), std::nullopt);
            }

            ASSERT_EQ(btree_leaf_page.getSize(), 0);
        }
    }
    {
        /// Insert duplicate key

        auto schema = shdb::createSchema({shdb::Type::uint64, {shdb::Type::varchar, 16}});
        BTreePageHolder<shdb::BTreePageType::leaf> page_holder(schema);

        auto btree_leaf_page = shdb::BTreeLeafPage(page_holder.getBTreePage());

        auto key = shdb::Row{shdb::Value{static_cast<uint64_t>(0)}, shdb::Value{"Key_" + std::to_string(0)}};
        auto value = shdb::RowId{0, static_cast<shdb::RowIndex>(0)};

        btree_leaf_page.insert(key, value);
        EXPECT_ANY_THROW(btree_leaf_page.insert(key, value));
    }
}
