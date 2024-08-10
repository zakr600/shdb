#pragma once

#include <cstdint>
#include "btree_page.h"
#include "database.h"
#include "row.h"
#include "table.h"
#include "index.h"
#include "comparator.h"

namespace shdb
{

bool operator<(const Null& lhs, const Null & rhs);
bool operator<=(const Null& lhs, const Null & rhs);
bool operator>(const Null& lhs, const Null & rhs);
bool operator>=(const Null& lhs, const Null & rhs);
bool operator!=(const Null& lhs, const Null & rhs);

class BTree;
using BTreePtr = std::shared_ptr<BTree>;

class BTreeIndexTable
{
public:
    BTreeIndexTable() = default;

    explicit BTreeIndexTable(std::shared_ptr<IIndexTable> table_) : table(std::move(table_)) { }

    void setIndexTable(std::shared_ptr<IIndexTable> index_table) { table = std::move(index_table); }

    PageIndex getPageCount() { return table->getPageCount(); }

    std::pair<BTreeMetadataPage, RowIndex> allocateMetadataPage()
    {
        PageIndex page_index = allocatePage();
        auto raw_page = getPage(page_index);
        raw_page->setPageType(BTreePageType::metadata);

        return {BTreeMetadataPage(raw_page), page_index};
    }

    BTreeMetadataPage getMetadataPage(PageIndex page_index) { return BTreeMetadataPage(getPage(page_index)); }

    std::pair<BTreeLeafPage, RowIndex> allocateLeafPage()
    {
        PageIndex page_index = allocatePage();
        auto raw_page = getPage(page_index);
        raw_page->setPageType(BTreePageType::leaf);

        return {BTreeLeafPage(raw_page), page_index};
    }

    BTreeLeafPage getLeafPage(PageIndex page_index) { return BTreeLeafPage(getPage(page_index)); }

    std::pair<BTreeInternalPage, RowIndex> allocateInternalPage()
    {
        PageIndex page_index = allocatePage();
        auto raw_page = getPage(page_index);
        raw_page->setPageType(BTreePageType::internal);

        return {BTreeInternalPage(raw_page), page_index};
    }

    BTreeInternalPage getInternalPage(PageIndex page_index) { return BTreeInternalPage(getPage(page_index)); }

    inline BTreePagePtr getPage(PageIndex page_index) { return std::static_pointer_cast<BTreePage>(table->getPage(page_index)); }

private:
    inline PageIndex allocatePage() { return table->allocatePage(); }

    std::shared_ptr<IIndexTable> table;
};

class BTree : public IIndex
{
public:
    static BTreePtr createIndex(const IndexMetadata & index_metadata, Store & store);

    static BTreePtr createIndex(const IndexMetadata & index_metadata, size_t page_max_keys_size, Store & store);

    static void removeIndex(const std::string & name_, Store & store);

    static void removeIndexIfExists(const std::string & name_, Store & store);

    void insert(const IndexKey & index_key, const RowId & row_id) override;

    bool remove(const IndexKey & index_key, const RowId & row_id) override;

    void lookup(const IndexKey & index_key, std::vector<RowId> & result) override;

    std::unique_ptr<IIndexIterator> read() override;

    std::unique_ptr<IIndexIterator> read(const KeyConditions & predicates) override;

    size_t getMaxPageSize() const { return max_page_size; }

    const BTreeIndexTable & getIndexTable() const { return index_table; }

    BTreeIndexTable & getIndexTable() { return index_table; }

    void dump(std::ostream & stream);

    static constexpr PageIndex MetadataPageIndex = 0;

private:
    BTreeLeafPage lookupLeafPage(const IndexKey & index_key);

    BTreeLeafPage lookupLeftmostLeafPage();

    BTree(const IndexMetadata & metadata_, Store & store, std::optional<size_t> page_max_keys_size);

    size_t max_page_size = 0;

    BTreeIndexTable index_table;

    BTreeMetadataPage metadata_page;
};

class SimpleIterator : public IIndexIterator {
public:
    SimpleIterator(BTreeIndexTable *index_table, PageIndex root_page_index) {
        this->indexTable = index_table;
        stack.push_back(root_page_index);
        cnt.push_back(0);
    }

    ~SimpleIterator() override = default;

    std::optional<std::pair<IndexKey, RowId>> nextRow() override {
        while (!stack.empty()) {
            auto page = indexTable->getPage(stack.back());
            if (page->isLeafPage()) {
                auto node = shdb::BTreeLeafPage(page);
                if (node.getSize() == cnt.back()) {
                    stack.pop_back();
                    cnt.pop_back();
                    if (!stack.empty()) {
                        ++cnt.back();
                    }
                    continue;
                }
                int index = cnt.back();
                ++cnt.back();
                auto key = node.getKey(index);
                auto value = node.getValue(index);
                return std::optional<std::pair<IndexKey, RowId>>({key, value});
            } else if (page->isInternalPage()) {
                auto node = shdb::BTreeInternalPage(page);
                if (node.getSize() == cnt.back()) {
                    stack.pop_back();
                    cnt.pop_back();
                    if (!stack.empty()) {
                        ++cnt.back();
                    }
                    continue;
                }
                int pos = cnt.back();
                auto pageId = node.getValue(pos);
                stack.push_back(pageId);
                cnt.push_back(0);
            }
        }
        return std::optional<std::pair<IndexKey, RowId>>();
    }

private:
    BTreeIndexTable *indexTable;
    std::vector<PageIndex> stack;
    std::vector<int> cnt;
};

class SmartIterator : public IIndexIterator {
public:
    SmartIterator(BTreeIndexTable *index_table, PageIndex root_page_index, const std::shared_ptr<Schema> & schema, const KeyConditions & predicates):
        iterator(index_table, root_page_index), predicates(predicates), schema(schema) {}

    ~SmartIterator() override = default;

    std::optional<std::pair<IndexKey, RowId>> nextRow() override {
        std::optional<std::pair<IndexKey, RowId>> item;
        while (true) {
            item = iterator.nextRow();
            if (!item.has_value()) {
                return item;
            }
            bool ok = true;
            for (const auto& condition : predicates) {
                for (int i = 0; i < schema->size(); ++i) {
                    if ((*schema)[i] == condition.column) {
                        auto rowVal = item.value().first[i];
                        auto cVal = condition.value;
                        if ((condition.comparator == IndexComparator::equal && !(rowVal == cVal)) ||
                            (condition.comparator == IndexComparator::less && !(rowVal < cVal)) ||
                            (condition.comparator == IndexComparator::lessOrEqual && !(rowVal <= cVal)) ||
                            (condition.comparator == IndexComparator::greater && !(rowVal > cVal)) ||
                            (condition.comparator == IndexComparator::greaterOrEqual && !(rowVal >= cVal)) ||
                            (condition.comparator == IndexComparator::notEqual && (rowVal == cVal))) {
                            ok = false;
                        }
                    }
                }
            }
            if (ok) {
                return item;
            }
        }
        return item;
    }

private:
    SimpleIterator iterator;
    const std::shared_ptr<Schema> & schema;
    const KeyConditions & predicates;
};
}
