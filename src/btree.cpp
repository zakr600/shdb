#include "btree.h"
#include "btree_page.h"

#include <cassert>

namespace shdb
{

BTree::BTree(const IndexMetadata & metadata_, Store & store_, std::optional<size_t> page_max_keys_size)
    : IIndex(metadata_), metadata_page(nullptr)
{
    if (!page_max_keys_size)
    {
        size_t internal_page_max_keys_size = BTreeInternalPage::calculateMaxKeysSize(metadata.fixedKeySizeInBytes());
        size_t leaf_page_max_keys_size = BTreeLeafPage::calculateMaxKeysSize(metadata.fixedKeySizeInBytes());
        page_max_keys_size = std::min(internal_page_max_keys_size, leaf_page_max_keys_size);
    }

    max_page_size = *page_max_keys_size;
    auto page_provider = createBTreePageProvider(metadata.getKeyMarshal(), metadata.fixedKeySizeInBytes(), max_page_size);
    index_table.setIndexTable(store_.createOrOpenIndexTable(metadata.getIndexName(), page_provider));

    bool initial_index_creation = index_table.getPageCount() == 0;

    if (initial_index_creation)
    {
        auto [allocated_metadata_page, metadata_page_index] = index_table.allocateMetadataPage();
        assert(metadata_page_index == MetadataPageIndex);

        auto [root_page, root_page_index] = index_table.allocateLeafPage();

        root_page.setPreviousPageIndex(InvalidPageIndex);
        root_page.setNextPageIndex(InvalidPageIndex);

        metadata_page = std::move(allocated_metadata_page);
        metadata_page.setRootPageIndex(root_page_index);
        metadata_page.setMaxPageSize(max_page_size);
        metadata_page.setKeySizeInBytes(metadata.fixedKeySizeInBytes());
        return;
    }

    metadata_page = index_table.getMetadataPage(MetadataPageIndex);

    if (metadata.fixedKeySizeInBytes() != metadata_page.getKeySizeInBytes())
        throw std::runtime_error(
            "BTree index inconsistency. Expected " + std::to_string(metadata_page.getKeySizeInBytes()) + " key size in bytes. Actual "
            + std::to_string(metadata.fixedKeySizeInBytes()));

    if (max_page_size != metadata_page.getMaxPageSize())
        throw std::runtime_error(
            "BTree index inconsistency. Expected " + std::to_string(metadata_page.getMaxPageSize()) + " max page size. Actual "
            + std::to_string(max_page_size));
}

struct need_insert {
public:
    const IndexKey& key;
    const RowIndex left;
    const RowIndex right;

    need_insert(const IndexKey& key, RowIndex left, RowIndex right): key(key), left(left), right(right) {}
};

void BTree::insert(const IndexKey & index_key, const RowId & row_id)
{
    std::vector<std::pair<shdb::BTreeInternalPage, RowIndex>> path;
    int key = metadata_page.getRootPageIndex();
    auto page = index_table.getPage(key);
    while (!page->isLeafPage()) {
        auto node = shdb::BTreeInternalPage(page);
        path.push_back({node, key});
        key = node.lookup(index_key);
        page = index_table.getPage(key);
    }
    assert(page->isLeafPage());
    auto node = shdb::BTreeLeafPage(page);
    if (node.insert(index_key, row_id)) {
        return;
    }
    auto second_leaf_page = index_table.allocateLeafPage();
    Row divKey = node.getKey(node.getSize() / 2);
    node.split(second_leaf_page.first);
    auto prevNext = node.getNextPageIndex();
    node.setNextPageIndex(second_leaf_page.second);
    second_leaf_page.first.setPreviousPageIndex(key);
    second_leaf_page.first.setNextPageIndex(prevNext);
//    shdb::BTreeLeafPage(index_table.getPage(prevNext)).setPreviousPageIndex(key);
    if (compareRows(index_key, divKey) == -1) {
        node.insert(index_key, row_id);
    } else
    {
        second_leaf_page.first.insert(index_key, row_id);
    }
    auto t = need_insert(divKey, key, second_leaf_page.second);
    while (!path.empty()) {
        auto node2 = path.back();
        path.pop_back();
        auto pos = node2.first.lookupWithIndex(t.key);
        if (node2.first.insertEntry(pos.second + 1, t.key, t.right)) {
            node2.first.setValue(pos.second, t.left);
            return;
        }
        throw std::runtime_error("123");
    }
    if (path.empty()) {
        auto newAllocRoot = index_table.allocateInternalPage();
        auto newRoot = std::move(newAllocRoot.first);
        newRoot.insertFirstEntry(t.left);
        newRoot.setEntry(1, t.key, t.right);
        newRoot.setSize(2);
        metadata_page.setRootPageIndex(newAllocRoot.second);
    }
}

bool BTree::remove(const IndexKey & index_key, const RowId &)
{
    std::vector<std::pair<shdb::BTreeInternalPage, RowIndex>> path;
    std::vector<int> idxs;
    int key = metadata_page.getRootPageIndex();
    auto page = index_table.getPage(key);
    while (!page->isLeafPage()) {
        auto node = shdb::BTreeInternalPage(page);
        path.push_back({node, key});
        auto pr = node.lookupWithIndex(index_key);
        key = pr.first;
        idxs.push_back(pr.second);
        page = index_table.getPage(key);
    }
    auto node = shdb::BTreeLeafPage(page);
    node.remove(index_key);
    if (node.getSize() != 0) {
        return true;
    }
    return true;
    int toRemoveKey = key;
    while (!path.empty()) {
        auto node = path.back().first;
        path.pop_back();
        int sz = node.getSize();
        for (int j = idxs.back(); j + 1 < sz; ++j) {
            node.setEntry(j, node.getKey(j + 1), node.getValue(j + 1));
        }
        idxs.pop_back();
        node.setSize(sz - 1);
        if (node.getSize() > 0) {
            return true;
        } else if (node.getSize() == 0 && path.empty()) {
            index_table.getPage(metadata_page.getRootPageIndex())->setPageType(BTreePageType::leaf);
//            raw_page->setPageType(BTreePageType::leaf);
//            metadata_page.setRootPageIndex(index_table.allocateLeafPage().second);
        }
    }
    return true;
}

void BTree::lookup(const IndexKey & index_key, std::vector<RowId> & result)
{
//    std::ostream stream(nullptr);
//    stream.rdbuf(std::cout.rdbuf());
//    dump(stream);
//    return;

    auto page = index_table.getPage(metadata_page.getRootPageIndex());

    if (!page->isLeafPage() && !page->isInternalPage()) {

        throw std::runtime_error("123");
    }

//
    while (!page->isLeafPage()) {
        assert(page->isInternalPage());
        auto node = shdb::BTreeInternalPage(page);
        auto child_page_id = node.lookup(index_key);
        page = index_table.getPage(child_page_id);
    }
    auto node = shdb::BTreeLeafPage(page);
    auto ans = node.lookup(index_key);
    if (ans.has_value()) {
        result.push_back(ans.value());
    }
}

namespace
{

class BTreeEmptyIndexIterator : public IIndexIterator
{
public:
    std::optional<std::pair<IndexKey, RowId>> nextRow() override { return {}; }
};

class BTreeIndexIterator : public IIndexIterator
{
public:
    explicit BTreeIndexIterator(
        BTreeIndexTable & index_table_,
        BTreeLeafPage leaf_page_,
        size_t leaf_page_offset_,
        std::optional<Row> max_key,
        bool include_max_key_ = false)
        : index_table(index_table_)
        , leaf_page(leaf_page_)
        , leaf_page_offset(leaf_page_offset_)
        , max_key(std::move(max_key))
        , include_max_key(include_max_key_){};

    std::optional<std::pair<IndexKey, RowId>> nextRow() override { throw std::runtime_error("Not implemented"); }

private:
    bool isRowValid(const Row & key)
    {
        (void)(key);
        throw std::runtime_error("Not implemented");
    }

    BTreeIndexTable & index_table;
    BTreeLeafPage leaf_page;
    size_t leaf_page_offset;
    std::optional<Row> max_key;
    bool include_max_key;
    Row previous_row;
};

}

std::unique_ptr<IIndexIterator> BTree::read()
{
    return std::make_unique<SimpleIterator>(&index_table, metadata_page.getRootPageIndex());
}

std::unique_ptr<IIndexIterator> BTree::read(const KeyConditions & predicates)
{
    return std::make_unique<SmartIterator>(&index_table, metadata_page.getRootPageIndex(), metadata.getKeySchema(), predicates);
}

void BTree::dump(std::ostream & stream)
{
    PageIndex pages_count = index_table.getPageCount();
    for (PageIndex i = 0; i < pages_count; ++i)
    {
        auto page = index_table.getPage(i);
        auto page_type = page->getPageType();

        stream << "Page " << i << " page type " << toString(page_type) << '\n';
        switch (page_type)
        {
            case BTreePageType::invalid: {
                break;
            }
            case BTreePageType::metadata: {
                auto metadata_page = BTreeMetadataPage(page);
                metadata_page.dump(stream);
                break;
            }
            case BTreePageType::internal: {
                auto internal_page = BTreeInternalPage(page);
                internal_page.dump(stream);
                break;
            }
            case BTreePageType::leaf: {
                auto leaf_page = BTreeLeafPage(page);
                leaf_page.dump(stream);
                break;
            }
        }
    }
}

BTreeLeafPage BTree::lookupLeafPage(const IndexKey & index_key)
{
    (void)(index_key);
    throw std::runtime_error("Not implemented");
}

BTreeLeafPage BTree::lookupLeftmostLeafPage()
{
    throw std::runtime_error("Not implemented");
}

BTreePtr BTree::createIndex(const IndexMetadata & index_metadata, Store & store)
{
    return std::shared_ptr<BTree>(new BTree(index_metadata, store, {}));
}

BTreePtr BTree::createIndex(const IndexMetadata & index_metadata, size_t page_max_keys_size, Store & store)
{
    return std::shared_ptr<BTree>(new BTree(index_metadata, store, page_max_keys_size));
}

void BTree::removeIndex(const std::string & name_, Store & store)
{
    store.removeTable(name_);
}

void BTree::removeIndexIfExists(const std::string & name_, Store & store)
{
    store.removeTableIfExists(name_);
}

}
