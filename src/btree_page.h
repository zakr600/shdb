#pragma once

#include <cstring>
#include <iostream>
#include <memory>
#include <optional>

#include "bufferpool.h"
#include "comparator.h"
#include "marshal.h"
#include "page.h"
#include "row.h"
#include "table.h"

namespace shdb
{

enum BTreePageType : uint32_t
{
    invalid = 0,
    metadata,
    internal,
    leaf
};

std::string toString(BTreePageType page_type);

class BTreePage : public IPage
{
public:
    explicit BTreePage(
        std::shared_ptr<Frame> frame_, std::shared_ptr<Marshal> marshal_, uint32_t key_size_in_bytes_, uint32_t max_page_size_)
        : frame(std::move(frame_)), marshal(std::move(marshal_)), key_size_in_bytes(key_size_in_bytes_), max_page_size(max_page_size_)
    {
    }

    static constexpr size_t HeaderOffset = sizeof(BTreePageType);

    const std::shared_ptr<Frame> & getFrame() const { return frame; }

    std::shared_ptr<Frame> & getFrame() { return frame; }

    const std::shared_ptr<Marshal> & getMarshal() const { return marshal; }

    BTreePageType getPageType() const { return getValue<BTreePageType>(0); }

    void setPageType(BTreePageType btree_page_type) { setValue(0, static_cast<uint32_t>(btree_page_type)); }

    bool isInvalidPage() const { return getPageType() == BTreePageType::invalid; }

    bool isLeafPage() const { return getPageType() == BTreePageType::leaf; }

    bool isInternalPage() const { return getPageType() == BTreePageType::internal; }

    bool isMetadataPage() const { return getPageType() == BTreePageType::metadata; }

    uint32_t getMaxPageSize() const { return max_page_size; }

    uint32_t getMinPageSize() const { return max_page_size / 2; }

    template <typename T>
    const T * getPtrValue(size_t index, size_t bytes_offset = 0) const
    {
        return reinterpret_cast<T *>(frame->getData() + bytes_offset) + index;
    }

    template <typename T>
    T * getPtrValue(size_t index, size_t bytes_offset = 0)
    {
        return reinterpret_cast<T *>(frame->getData() + bytes_offset) + index;
    }

    template <typename T>
    const T & getValue(size_t index, size_t bytes_offset = 0) const
    {
        return *getPtrValue<T>(index, bytes_offset);
    }

    template <typename T>
    T & getValue(size_t index, size_t bytes_offset = 0)
    {
        return *getPtrValue<T>(index, bytes_offset);
    }

    template <typename T>
    void setValue(size_t index, const T & value, size_t bytes_offset = 0)
    {
        getValue<T>(index, bytes_offset) = value;
    }

    const uint32_t key_size_in_bytes;
    const uint32_t max_page_size;

private:
    std::shared_ptr<Frame> frame;
    std::shared_ptr<Marshal> marshal;
};

using BTreePagePtr = std::shared_ptr<BTreePage>;

/** BTreeMetadataPage, first page in BTree index.
  * Contains necessary metadata information for btree index startup.
  *
  * Header format:
  * --------------------------------------------------------------------------
  * | PageType (4) | RootPageIndex (4) | KeySizeInBytes (4) | MaxPageSize(4) |
  * --------------------------------------------------------------------------
  */
class BTreeMetadataPage
{
public:
    explicit BTreeMetadataPage(BTreePagePtr page) : page(std::move(page)) { }

    static_assert(sizeof(PageIndex) == sizeof(uint32_t));

    static constexpr size_t RootPageIndexHeaderOffset = 0;

    static constexpr size_t KeySizeInBytesHeaderOffset = 1;

    static constexpr size_t MaxPageSizeHeaderOffset = 2;

    const BTreePagePtr & getRawPage() const { return page; }

    PageIndex getRootPageIndex() const { return page->getValue<PageIndex>(RootPageIndexHeaderOffset, BTreePage::HeaderOffset); }

    void setRootPageIndex(PageIndex root_page_index)
    {
        page->setValue(RootPageIndexHeaderOffset, root_page_index, BTreePage::HeaderOffset);
    }

    uint32_t getKeySizeInBytes() const { return page->getValue<uint32_t>(KeySizeInBytesHeaderOffset, BTreePage::HeaderOffset); }

    void setKeySizeInBytes(uint32_t key_size_in_bytes)
    {
        page->setValue(KeySizeInBytesHeaderOffset, key_size_in_bytes, BTreePage::HeaderOffset);
    }

    uint32_t getMaxPageSize() const { return page->getValue<uint32_t>(MaxPageSizeHeaderOffset, BTreePage::HeaderOffset); }

    void setMaxPageSize(uint32_t max_page_size) { page->setValue(MaxPageSizeHeaderOffset, max_page_size, BTreePage::HeaderOffset); }

    std::ostream & dump(std::ostream & stream, size_t offset = 0) const
    {
        std::string offset_string(offset, ' ');

        stream << offset_string << "Root page index " << getRootPageIndex() << '\n';
        stream << offset_string << "Key size in bytes " << getKeySizeInBytes() << '\n';
        stream << offset_string << "Max page size " << getMaxPageSize() << '\n';

        return stream;
    }

private:
    BTreePagePtr page;
};

class BTreePageItem {
public:

    Row key;
    PageIndex value;
};

class BTreeLeafItem {
public:

    Row key;
    RowId value;
};

/* BTree internal page.
 * Store N indexed keys and N + 1 child page indexes (PageIndex) within internal page.
 * First key is always invalid.
 *
 *  Header format (size in bytes, 4 * 2 = 8 bytes in total):
 *  -------------------------------
 * | PageType (4) | CurrentSize(4) |
 *  -------------------------------
 *
 * Internal page format (keys are stored in order):
 *
 *  -------------------------------------------------------------------------------------------------
 * | HEADER | INVALID_KEY(1) + PAGE_INDEX(1) | KEY(2) + PAGE_INDEX(2) | ... | KEY(n) + PAGE_INDEX(n) |
 *  -------------------------------------------------------------------------------------------------
 */
class BTreeInternalPage
{
public:
    explicit BTreeInternalPage(BTreePagePtr page_) : page(std::move(page_)) { }

    static_assert(sizeof(PageIndex) == sizeof(BTreePageType));

    static_assert(sizeof(PageIndex) == sizeof(uint32_t));

    static constexpr size_t CurrentSizeHeaderIndex = 0;

    static constexpr size_t HeaderOffset = BTreePage::HeaderOffset + sizeof(uint32_t) * (CurrentSizeHeaderIndex + 1);

    static constexpr size_t calculateMaxKeysSize(uint32_t key_size_in_bytes)
    {
        return (PageSize - HeaderOffset) / (key_size_in_bytes + sizeof(PageIndex));
    }

    const BTreePagePtr & getRawPage() const { return page; }

    uint32_t getSize() const { return page->getValue<uint32_t>(CurrentSizeHeaderIndex, BTreePage::HeaderOffset); }

    void setSize(uint32_t size) { page->setValue(CurrentSizeHeaderIndex, size, BTreePage::HeaderOffset); }

    void increaseSize(uint32_t amount) { page->getValue<uint32_t>(CurrentSizeHeaderIndex, BTreePage::HeaderOffset) += amount; }

    Row getKey(size_t index) const
    {
        BTreePageItem item = loadEntry(index);
        return item.key;
    }

    PageIndex getValue(size_t index) const
    {
        BTreePageItem item = loadEntry(index);
        return item.value;
    }

    /// Set value for specified index
    void setValue(size_t index, const PageIndex & value)
    {
        BTreePageItem item = loadEntry(index);
        item.value = value;
        saveEntry(index, item);
    }

    /// Set key and value for specified index
    void setEntry(size_t index, const Row & key, const PageIndex & value)
    {
        BTreePageItem item;
        item.value = value;
        item.key = key;
        saveEntry(index, item);
    }

    /// Insert first value for invalid key
    void insertFirstEntry(const PageIndex & value)
    {
        BTreePageItem item;
        item.value = value;
        saveEntry(0, item);
        setSize(1);
    }

    /// Insert key and value for specified index
    bool insertEntry(size_t index, const Row & key, const PageIndex & value)
    {
        if (getSize() >= calculateMaxKeysSize(page->key_size_in_bytes)) {
            return false;
        }
        int size = getSize();
        for (int i = size - 1; i >= index; --i) {
            setEntry(i + 1, getKey(i), getValue(i));
        }
        setEntry(index, key, value);
        setSize(getSize() + 1);
        ++size;
//        for (int i = 2; i < size; ++i) {
//            if (compareRows(getKey(i - 1), getKey(i)) >= 0) {
//                for (int j = 1; j < size; ++j) {
//                    std::cout << getKey(j) << std::endl;
//                }
//                assert(false);
//            }
//        }
        return true;
    }

    /// Lookup specified key in page
    std::pair<PageIndex, size_t> lookupWithIndex(const Row & key) const
    {
        size_t size = getSize();
        int l = -1, r = size;
        while (r - l > 1) {
            int mid = (l + r) / 2;
            if (compareRows(key, getKey(mid)) < 0) {
                r = mid;
            } else {
                l = mid;
            }
        }
        return {getValue(l), l};
        for (size_t i = 1; i < size; ++i) {
            if (compareRows(key, getKey(i)) < 0) {
                return {
                    getValue(i - 1), i - 1};
            }
        }
        return {getValue(size - 1), size - 1};
    }

    /// Lookup specified key in page
    PageIndex lookup(const Row & key) const { return lookupWithIndex(key).first; }

    /** Split current page and move top half of keys to rhs_page.
      * Return top key.
      */
    Row split(BTreeInternalPage & rhs_page)
    {
        int all_size = getSize();
        int lhs_size = all_size / 2;
        int rhs_size = all_size - lhs_size;
        for (int i = 0; i < rhs_size; ++i) {
            rhs_page.setEntry(i, getKey(i + lhs_size), getValue(i + lhs_size));
        }
        setSize(lhs_size);
        rhs_page.setSize(rhs_size);
        return Row();
    }

    std::ostream & dump(std::ostream & stream, size_t offset = 0) const
    {
        size_t size = getSize();

        std::string offset_string(offset, ' ');
        stream << offset_string << "Size " << size << '\n';
        for (size_t i = 0; i < size; ++i)
        {
            stream << offset_string << "I " << i << " key " << (i == 0 ? "invalid" : toString(getKey(i)));
            stream << " value " << getValue(i) << '\n';
        }

        return stream;
    }

private:
    inline uint8_t * getEntryStartOffset(size_t index) const
    {
        uint8_t * key_ptr = page->getPtrValue<uint8_t>(0, HeaderOffset);
        size_t key_offset = getEntrySize() * index;
        return key_ptr + key_offset;
    }

    inline size_t getEntrySize() const { return page->key_size_in_bytes + sizeof(PageIndex); }

    BTreePagePtr page;

    BTreePageItem loadEntry(size_t index) const {
        BTreePageItem item;
        uint8_t* data = getEntryStartOffset(index);
        if (index != 0)
            item.key = page->getMarshal()->deserializeRow(data);
        item.value = *(data + page->key_size_in_bytes);
        return item;
    }

    void saveEntry(size_t index, BTreePageItem item) {
        uint8_t* data = getEntryStartOffset(index);
        if (index != 0)
            page->getMarshal()->serializeRow(data, item.key);
        *(data + page->key_size_in_bytes) = item.value;
    }

};

/** Store indexed key and value. Only support unique key.
  *
  *  Header format (size in byte, 4 * 4 = 16 bytes in total):
  *  ------------------------------------------------------------------------
  * | PageType (4) | PageSize(4) | PreviousPageIndex (4) | NextPageIndex (4) |
  *  ------------------------------------------------------------------------
  *
  *  Leaf page format (keys are stored in order):
  *  --------------------------------------------------------------------
  * | HEADER | KEY(1) + RID(1) | KEY(2) + RID(2) | ... | KEY(n) + RID(n) |
  *  --------------------------------------------------------------------
  */
class BTreeLeafPage
{
public:
    explicit BTreeLeafPage(BTreePagePtr page_) : page(std::move(page_)) { }

    static_assert(sizeof(PageIndex) == sizeof(BTreePageType));

    static_assert(sizeof(PageIndex) == sizeof(uint32_t));

    static constexpr size_t PageSizeHeaderIndex = 0;

    static constexpr size_t PreviousPageIdHeaderIndex = 1;

    static constexpr size_t NextPageIdHeaderIndex = 2;

    static constexpr size_t HeaderOffset = BTreePage::HeaderOffset + sizeof(uint32_t) * (NextPageIdHeaderIndex + 1);

    static constexpr size_t calculateMaxKeysSize(uint32_t key_size_in_bytes)
    {
        return (PageSize - HeaderOffset) / (sizeof(RowId) + key_size_in_bytes);
    }

    const BTreePagePtr & getRawPage() const { return page; }

    uint32_t getSize() const { return page->getValue<uint32_t>(PageSizeHeaderIndex, BTreePage::HeaderOffset); }

    void setSize(uint32_t size) { page->setValue(PageSizeHeaderIndex, size, BTreePage::HeaderOffset); }

    void increaseSize(uint32_t amount) { page->getValue<uint32_t>(PageSizeHeaderIndex, BTreePage::HeaderOffset) += amount; }

    PageIndex getPreviousPageIndex() const { return page->getValue<PageIndex>(PreviousPageIdHeaderIndex, BTreePage::HeaderOffset); }

    void setPreviousPageIndex(PageIndex previous_page_index)
    {
        page->setValue(PreviousPageIdHeaderIndex, previous_page_index, BTreePage::HeaderOffset);
    }

    PageIndex getNextPageIndex() const { return page->getValue<PageIndex>(NextPageIdHeaderIndex, BTreePage::HeaderOffset); }

    void setNextPageIndex(PageIndex next_page_index) { page->setValue(NextPageIdHeaderIndex, next_page_index, BTreePage::HeaderOffset); }

    Row getKey(size_t index) const
    {
        uint8_t* data = getEntryStartOffset(index);
        return page->getMarshal()->deserializeRow(data);
    }

    RowId getValue(size_t index) const
    {
        RowId value;
        uint8_t* data = getEntryStartOffset(index);
        memcpy(&value, data + page->key_size_in_bytes, sizeof(RowId));
        return value;
    }

    /// Insert specified key and value in page
    bool insert(const Row & key, const RowId & value)
    {
        if (getSize() >= calculateMaxKeysSize(page->key_size_in_bytes)) {
            return false;
        }
        std::vector<BTreeLeafItem> items;
        size_t size = getSize();
        for (int i = 0; i < size; ++i) {
            items.push_back(loadEntry(i));
            if (compareRows(key, items[i].key) == 0) {
                throw std::runtime_error("duplicate key!");
            }
        }
        bool added = false;
        for (int pos = 0; pos <= size; ++pos) {
            if ((pos - 1 < 0 || compareRows(items[pos - 1].key, key) < 0) && (pos == size || compareRows(key, items[pos].key) < 0)) {
                items.insert(items.begin() + pos, BTreeLeafItem{.key = key, .value = value});
                added = true;
                break;
            }
        }
//        assert(added);
//        for (int i = 1; i < items.size(); ++i) {
//            assert(compareRows(items[i - 1].key, items[i].key) <= 0);
//        }
        for (int i = 0; i < items.size(); ++i) {
            saveEntry(i, items[i]);
        }
        setSize(items.size());
        return true;
    }

    /// Lookup specified key in page
    std::optional<RowId> lookup(const Row & key) const
    {
        size_t size = getSize();
        if (size == 0) {
            return std::optional<RowId>();
        }
        int l = 0, r = size;
        while (r - l > 1) {
            int mid = (l + r) / 2;
            int cm = compareRows(getKey(mid), key);
            if (cm == 0) {
                return getValue(mid);
            } else if (cm < 0) {
                l = mid;
            } else {
                r = mid;
            }
        }
        if (compareRows(getKey(l), key) == 0) {
            return getValue(l);
        } else {
            return std::optional<RowId>();
        }
    }

    /// Return index of lower bound for specified key
    size_t lowerBound(const Row & key) const
    {
        (void)(key);
        throw std::runtime_error("Not implemented");
    }

    /// Remove specified key from page
    bool remove(const Row & key)
    {
        size_t size = getSize();
        size_t nsize = 0;
        bool found = false;
        for (int i = 0; i < size; ++i) {
            BTreeLeafItem item = loadEntry(i);
            if (compareRows(item.key, key) != 0) {
                saveEntry(nsize++, item);
            } else {
                found = true;
            }
        }
        setSize(nsize);
        return found;
    }

    /** Split current page and move top half of keys to rhs_page.
      * Return top key.
      */
    Row split(BTreeLeafPage & rhs_page)
    {
        int all_size = getSize();
        int lhs_size = all_size / 2;
        int rhs_size = all_size - lhs_size;
        for (int i = 0; i < rhs_size; ++i) {
            rhs_page.insert(getKey(i + lhs_size), getValue(i + lhs_size));
        }
        setSize(lhs_size);
        rhs_page.setSize(rhs_size);
        return Row();
    }

    std::ostream & dump(std::ostream & stream, size_t offset = 0) const
    {
        size_t size = getSize();

        std::string offset_string(offset, ' ');
        stream << offset_string << "Size " << size << '\n';

        auto previous_page_index = getPreviousPageIndex();
        auto next_page_index = getNextPageIndex();

        stream << "Previous page index " << (previous_page_index == InvalidPageIndex ? "invalid" : std::to_string(previous_page_index))
               << '\n';
        stream << "Next page index " << (next_page_index == InvalidPageIndex ? "invalid" : std::to_string(next_page_index)) << '\n';

        for (size_t i = 0; i < size; ++i)
        {
            stream << offset_string << "I " << i << " key " << getKey(i);
            stream << " value " << getValue(i) << '\n';
        }

        return stream;
    }

private:
    inline uint8_t * getEntryStartOffset(size_t index) const
    {
        uint8_t * key_ptr = page->getPtrValue<uint8_t>(0, HeaderOffset);
        size_t key_offset = getEntrySize() * index;
        return key_ptr + key_offset;
    }

    inline size_t getEntrySize() const { return page->key_size_in_bytes + sizeof(RowId); }

    BTreePagePtr page;

    BTreeLeafItem loadEntry(size_t index) const {
        BTreeLeafItem item;
        uint8_t* data = getEntryStartOffset(index);
        item.key = page->getMarshal()->deserializeRow(data);
        memcpy(&item.value, data + page->key_size_in_bytes, sizeof(RowId));
        return item;
    }

    void saveEntry(size_t index, BTreeLeafItem item) {
        uint8_t* data = getEntryStartOffset(index);
        page->getMarshal()->serializeRow(data, item.key);
        memcpy(data + page->key_size_in_bytes, &item.value, sizeof(RowId));
    }
};

std::shared_ptr<IPageProvider>
createBTreePageProvider(std::shared_ptr<Marshal> marshal, uint32_t key_size_in_bytes, uint32_t max_page_size);

}
