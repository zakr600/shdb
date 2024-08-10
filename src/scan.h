#pragma once

#include "table.h"

namespace shdb
{

class ScanIterator
{
public:
    ScanIterator(std::shared_ptr<ITable> table, PageIndex page_index, RowIndex row_index): table(table), row_index(row_index), page_index(page_index)
    {
    }

    RowId getRowId() const { return RowId{page_index, row_index}; }

    Row getRow() { return table->getRow(getRowId()); }

    Row operator*() { return table->getRow(getRowId()); }

    bool operator==(const ScanIterator & other) const
    {
        return this->table == other.table && this->row_index == other.row_index && this->page_index == other.page_index;
    }

    bool operator!=(const ScanIterator & other) const
    {
        return !(*this == other);
    }

    ScanIterator & operator++() {
        auto page = table->getPage(page_index);
        if (row_index == page->getRowCount()) {
            ++page_index;
            row_index = 0;
        } else {
            ++row_index;
        }
        return *this;
    }

private:
    std::shared_ptr<ITable> table;
    RowIndex row_index;
    PageIndex page_index;
};


class Scan
{
public:
    explicit Scan(std::shared_ptr<ITable> table): table(table)
    {
    }

    ScanIterator begin() const { return ScanIterator(table, 0, 0); }

    ScanIterator end() const { return ScanIterator(table, table->getPageCount(), 0); }

private:
    std::shared_ptr<ITable> table;
};

}
