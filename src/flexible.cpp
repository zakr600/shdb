#include "flexible.h"
#include <cstring>
#include "marshal.h"

namespace shdb
{

class FlexiblePage : public ITablePage
{
public:
    FlexiblePage(std::shared_ptr<Frame> frame, std::shared_ptr<Marshal> marshal) : frame(std::move(frame)), marshal(std::move(marshal)) {
    }

    RowIndex getRowCount() {
        int id = -1;
        for (auto& [idx, _] : readHeader())
        {
            if (idx >= id) {
                id = idx;
            }
        }
        ++id;
        return id;
    }

    Row getRow(RowIndex index)
    {
        for (auto& [idx, row] : readHeader()) {
            if ((int)idx == index) {
                return row;
            }
        }
        return Row();
    }

    void deleteRow(RowIndex index)
    {
        std::vector<std::pair<uint8_t, Row>> new_header;

        for (auto& [idx, row] : readHeader()) {
            if ((int)idx != index) {
                new_header.emplace_back(idx, row);
            }
        }
        write_header(new_header);
    }

    std::pair<bool, RowIndex> insertRow(const Row & row)
    {
        auto header = readHeader();
        int id = -1;
        for (auto& [idx, _] : header)
        {
            if (idx >= id) {
                id = idx;
            }
        }
        ++id;

        header.emplace_back(id, row);

        if (getSize(header) <= PageSize - 10) {
            //        if (header.size() == 1) {
            write_header(header);
            return {true, id};
        }

        return {false, -1};
    }

private:

    std::vector <std::pair<uint8_t, Row>> readHeader()
    {
        uint8_t * data = this->frame->getData();

        std::vector <std::pair<uint8_t, Row>> header;
        uint32_t strings_cnt;
        memcpy(&strings_cnt, data, sizeof(strings_cnt));
        uint8_t * mem = data + sizeof(strings_cnt);
        for (int i = 0; i < strings_cnt; ++i)
        {
            uint8_t rowIndex;
            memcpy(&rowIndex, mem, sizeof(rowIndex));
            mem += sizeof(rowIndex);
            uint8_t * addr;
            memcpy(&addr, mem, sizeof(addr));
            mem += sizeof(addr);
            header.emplace_back(rowIndex, this->marshal->deserializeRow(addr));
        }
        return header;
    }

    int getSize(std::vector <std::pair<uint8_t, Row>> header) {
        int ans = sizeof(uint32_t) + header.size() * (sizeof(uint8_t) + sizeof(uint8_t*));
        for (auto [idx, row] : header) {
            ans += marshal->getRowSpace(row);
        }
        return ans;
    }

    void write_header(std::vector <std::pair<uint8_t, Row>> header) {
        uint8_t * data = this->frame->getData();

        uint32_t strings_cnt = header.size();
        uint8_t* mem = data + sizeof(strings_cnt);
        uint8_t* buf_addr = data + PageSize;

        memcpy(data, &strings_cnt, sizeof(strings_cnt));

        for (int i = 0; i < strings_cnt; ++i)
        {
            uint8_t rowIndex = header[i].first;
            buf_addr -= marshal->getRowSpace(header[i].second);
            marshal->serializeRow(buf_addr, header[i].second);
            memcpy(mem, &rowIndex, sizeof(rowIndex));
            assert(mem + sizeof(rowIndex) < this->frame->getData() + PageSize);
            assert(buf_addr + marshal->getRowSpace(header[i].second) <= this->frame->getData() + PageSize);
            mem += sizeof(rowIndex);
            memcpy(mem, &buf_addr, sizeof(buf_addr));
            mem += sizeof(buf_addr);
        }
    }

    std::shared_ptr<Frame> frame;
    std::shared_ptr<Marshal> marshal;
};

class FlexiblePageProvider : public IPageProvider
{
public:
    explicit FlexiblePageProvider(std::shared_ptr<Marshal> marshal) : marshal(marshal) { }

    std::shared_ptr<IPage> getPage(std::shared_ptr<Frame> frame) override {
        return std::make_shared<FlexiblePage>(std::move(frame), marshal);
    }

    std::shared_ptr<Marshal> marshal;
};

std::shared_ptr<IPageProvider> createFlexiblePageProvider(std::shared_ptr<Schema> schema)
{
    auto marshal = std::make_shared<Marshal>(std::move(schema));
    return std::make_shared<FlexiblePageProvider>(std::move(marshal));
}

}
