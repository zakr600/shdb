#pragma once

#include <vector>
#include <map>
#include <cassert>

namespace shdb
{

template <class PageId, class FrameIndex>
class ClockCache
{
public:
    explicit ClockCache(std::vector<FrameIndex> frame_index): frames(std::move(frame_index)) {
        size = frames.size();
        isLocked.assign(frames.size(), false);
        isRead.assign(frames.size(), false);
        itemToPageId.resize(frames.size());
    }

    FrameIndex put(const PageId& pageId) {
        while (true) {
            if (isRead[ptr]) {
                isRead[ptr] = false;
                moveNext();
            } else if (isLocked[ptr]) {
                moveNext();
            } else {
                map.erase(itemToPageId[ptr]);
                itemToPageId[ptr] = pageId;
                map[pageId] = ptr;
                return frames[ptr];
            }
        }
    }

    std::pair<bool, FrameIndex> find(const PageId& pageId) {
        auto it = map.find(pageId);
        if (it == map.end()) {
            return {false, 0};
        }
        FrameIndex index = it->second;
        isRead[index] = true;
        return {true, index};
    }

    void lock(const PageId& pageId) {
        assert(map.find(pageId) != map.end());
        FrameIndex index = map[pageId];
        isLocked[index] = true;
    }

    void unlock(const PageId& pageId) {
        assert(map.find(pageId) != map.end());
        FrameIndex index = map[pageId];
        isLocked[index] = false;
        map.erase(pageId);

    }

private:
    void moveNext() {
        ++ptr;
        if (ptr == size) {
            ptr = 0;
        }
    }

    std::vector<FrameIndex> frames;
    std::vector<bool> isLocked;
    std::vector<bool> isRead;
    std::vector<PageId> itemToPageId;
    std::map<PageId, FrameIndex> map;
    size_t size = 0;
    int ptr = 0;
};

}

