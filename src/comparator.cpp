#include "comparator.h"

#include <cassert>
#include <cstdint>
#include <string>
#include <variant>

namespace shdb
{

int16_t Comparator::operator()(const Row & lhs, const Row & rhs) const
{
    for (size_t i = 0; i < lhs.size() && i < rhs.size(); ++i) {
        if (lhs[i] < rhs[i]) {
            return -1;
        } else if (lhs[i] > rhs[i]) {
            return 1;
        }
    }
    if (lhs.size() < rhs.size()) {
        return -1;
    }
    if (lhs.size() == rhs.size()) {
        return 0;
    }
    return 1;
}

bool operator<(const Null& lhs, const Null & rhs) {
    return false;
}
bool operator<=(const Null& lhs, const Null & rhs) {
    return false;
}
bool operator>=(const Null& lhs, const Null & rhs) {
    return false;
}
bool operator!=(const Null& lhs, const Null & rhs) {
    return false;
}
bool operator>(const Null& lhs, const Null & rhs) {
    return false;
}

int16_t compareRows(const Row & lhs, const Row & rhs)
{
    return Comparator()(lhs, rhs);
}

int16_t compareValue(const Value & lhs, const Value & rhs)
{
    if (lhs < rhs) {
        return -1;
    }
    if (lhs > rhs) {
        return 1;
    }
    return 0;
}

}
