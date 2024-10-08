#include "schema.h"

namespace shdb
{

std::string toString(Type type)
{
    switch (type)
    {
        case Type::boolean:
            return "boolean";
        case Type::uint64:
            return "uint64";
        case Type::int64:
            return "int64";
        case Type::varchar:
            return "varchar";
        case Type::string:
            return "string";
    }

    return {};
}

std::ostream & operator<<(std::ostream & stream, Type type)
{
    stream << toString(type);
    return stream;
}

std::string toString(const ColumnSchema & schema)
{
    std::string result = schema.name + " " + toString(schema.type);

    if (schema.length != 0)
    {
        result += '(';
        result += std::to_string(schema.length);
        result += ')';
    }

    return result;
}

std::ostream & operator<<(std::ostream & stream, const ColumnSchema & schema)
{
    stream << toString(schema);
    return stream;
}

std::string toString(const Schema & schema)
{
    std::string result;

    for (const auto & column : schema)
    {
        result += toString(column);
        result += ", ";
    }

    if (!result.empty())
    {
        result.pop_back();
        result.pop_back();
    }

    return result;
}

std::ostream & operator<<(std::ostream & stream, const Schema & schema)
{
    stream << toString(schema);
    return stream;
}

}
