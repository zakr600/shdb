#pragma once

#include "fixed.h"
#include "scan.h"
#include "schema.h"
#include "store.h"
#include "table.h"

namespace shdb
{

class Catalog
{
public:
    explicit Catalog(std::shared_ptr<Store> store): store(store) { }

    void saveTableSchema(const std::filesystem::path & name, std::shared_ptr<Schema> schema)
    {
        auto table = connectToTable(name);


        for (size_t index = 0; index < schema->size(); ++index)
        {
            Row row;

            row.push_back((*schema)[index].name);
            row.push_back(static_cast<uint64_t>((*schema)[index].length));
            std::string tp;
            switch ((*schema)[index].type)
            {
                case Type::boolean: {
                    tp = "boolean";
                    break;
                }
                case Type::uint64: {
                    tp = "uint64";
                    break;
                }
                case Type::int64: {
                    tp = "int64";
                    break;
                }
                case Type::varchar: {
                    tp = "varchar";
                    break;
                }
                case Type::string: {
                    tp = "string";
                    break;
                }
            }
            row.emplace_back(tp);
            table->insertRow(row);
        }

    }

    std::shared_ptr<Schema> findTableSchema(const std::filesystem::path & name)
    {
        auto table = connectToTable(name);

        Schema schema;

        auto scan = shdb::Scan(table);
        for (auto it = scan.begin(), end = scan.end(); it != end; ++it)
        {
            auto row = it.getRow();
            if (!row.empty())
            {
                std::string var_name = std::get<std::string>(row[0]);
                uint64_t var_size = std::get<uint64_t>(row[1]);
                Type type;
                std::string int_type = std::get<std::string>(row[2]);
                if (int_type == "boolean") {
                    type = Type::boolean;
                } else if (int_type == "uint64") {
                    type = Type::uint64;
                } else if (int_type == "int64") {
                    type = Type::int64;
                } else if (int_type == "varchar") {
                    type = Type::varchar;
                } else {
                    type = Type::string;
                }
                ColumnSchema item;
                item.length = var_size;
                item.name = var_name;
                item.type = type;
                schema.push_back(item);
            }
        }
        return std::make_shared<Schema>(schema);
    }

    void forgetTableSchema(const std::filesystem::path & name)
    {
        std::string table_name = std::string(name) + "_schema";
        store->removeTable(table_name);
    }

private:

    std::shared_ptr<ITable> connectToTable(std::string name) {
        auto fixed_schema = std::make_shared<shdb::Schema>(shdb::Schema{
            {"name", shdb::Type::varchar, 1024}, {"length", shdb::Type::uint64}, {"type", shdb::Type::varchar, 1024}});

        std::string table_name = std::string(name) + "_schema";
        if (!store->checkTableExists(table_name))
            store->createTable(table_name);
        std::shared_ptr<ITable> table = store->openTable(table_name, createFixedPageProvider(std::move(fixed_schema)));

        return table;
    }

    std::shared_ptr<Store> store;
};

}

