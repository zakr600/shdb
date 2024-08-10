#include "executor.h"
#include "scan.h"
#include "comparator.h"

namespace shdb
{

namespace
{

class ReadFromRowsExecutor : public IExecutor
{
public:
    explicit ReadFromRowsExecutor(Rows rows_, std::shared_ptr<Schema> rows_schema_)
        : rows(std::move(rows_)), rows_schema(std::move(rows_schema_))
    {
    }

    std::optional<Row> next() override {
        if (idx < rows.size()) {
            return rows[idx++];
        } else {
            return std::optional<Row>();
        }
    }

    std::shared_ptr<Schema> getOutputSchema() override {
        return rows_schema;
    }

private:
    Rows rows;
    std::shared_ptr<Schema> rows_schema;
    size_t idx = 0;
};

class ReadFromTableExecutor : public IExecutor
{
public:
    explicit ReadFromTableExecutor(std::shared_ptr<ITable> table_, std::shared_ptr<Schema> table_schema_)
        : table(std::move(table_)), table_schema(std::move(table_schema_)), scan(table), it(scan.begin())
    {
    }

    std::optional<Row> next() override {
        while (true)
        {
            if (it == scan.end())
            {
                return std::optional<Row>();
            }
            else if (!it.getRow().empty())
            {
                auto row = it.getRow();
                ++it;
                return row;
            } else {
                ++it;
            }
        }
    }

    std::shared_ptr<Schema> getOutputSchema() override {
        return table_schema;
    }

private:
    std::shared_ptr<ITable> table;
    std::shared_ptr<Schema> table_schema;
    Scan scan;
    ScanIterator it;
};

class ExpressionsExecutor : public IExecutor
{
public:
    explicit ExpressionsExecutor(ExecutorPtr input_executor_, Expressions expressions_)
        : input_executor(std::move(input_executor_)), expressions(std::move(expressions_))
    {
    }

    std::optional<Row> next() override {
        auto input = input_executor->next();
        if (!input.has_value()) {
            return std::optional<Row>();
        }
        Row row;
        for (const auto & expression : expressions) {
            row.push_back(expression->evaluate(input.value()));
        }
        return row;
    }

    std::shared_ptr<Schema> getOutputSchema() override {
        auto schema = std::make_shared<Schema>();
        for (const auto & expression : expressions) {
            schema->push_back(expression->getResultType());
        }
        return schema;
    }

private:
    ExecutorPtr input_executor;
    Expressions expressions;
};

class FilterExecutor : public IExecutor
{
public:
    explicit FilterExecutor(ExecutorPtr input_executor_, ExpressionPtr filter_expression_)
        : input_executor(std::move(input_executor_)), filter_expression(std::move(filter_expression_))
    {
    }

    std::optional<Row> next() override {
        std::optional<Row> row;
        while ((row = input_executor->next()).has_value()) {
            if (std::get<bool>(filter_expression->evaluate(row.value()))) {
                return row.value();
            }
        }
        return row;
    }

    std::shared_ptr<Schema> getOutputSchema() override {
        return input_executor->getOutputSchema();
    }

private:
    ExecutorPtr input_executor;
    ExpressionPtr filter_expression;
};

class SortExecutor : public IExecutor
{
public:
    explicit SortExecutor(ExecutorPtr input_executor_, SortExpressions sort_expressions_)
        : input_executor(std::move(input_executor_)), sort_expressions(std::move(sort_expressions_))
    {
        std::optional<Row> row;
        while ((row = input_executor->next()).has_value()) {
            rows.push_back(std::move(row.value()));
        }
        std::sort(rows.begin(), rows.end(), [&](const Row& lhs, const Row& rhs) {
            for (auto & sort_expression : sort_expressions) {
                auto lhs_val = sort_expression.expression->evaluate(lhs);
                auto rhs_val = sort_expression.expression->evaluate(rhs);
                if (lhs_val == rhs_val) {
                    continue;
                }
                if (!sort_expression.desc) {
                    return compareValue(lhs_val, rhs_val) == -1;
                } else {
                    return compareValue(lhs_val, rhs_val) == 1;
                }
            }
            return false;
        });
    }

    std::optional<Row> next() override {
        if (idx < rows.size()) {
            return rows[idx++];
        } else {
            return std::optional<Row>();
        }
    }

    std::shared_ptr<Schema> getOutputSchema() override {
        return input_executor->getOutputSchema();
    }

private:
    ExecutorPtr input_executor;
    SortExpressions sort_expressions;
    std::vector<Row> rows;
    size_t idx = 0;
};

class JoinExecutor : public IExecutor
{
public:
    explicit JoinExecutor(ExecutorPtr left_input_executor_, ExecutorPtr right_input_executor_)
        : left_input_executor(std::move(left_input_executor_)), right_input_executor(std::move(right_input_executor_))
    {
        out_schema = std::make_shared<Schema>();
        key_schema = std::make_shared<Schema>();
        for (const auto& item : *left_input_executor->getOutputSchema()) {
            out_schema->push_back(item);
        }
        for (const auto& item : *right_input_executor->getOutputSchema()) {
            if (std::find((*out_schema).begin(), (*out_schema).end(), item) != (*out_schema).end()) {
                key_schema->push_back(item);
            } else {
                out_schema->push_back(item);
            }
        }
        std::optional<Row> item;
        while ((item = left_input_executor->next()).has_value()) {
            Row key;
            for (size_t i = 0; i < item->size(); ++i) {
                ColumnSchema citem = (*(left_input_executor->getOutputSchema()))[i];
                if (std::find((*key_schema).begin(), (*key_schema).end(), citem) != (*key_schema).end()) {
                    key.push_back((*item)[i]);
                }
            }
            storage[key].push_back(item.value());
        }
    }

    std::optional<Row> next() override {
        if (last_row.has_value()) {
            if (storage[saved_key].size() < idx) {
                Row value = storage[saved_key][idx++];
                for (size_t i = 0; i < last_row->size(); ++i) {
                    ColumnSchema citem = (*(right_input_executor->getOutputSchema()))[i];
                    if (std::find((*key_schema).begin(), (*key_schema).end(), citem) == (*key_schema).end()) {
                        value.push_back((*last_row)[i]);
                    }
                }
                return value;
            }
        }
        last_row.reset();
        std::optional<Row> item;
        while ((item = right_input_executor->next()).has_value()) {
            Row key;
            for (size_t i = 0; i < item->size(); ++i) {
                ColumnSchema citem = (*(right_input_executor->getOutputSchema()))[i];
                if (std::find((*key_schema).begin(), (*key_schema).end(), citem) != (*key_schema).end()) {
                    key.push_back((*item)[i]);
                }
            }
            if (storage.contains(key) && !storage[key].empty()) {
                Row value = storage[key][0];
                idx = 1;
                saved_key = key;
                for (size_t i = 0; i < item->size(); ++i) {
                    ColumnSchema citem = (*(right_input_executor->getOutputSchema()))[i];
                    if (std::find((*key_schema).begin(), (*key_schema).end(), citem) == (*key_schema).end()) {
                        value.push_back((*item)[i]);
                    }
                }
                last_row = item;
                return value;
            }
        }
        return std::optional<Row>();
    }

    std::shared_ptr<Schema> getOutputSchema() override {
        return out_schema;
    }

private:
    ExecutorPtr left_input_executor;
    ExecutorPtr right_input_executor;
    std::unordered_map<Row, std::vector<Row>> storage;
    std::shared_ptr<Schema> key_schema;
    std::shared_ptr<Schema> out_schema;
    std::optional<Row> last_row;
    Row saved_key;
    size_t idx;

};

class GroupByExecutor : public IExecutor
{
public:
    explicit GroupByExecutor(ExecutorPtr input_executor_, GroupByKeys group_by_keys_, GroupByExpressions group_by_expressions_)
        : input_executor(std::move(input_executor_))
        , group_by_keys(std::move(group_by_keys_))
        , group_by_expressions(std::move(group_by_expressions_))
    {
        throw std::runtime_error("Not implemented");
    }

    std::optional<Row> next() override { throw std::runtime_error("Not implemented"); }

    std::shared_ptr<Schema> getOutputSchema() override { throw std::runtime_error("Not implemented"); }

private:
    ExecutorPtr input_executor;
    GroupByKeys group_by_keys;
    GroupByExpressions group_by_expressions;
};

}

ExecutorPtr createReadFromRowsExecutor(Rows rows, std::shared_ptr<Schema> rows_schema)
{
    return std::make_unique<ReadFromRowsExecutor>(rows, rows_schema);
}

ExecutorPtr createReadFromTableExecutor(std::shared_ptr<ITable> table, std::shared_ptr<Schema> table_schema)
{
    return std::make_unique<ReadFromTableExecutor>(table, table_schema);
}

ExecutorPtr createExpressionsExecutor(ExecutorPtr input_executor, Expressions expressions)
{
    return std::make_unique<ExpressionsExecutor>(std::move(input_executor), expressions);
}

ExecutorPtr createFilterExecutor(ExecutorPtr input_executor, ExpressionPtr filter_expression)
{
    return std::make_unique<FilterExecutor>(std::move(input_executor), filter_expression);
}

ExecutorPtr createSortExecutor(ExecutorPtr input_executor, SortExpressions sort_expressions)
{
    return std::make_unique<SortExecutor>(std::move(input_executor), sort_expressions);
}

ExecutorPtr createJoinExecutor(ExecutorPtr left_input_executor, ExecutorPtr right_input_executor)
{
    return std::make_unique<JoinExecutor>(std::move(left_input_executor), std::move(right_input_executor));
}

ExecutorPtr createGroupByExecutor(ExecutorPtr input_executor, GroupByKeys group_by_keys, GroupByExpressions group_by_expressions)
{
    (void)(input_executor);
    (void)(group_by_keys);
    (void)(group_by_expressions);
    throw std::runtime_error("Not implemented");
}

RowSet rexecute(ExecutorPtr executor)
{
    RowSet ans(executor->getOutputSchema());
    std::optional<Row> t;
    while ((t = executor->next()).has_value()) {
        ans.addRow(t.value());
    }
    return ans;
}

}
