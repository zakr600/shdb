#include "interpreter.h"

#include "accessors.h"
#include "ast.h"
#include "ast_visitor.h"
#include "executor.h"
#include "expression.h"
#include "lexer.h"
#include "parser.hpp"
#include "row.h"

namespace shdb
{

Interpreter::Interpreter(std::shared_ptr<Database> db_) : db(std::move(db_))
{
    registerAggregateFunctions(aggregate_function_factory);
}

RowSet Interpreter::execute(const std::string & query)
{
    Lexer lexer(query.c_str(), query.c_str() + query.size());
    ASTPtr result;
    std::string error;
    shdb::Parser parser(lexer, result, error);
    parser.parse();
    if (!result || !error.empty())
        throw std::runtime_error("Bad input: " + error);

    switch (result->type)
    {
        case ASTType::selectQuery:
            return executeSelect(std::static_pointer_cast<ASTSelectQuery>(result));
        case ASTType::insertQuery:
            executeInsert(std::static_pointer_cast<ASTInsertQuery>(result));
            break;
        case ASTType::createQuery:
            executeCreate(std::static_pointer_cast<ASTCreateQuery>(result));
            break;
        case ASTType::dropQuery:
            executeDrop(std::static_pointer_cast<ASTDropQuery>(result));
            break;
        default:
            throw std::runtime_error("Invalid AST. Expected SELECT, INSERT, CREATE or DROP query");
    }

    return RowSet{};
}

RowSet Interpreter::executeSelect(const ASTSelectQueryPtr & select_query_ptr)
{

    std::unique_ptr<IExecutor> executor;
    if (select_query_ptr->from.empty()) {
        executor = createReadFromRowsExecutor({Row()}, std::shared_ptr<Schema>());
    } else {
        auto table_name = select_query_ptr->from[0];
        auto table = db->getTable(table_name);
        auto schema = db->findTableSchema(table_name);
        executor = createReadFromTableExecutor(table, schema);

        for (size_t i = 1; i < select_query_ptr->from.size(); ++i) {
            auto table_name1 = select_query_ptr->from[i];
            auto table1 = db->getTable(table_name1);
            auto schema1 = db->findTableSchema(table_name1);
            executor = createJoinExecutor(std::move(executor), createReadFromTableExecutor(table1, schema1));
        }
    }

    if (select_query_ptr->getWhere() != nullptr) {
        auto schema = executor->getOutputSchema();
        executor = createFilterExecutor(
            std::move(executor), buildExpression(select_query_ptr->getWhere(), std::make_shared<SchemaAccessor>(schema)));
    }

    if (select_query_ptr->getOrder() != nullptr) {
        SortExpressions sort_exprs;
        auto schema = executor->getOutputSchema();
        for (const auto& ast : select_query_ptr->getOrder()->getChildren()) {
            const auto ast_order = std::static_pointer_cast<const ASTOrder>(ast);
            SortExpression expr;
            expr.desc = ast_order->desc;
            expr.expression = buildExpression(ast_order->getExpr(), std::make_shared<SchemaAccessor>(schema));
            sort_exprs.push_back(expr);
        }
        executor = createSortExecutor(std::move(executor), sort_exprs);
    }

    std::vector<ASTPtr> proj;
    SchemaAccessorPtr schemaAccessor;
    if (!select_query_ptr->from.empty()) {
        schemaAccessor.reset(new SchemaAccessor(executor->getOutputSchema()));
    }
    if (select_query_ptr->getProjection() != nullptr) {
        proj = select_query_ptr->getProjectionList().getChildren();
    } else {
        for (const auto& item : *executor->getOutputSchema()) {
            proj.push_back(newIdentifier(item.name));
        }
    }

    auto expressions = buildExpressions(proj, schemaAccessor);
    executor = createExpressionsExecutor(std::move(executor), expressions);

    return rexecute(std::move(executor));
}

void Interpreter::executeInsert(const std::shared_ptr<ASTInsertQuery> & insert_query)
{
    auto proj = insert_query->getValuesList().getChildren();

    auto expressions = buildExpressions(proj, std::shared_ptr<SchemaAccessor>{});
    auto readFromRowsExecutor = createReadFromRowsExecutor({Row()}, std::shared_ptr<Schema>());
    auto expressionsExecutor = createExpressionsExecutor(std::move(readFromRowsExecutor), expressions);

    auto mySchema = expressionsExecutor->getOutputSchema();

    std::string table_name = insert_query->table;
    Row row = rexecute(std::move(expressionsExecutor)).getRows()[0];

    assert(db->checkTableExists(table_name));
    auto tableSchema = db->findTableSchema(table_name);
    for (size_t i = 0; i < row.size(); ++i) {
        if((*mySchema)[i].type != (*tableSchema)[i].type) {
            throw std::runtime_error("invalid row schema!");
        }
    }
    db->getTable(table_name)->insertRow(row);
}

void Interpreter::executeCreate(const ASTCreateQueryPtr & create_query)
{
    db->createTable(create_query->table, create_query->schema);
}

void Interpreter::executeDrop(const ASTDropQueryPtr & drop_query)
{
    db->dropTable(drop_query->table);
}

}
