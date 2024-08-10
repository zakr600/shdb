#include "expression.h"
#include <variant>

namespace shdb
{

namespace
{

class IdentifierExpression : public IExpression
{
public:
    explicit IdentifierExpression(const std::string & identifier_name, const std::shared_ptr<SchemaAccessor> & input_schema_accessor)
    {
            idx = input_schema_accessor->getColumnIndexOrThrow(identifier_name);
            identifier_type = input_schema_accessor->getColumnOrThrow(identifier_name).type;
    }

    Type getResultType() override {
            return identifier_type;
    }

    Value evaluate(const Row & input_row) override
    {
        return input_row[idx];
    }

private:
    Type identifier_type;
    size_t idx;
};

class NumberConstantExpression : public IExpression
{
public:
    explicit NumberConstantExpression(int64_t value_) : value(value_) { }

    Type getResultType() override { return Type::int64; }

    Value evaluate(const Row &) override { return value; }

    Value value;
};

class StringConstantExpression : public IExpression
{
public:
    explicit StringConstantExpression(std::string value_) : value(value_) { }

    Type getResultType() override { return Type::string; }

    Value evaluate(const Row &) override { return value; }

    Value value;
};

class BinaryOperatorExpression : public IExpression
{
public:
    explicit BinaryOperatorExpression(
        BinaryOperatorCode binary_operator_code_, ExpressionPtr lhs_expression_, ExpressionPtr rhs_expression_)
        : binary_operator_code(binary_operator_code_)
        , lhs_expression(std::move(lhs_expression_))
        , rhs_expression(std::move(rhs_expression_))
        , lhs_type(lhs_expression->getResultType())
        , rhs_type(rhs_expression->getResultType())
    {
    }

    Type getResultType() override {
        switch (binary_operator_code)
        {
            case BinaryOperatorCode::plus:
            case BinaryOperatorCode::minus:
            case BinaryOperatorCode::mul:
            case BinaryOperatorCode::div:
                return Type::int64;
            case BinaryOperatorCode::land:
            case BinaryOperatorCode::lor:
            case BinaryOperatorCode::eq:
            case BinaryOperatorCode::ne:
            case BinaryOperatorCode::lt:
            case BinaryOperatorCode::le:
            case BinaryOperatorCode::gt:
            case BinaryOperatorCode::ge:
                return Type::boolean;
        }
        throw std::runtime_error("???");
    }

    Value evaluate(const Row & input_row) override
    {
        switch (binary_operator_code)
        {
            case BinaryOperatorCode::plus:
                return std::get<int64_t>(lhs_expression->evaluate(input_row)) + std::get<int64_t>(rhs_expression->evaluate(input_row));
            case BinaryOperatorCode::minus:
                return std::get<int64_t>(lhs_expression->evaluate(input_row)) - std::get<int64_t>(rhs_expression->evaluate(input_row));
            case BinaryOperatorCode::mul:
                return std::get<int64_t>(lhs_expression->evaluate(input_row)) * std::get<int64_t>(rhs_expression->evaluate(input_row));
            case BinaryOperatorCode::div:
                return std::get<int64_t>(lhs_expression->evaluate(input_row)) / std::get<int64_t>(rhs_expression->evaluate(input_row));
            case BinaryOperatorCode::land:
                return std::get<bool>(lhs_expression->evaluate(input_row)) && std::get<bool>(rhs_expression->evaluate(input_row));
            case BinaryOperatorCode::lor:
                return std::get<bool>(lhs_expression->evaluate(input_row)) || std::get<bool>(rhs_expression->evaluate(input_row));
            case BinaryOperatorCode::eq:
                return lhs_expression->evaluate(input_row) == rhs_expression->evaluate(input_row);
            case BinaryOperatorCode::ne:
                return lhs_expression->evaluate(input_row) != rhs_expression->evaluate(input_row);
            case BinaryOperatorCode::lt:
                return std::get<int64_t>(lhs_expression->evaluate(input_row)) < std::get<int64_t>(rhs_expression->evaluate(input_row));
            case BinaryOperatorCode::le:
                return std::get<int64_t>(lhs_expression->evaluate(input_row)) <= std::get<int64_t>(rhs_expression->evaluate(input_row));
            case BinaryOperatorCode::gt:
                return std::get<int64_t>(lhs_expression->evaluate(input_row)) > std::get<int64_t>(rhs_expression->evaluate(input_row));
            case BinaryOperatorCode::ge:
                return std::get<int64_t>(lhs_expression->evaluate(input_row)) >= std::get<int64_t>(rhs_expression->evaluate(input_row));

        }
        throw std::runtime_error("??");
    }

    const BinaryOperatorCode binary_operator_code;
    ExpressionPtr lhs_expression;
    ExpressionPtr rhs_expression;
    Type lhs_type;
    Type rhs_type;
};

class UnaryOperatorExpression : public IExpression
{
public:
    explicit UnaryOperatorExpression(UnaryOperatorCode unary_operator_code_, ExpressionPtr expression_)
        : unary_operator_code(unary_operator_code_), expression(std::move(expression_)), expression_type(expression->getResultType())
    {
    }

    Type getResultType() override { return expression->getResultType(); }

    Value evaluate(const Row & input_row) override
    {
        if (unary_operator_code == UnaryOperatorCode::lnot) {
            return !std::get<bool>(expression->evaluate(input_row));
        } else {
            return -std::get<int64_t>(expression->evaluate(input_row));
        }
        throw std::runtime_error("???");
    }

    const UnaryOperatorCode unary_operator_code;
    ExpressionPtr expression;
    Type expression_type;
};

}

ExpressionPtr buildExpression(const ASTPtr & ast, const std::shared_ptr<SchemaAccessor> & input_schema_accessor)
{
    switch (ast->type)
    {
        case ASTType::identifier: {
            const auto identifier = std::static_pointer_cast<const ASTIdentifier>(ast);
            return std::make_shared<IdentifierExpression>(identifier->getName(), input_schema_accessor);
        }
        case ASTType::literal: {
            const auto literal = std::static_pointer_cast<const ASTLiteral>(ast);
            switch (literal->literal_type)
            {
                case ASTLiteralType::number:
                    return std::make_shared<NumberConstantExpression>(literal->integer_value);
                case ASTLiteralType::string:
                    return std::make_shared<StringConstantExpression>(literal->string_value);
            }
        }
        case ASTType::binaryOperator: {
            auto binary_operator = std::static_pointer_cast<const ASTBinaryOperator>(ast);
            return std::make_shared<BinaryOperatorExpression>(binary_operator->operator_code, buildExpression(binary_operator->getLHS(), input_schema_accessor), buildExpression(binary_operator->getRHS(), input_schema_accessor));
        }
        case ASTType::unaryOperator: {
            auto unary_operator = std::static_pointer_cast<const ASTUnaryOperator>(ast);
            return std::make_shared<UnaryOperatorExpression>(unary_operator->operator_code, buildExpression(unary_operator->getOperand(), input_schema_accessor));
        }
    }
    throw std::runtime_error("???");
}

Expressions buildExpressions(const ASTs & expressions, const std::shared_ptr<SchemaAccessor> & input_schema_accessor)
{
    Expressions exprs;
    for (const auto& item : expressions) {
        exprs.push_back(buildExpression(item, input_schema_accessor));
    }
    return exprs;
}

}
