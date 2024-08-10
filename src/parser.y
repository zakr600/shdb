%skeleton "lalr1.cc"
%require "2.5"
%defines
%define api.namespace {shdb}
%define api.value.type variant
%define parser_class_name {Parser}

%code requires {
    #include <memory>
    #include "ast.h"
    #include "schema.h"
    namespace shdb {class Lexer;}
}

%parse-param {shdb::Lexer& lexer} {ASTPtr & result} {std::string & message}

%code {
    #include "lexer.h"
    #define yylex lexer.lex
}

%token END 0 "end of file"
%token ERROR
%token CREATE_TABLE "CREATE TABLE"
%token DROP_TABLE "DROP TABLE"
%token SELECT "SELECT"
%token INSERT "INSERT"
%token VALUES "VALUES"
%token FROM "FROM"
%token WHERE "WHERE"
%token ORDER_BY "ORDER BY"
%token DESC "DESC"
%token GROUP_BY "GROUP BY"
%token LPAR "("
%token RPAR ")"
%token COMMA ","
%token BOOLEAN "boolean"
%token UINT64 "uint64"
%token INT64 "int64"
%token VARCHAR "varchar"
%token STRING "string"
%token PLUS "+"
%token MINUS "-"
%token MUL "*"
%token DIV "/"
%token QUOTE "\""
%token EQ "="
%token NEQ "<>"
%token LESS "<"
%token LQ "<="
%token GREATER ">"
%token GQ ">="
%token LAND "AND"
%token LOR "OR"
%token LNOT "!"

%token <std::string> NAME
%token <int> NUM

%type <std::shared_ptr<IAST>> command
%type <Schema> schemaParams
%type <ColumnSchema> schemaParam
%type <std::shared_ptr<IAST>> expr
%type <std::shared_ptr<ASTList>> exprs
%type <std::vector<std::string>> names
%type <std::shared_ptr<IAST>> where_expr
%type <std::vector<std::string>> from_expr
%type <std::shared_ptr<ASTList>> order_by_expr
%type <std::shared_ptr<IAST>> order_expr
%type <std::shared_ptr<ASTList>> order_exprs

%left "=" "<>" "<" "<=" ">" ">="
%left "+" "-"
%left "*" "/"
%nonassoc UMINUS
%left "AND"
%left "OR"
%nonassoc "!"

%%

input: command { result = $1; }

command:
    "CREATE TABLE" NAME "(" schemaParams ")" { $$ = newCreateQuery($2, $4); }
    | "DROP TABLE" NAME { $$ = newDropQuery($2); }
    | "SELECT" exprs from_expr where_expr order_by_expr {{ $$ = newSelectQuery($2, $3, $4, nullptr, nullptr, $5); }}
    | "INSERT" NAME "VALUES" "(" exprs ")" {{ $$ = newInsertQuery($2, $5); }}

from_expr: {{ $$ = {}; }}
    | "FROM" names {{ $$ = $2; }}

where_expr: {{ $$ = nullptr; }}
    | "WHERE" expr {{ $$ = $2; }}

order_by_expr: {{ $$ = nullptr; }}
    | "ORDER BY" order_exprs {{ $$ = $2; }}

names: NAME {{ $$ = std::vector<std::string>{$1}; }}
    | names "," NAME {{ $1.push_back($3); $$ = std::move($1); }}

order_expr: expr {{ $$ = newOrder($1, false); }}
    | expr "DESC" {{ $$ = newOrder($1, true); }}

order_exprs: order_expr {{ $$ = newList($1); }}
    | order_exprs "," order_expr {{ $1->append($3); $$ = std::move($1); }}

expr: NUM {{ $$ = newNumberLiteral($1); }}
    | "\"" NAME "\"" {{ $$ = newStringLiteral($2); }}
    | NAME {{ $$ = newIdentifier($1); }}
    | "(" expr ")" { $$ = $2; }
    | expr "=" expr {{ $$ = newBinaryOperator(BinaryOperatorCode::eq, $1, $3); }}
    | expr "<>" expr {{ $$ = newBinaryOperator(BinaryOperatorCode::ne, $1, $3); }}
    | expr "<" expr {{ $$ = newBinaryOperator(BinaryOperatorCode::lt, $1, $3); }}
    | expr "<=" expr {{ $$ = newBinaryOperator(BinaryOperatorCode::le, $1, $3); }}
    | expr ">" expr {{ $$ = newBinaryOperator(BinaryOperatorCode::gt, $1, $3); }}
    | expr ">=" expr {{ $$ = newBinaryOperator(BinaryOperatorCode::ge, $1, $3); }}
    | expr "AND" expr {{ $$ = newBinaryOperator(BinaryOperatorCode::land, $1, $3); }}
    | expr "OR" expr {{ $$ = newBinaryOperator(BinaryOperatorCode::lor, $1, $3); }}
    | expr "+" expr {{ $$ = newBinaryOperator(BinaryOperatorCode::plus, $1, $3); }}
    | expr "-" expr {{ $$ = newBinaryOperator(BinaryOperatorCode::minus, $1, $3); }}
    | expr "*" expr {{ $$ = newBinaryOperator(BinaryOperatorCode::mul, $1, $3); }}
    | expr "/" expr {{ $$ = newBinaryOperator(BinaryOperatorCode::div, $1, $3); }}
    | "!" expr {{ $$ = newUnaryOperator(UnaryOperatorCode::lnot, $2); }}
    | "-" expr {{ $$ = newUnaryOperator(UnaryOperatorCode::uminus, $2); }}

exprs : expr {{ $$ = newList($1); }}
    | exprs "," expr {{ $1->append($3); $$ = std::move($1); }}
    | "*" {{ $$ = nullptr; }}

schemaParam:
    NAME BOOLEAN { $$ = ColumnSchema($1, Type::boolean); }
    | NAME UINT64 { $$ = ColumnSchema($1, Type::uint64); }
    | NAME INT64 { $$ = ColumnSchema($1, Type::int64); }
    | NAME VARCHAR "(" NUM ")" { $$ = ColumnSchema($1, Type::varchar, $4); }
    | NAME STRING { $$ = ColumnSchema($1, Type::string); }

schemaParams : schemaParam { $$ = Schema{$1}; }
    | schemaParams "," schemaParam { $1.push_back($3); $$ = std::move($1); }
;
%%

void shdb::Parser::error(const std::string& err)
{
	message = err;
}
