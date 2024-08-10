#include "lexer.h"

namespace shdb {

%%{
    machine lexer;
    main := |*
        'CREATE TABLE' => { ret = Parser::token::CREATE_TABLE; fbreak; };
        'DROP TABLE' => { ret = Parser::token::DROP_TABLE; fbreak; };
        'SELECT' => { ret = Parser::token::SELECT; fbreak; };
        'INSERT' => { ret = Parser::token::INSERT; fbreak; };
        'VALUES' => { ret = Parser::token::VALUES; fbreak; };
        'FROM' => { ret = Parser::token::FROM; fbreak; };
        'WHERE' => { ret = Parser::token::WHERE; fbreak; };
        'ORDER BY' => { ret = Parser::token::ORDER_BY; fbreak; };
        'DESC' => { ret = Parser::token::DESC; fbreak; };
        'GROUP BY' => { ret = Parser::token::GROUP_BY; fbreak; };
        '(' => { ret = Parser::token::LPAR; fbreak; };
        ')' => { ret = Parser::token::RPAR; fbreak; };
        ',' => { ret = Parser::token::COMMA; fbreak; };
        'boolean' => { ret = Parser::token::BOOLEAN; fbreak; };
        'uint64' => { ret = Parser::token::UINT64; fbreak; };
        'int64' => { ret = Parser::token::INT64; fbreak; };
        'varchar' => { ret = Parser::token::VARCHAR; fbreak; };
        'string' => { ret = Parser::token::STRING; fbreak; };
        '+' => { ret = Parser::token::PLUS; fbreak; };
        '-' => { ret = Parser::token::MINUS; fbreak; };
        '*' => { ret = Parser::token::MUL; fbreak; };
        '/' => { ret = Parser::token::DIV; fbreak; };
        '"' => { ret = Parser::token::QUOTE; fbreak; };
        '<' => { ret = Parser::token::LESS; fbreak; };
        '<=' => { ret = Parser::token::LQ; fbreak; };
        '>' => { ret = Parser::token::GREATER; fbreak; };
        '>=' => { ret = Parser::token::GQ; fbreak; };
        '=' => { ret = Parser::token::EQ; fbreak; };
        '<>' => { ret = Parser::token::NEQ; fbreak; };
        'AND' => { ret = Parser::token::LAND; fbreak; };
        'OR' => { ret = Parser::token::LOR; fbreak; };
        '!' => { ret = Parser::token::LNOT; fbreak; };

        digit+ => {
            ret = Parser::token::NUM;
            Parser::semantic_type num(strtol(std::string(ts, te).c_str(), 0, 10));
            val->move<int>(num);
            fbreak;
        };

        [0-9a-zA-Z_]* => {
            ret = Parser::token::NAME;
            Parser::semantic_type str(std::string(ts, te));
            val->move<std::string>(str);
            fbreak;
        };

        space;
    *|;
    write data;
}%%


Lexer::Lexer(const char *p, const char *pe)
    : p(p), pe(pe), eof(pe)
{
    %%write init;
}

Parser::token_type Lexer::lex(Parser::semantic_type* val)
{
    Parser::token_type ret = Parser::token::END;
    %%write exec;

    if (ret == Parser::token::END && p != pe && te != pe) {
        std::cerr << "Unexpected input: \"" << std::string(te, pe) << "\"" << std::endl;
        ret = Parser::token::ERROR;
    }

    return ret;
}

}
