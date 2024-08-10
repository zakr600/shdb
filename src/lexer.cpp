
#line 1 "lexer.l"
#include "lexer.h"

namespace shdb {


#line 9 "/Users/kravchukz/shad/shad-db1/shdb/src/lexer.cpp"
static const char _lexer_actions[] = {
	0, 1, 0, 1, 1, 1, 2, 1, 
	9, 1, 10, 1, 11, 1, 12, 1, 
	13, 1, 14, 1, 15, 1, 16, 1, 
	17, 2, 2, 3, 2, 2, 4, 2, 
	2, 5, 2, 2, 6, 2, 2, 7, 
	2, 2, 8
};

static const short _lexer_key_offsets[] = {
	0, 0, 1, 2, 3, 4, 5, 6, 
	7, 8, 9, 10, 27, 33, 40, 47, 
	54, 61, 68, 75, 82, 89, 96, 103, 
	110, 117, 124, 131, 138, 145, 152, 159, 
	166, 173, 180, 187, 194, 201, 208, 215, 
	222, 229, 236, 243, 250, 257, 264, 271, 
	278
};

static const char _lexer_trans_keys[] = {
	84, 65, 66, 76, 69, 84, 65, 66, 
	76, 69, 32, 40, 41, 44, 67, 68, 
	98, 105, 115, 117, 118, 9, 13, 65, 
	90, 97, 122, 48, 57, 65, 90, 97, 
	122, 82, 48, 57, 65, 90, 97, 122, 
	69, 48, 57, 65, 90, 97, 122, 65, 
	48, 57, 66, 90, 97, 122, 84, 48, 
	57, 65, 90, 97, 122, 69, 48, 57, 
	65, 90, 97, 122, 32, 48, 57, 65, 
	90, 97, 122, 82, 48, 57, 65, 90, 
	97, 122, 79, 48, 57, 65, 90, 97, 
	122, 80, 48, 57, 65, 90, 97, 122, 
	32, 48, 57, 65, 90, 97, 122, 111, 
	48, 57, 65, 90, 97, 122, 111, 48, 
	57, 65, 90, 97, 122, 108, 48, 57, 
	65, 90, 97, 122, 101, 48, 57, 65, 
	90, 97, 122, 97, 48, 57, 65, 90, 
	98, 122, 110, 48, 57, 65, 90, 97, 
	122, 110, 48, 57, 65, 90, 97, 122, 
	116, 48, 57, 65, 90, 97, 122, 54, 
	48, 57, 65, 90, 97, 122, 52, 48, 
	57, 65, 90, 97, 122, 116, 48, 57, 
	65, 90, 97, 122, 114, 48, 57, 65, 
	90, 97, 122, 105, 48, 57, 65, 90, 
	97, 122, 110, 48, 57, 65, 90, 97, 
	122, 103, 48, 57, 65, 90, 97, 122, 
	105, 48, 57, 65, 90, 97, 122, 110, 
	48, 57, 65, 90, 97, 122, 116, 48, 
	57, 65, 90, 97, 122, 54, 48, 57, 
	65, 90, 97, 122, 52, 48, 57, 65, 
	90, 97, 122, 97, 48, 57, 65, 90, 
	98, 122, 114, 48, 57, 65, 90, 97, 
	122, 99, 48, 57, 65, 90, 97, 122, 
	104, 48, 57, 65, 90, 97, 122, 97, 
	48, 57, 65, 90, 98, 122, 114, 48, 
	57, 65, 90, 97, 122, 0
};

static const char _lexer_single_lengths[] = {
	0, 1, 1, 1, 1, 1, 1, 1, 
	1, 1, 1, 11, 0, 1, 1, 1, 
	1, 1, 1, 1, 1, 1, 1, 1, 
	1, 1, 1, 1, 1, 1, 1, 1, 
	1, 1, 1, 1, 1, 1, 1, 1, 
	1, 1, 1, 1, 1, 1, 1, 1, 
	1
};

static const char _lexer_range_lengths[] = {
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 3, 3, 3, 3, 3, 
	3, 3, 3, 3, 3, 3, 3, 3, 
	3, 3, 3, 3, 3, 3, 3, 3, 
	3, 3, 3, 3, 3, 3, 3, 3, 
	3, 3, 3, 3, 3, 3, 3, 3, 
	3
};

static const short _lexer_index_offsets[] = {
	0, 0, 2, 4, 6, 8, 10, 12, 
	14, 16, 18, 20, 35, 39, 44, 49, 
	54, 59, 64, 69, 74, 79, 84, 89, 
	94, 99, 104, 109, 114, 119, 124, 129, 
	134, 139, 144, 149, 154, 159, 164, 169, 
	174, 179, 184, 189, 194, 199, 204, 209, 
	214
};

static const char _lexer_indicies[] = {
	1, 0, 2, 0, 3, 0, 4, 0, 
	5, 0, 6, 0, 7, 0, 8, 0, 
	9, 0, 10, 0, 11, 13, 14, 15, 
	17, 18, 19, 20, 21, 22, 23, 11, 
	16, 16, 12, 16, 16, 16, 24, 26, 
	16, 16, 16, 25, 27, 16, 16, 16, 
	25, 28, 16, 16, 16, 25, 29, 16, 
	16, 16, 25, 30, 16, 16, 16, 25, 
	31, 16, 16, 16, 25, 32, 16, 16, 
	16, 25, 33, 16, 16, 16, 25, 34, 
	16, 16, 16, 25, 35, 16, 16, 16, 
	25, 36, 16, 16, 16, 25, 37, 16, 
	16, 16, 25, 38, 16, 16, 16, 25, 
	39, 16, 16, 16, 25, 40, 16, 16, 
	16, 25, 41, 16, 16, 16, 25, 42, 
	16, 16, 16, 25, 43, 16, 16, 16, 
	25, 44, 16, 16, 16, 25, 45, 16, 
	16, 16, 25, 46, 16, 16, 16, 25, 
	47, 16, 16, 16, 25, 48, 16, 16, 
	16, 25, 49, 16, 16, 16, 25, 50, 
	16, 16, 16, 25, 51, 16, 16, 16, 
	25, 52, 16, 16, 16, 25, 53, 16, 
	16, 16, 25, 54, 16, 16, 16, 25, 
	55, 16, 16, 16, 25, 56, 16, 16, 
	16, 25, 57, 16, 16, 16, 25, 58, 
	16, 16, 16, 25, 59, 16, 16, 16, 
	25, 60, 16, 16, 16, 25, 61, 16, 
	16, 16, 25, 0
};

static const char _lexer_trans_targs[] = {
	11, 2, 3, 4, 5, 11, 7, 8, 
	9, 10, 11, 11, 0, 11, 11, 11, 
	12, 13, 19, 23, 29, 33, 38, 43, 
	11, 11, 14, 15, 16, 17, 18, 1, 
	20, 21, 22, 6, 24, 25, 26, 27, 
	28, 12, 30, 31, 32, 12, 34, 35, 
	36, 37, 12, 39, 40, 41, 42, 12, 
	44, 45, 46, 47, 48, 12
};

static const char _lexer_trans_actions[] = {
	21, 0, 0, 0, 0, 7, 0, 0, 
	0, 0, 9, 17, 0, 11, 13, 15, 
	40, 0, 0, 0, 0, 0, 0, 0, 
	23, 19, 0, 0, 0, 0, 5, 0, 
	0, 0, 5, 0, 0, 0, 0, 0, 
	0, 25, 0, 0, 0, 31, 0, 0, 
	0, 0, 37, 0, 0, 0, 0, 28, 
	0, 0, 0, 0, 0, 34
};

static const char _lexer_to_state_actions[] = {
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 1, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0
};

static const char _lexer_from_state_actions[] = {
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 3, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0
};

static const short _lexer_eof_trans[] = {
	0, 1, 1, 1, 1, 1, 1, 1, 
	1, 1, 1, 0, 25, 26, 26, 26, 
	26, 26, 26, 26, 26, 26, 26, 26, 
	26, 26, 26, 26, 26, 26, 26, 26, 
	26, 26, 26, 26, 26, 26, 26, 26, 
	26, 26, 26, 26, 26, 26, 26, 26, 
	26
};

static const int lexer_start = 11;
static const int lexer_first_final = 11;
static const int lexer_error = 0;

static const int lexer_en_main = 11;


#line 29 "lexer.l"



Lexer::Lexer(const char *p, const char *pe)
    : p(p), pe(pe), eof(pe)
{
    
#line 196 "/Users/kravchukz/shad/shad-db1/shdb/src/lexer.cpp"
	{
	cs = lexer_start;
	ts = 0;
	te = 0;
	act = 0;
	}

#line 36 "lexer.l"
}

Parser::token_type Lexer::lex(Parser::semantic_type* val)
{
    Parser::token_type ret = Parser::token::END;
    
#line 211 "/Users/kravchukz/shad/shad-db1/shdb/src/lexer.cpp"
	{
	int _klen;
	unsigned int _trans;
	const char *_acts;
	unsigned int _nacts;
	const char *_keys;

	if ( p == pe )
		goto _test_eof;
	if ( cs == 0 )
		goto _out;
_resume:
	_acts = _lexer_actions + _lexer_from_state_actions[cs];
	_nacts = (unsigned int) *_acts++;
	while ( _nacts-- > 0 ) {
		switch ( *_acts++ ) {
	case 1:
#line 1 "NONE"
	{ts = p;}
	break;
#line 232 "/Users/kravchukz/shad/shad-db1/shdb/src/lexer.cpp"
		}
	}

	_keys = _lexer_trans_keys + _lexer_key_offsets[cs];
	_trans = _lexer_index_offsets[cs];

	_klen = _lexer_single_lengths[cs];
	if ( _klen > 0 ) {
		const char *_lower = _keys;
		const char *_mid;
		const char *_upper = _keys + _klen - 1;
		while (1) {
			if ( _upper < _lower )
				break;

			_mid = _lower + ((_upper-_lower) >> 1);
			if ( (*p) < *_mid )
				_upper = _mid - 1;
			else if ( (*p) > *_mid )
				_lower = _mid + 1;
			else {
				_trans += (unsigned int)(_mid - _keys);
				goto _match;
			}
		}
		_keys += _klen;
		_trans += _klen;
	}

	_klen = _lexer_range_lengths[cs];
	if ( _klen > 0 ) {
		const char *_lower = _keys;
		const char *_mid;
		const char *_upper = _keys + (_klen<<1) - 2;
		while (1) {
			if ( _upper < _lower )
				break;

			_mid = _lower + (((_upper-_lower) >> 1) & ~1);
			if ( (*p) < _mid[0] )
				_upper = _mid - 2;
			else if ( (*p) > _mid[1] )
				_lower = _mid + 2;
			else {
				_trans += (unsigned int)((_mid - _keys)>>1);
				goto _match;
			}
		}
		_trans += _klen;
	}

_match:
	_trans = _lexer_indicies[_trans];
_eof_trans:
	cs = _lexer_trans_targs[_trans];

	if ( _lexer_trans_actions[_trans] == 0 )
		goto _again;

	_acts = _lexer_actions + _lexer_trans_actions[_trans];
	_nacts = (unsigned int) *_acts++;
	while ( _nacts-- > 0 )
	{
		switch ( *_acts++ )
		{
	case 2:
#line 1 "NONE"
	{te = p+1;}
	break;
	case 3:
#line 13 "lexer.l"
	{act = 6;}
	break;
	case 4:
#line 14 "lexer.l"
	{act = 7;}
	break;
	case 5:
#line 15 "lexer.l"
	{act = 8;}
	break;
	case 6:
#line 16 "lexer.l"
	{act = 9;}
	break;
	case 7:
#line 17 "lexer.l"
	{act = 10;}
	break;
	case 8:
#line 19 "lexer.l"
	{act = 11;}
	break;
	case 9:
#line 8 "lexer.l"
	{te = p+1;{ ret = Parser::token::CREATE_TABLE; {p++; goto _out; } }}
	break;
	case 10:
#line 9 "lexer.l"
	{te = p+1;{ ret = Parser::token::DROP_TABLE; {p++; goto _out; } }}
	break;
	case 11:
#line 10 "lexer.l"
	{te = p+1;{ ret = Parser::token::LPAR; {p++; goto _out; } }}
	break;
	case 12:
#line 11 "lexer.l"
	{te = p+1;{ ret = Parser::token::RPAR; {p++; goto _out; } }}
	break;
	case 13:
#line 12 "lexer.l"
	{te = p+1;{ ret = Parser::token::COMMA; {p++; goto _out; } }}
	break;
	case 14:
#line 26 "lexer.l"
	{te = p+1;}
	break;
	case 15:
#line 19 "lexer.l"
	{te = p;p--;{
            ret = Parser::token::NAME;
            Parser::semantic_type str(std::string(ts, te));
            val->move<std::string>(str);
            {p++; goto _out; }
        }}
	break;
	case 16:
#line 19 "lexer.l"
	{{p = ((te))-1;}{
            ret = Parser::token::NAME;
            Parser::semantic_type str(std::string(ts, te));
            val->move<std::string>(str);
            {p++; goto _out; }
        }}
	break;
	case 17:
#line 1 "NONE"
	{	switch( act ) {
	case 6:
	{{p = ((te))-1;} ret = Parser::token::BOOLEAN; {p++; goto _out; } }
	break;
	case 7:
	{{p = ((te))-1;} ret = Parser::token::UINT64; {p++; goto _out; } }
	break;
	case 8:
	{{p = ((te))-1;} ret = Parser::token::INT64; {p++; goto _out; } }
	break;
	case 9:
	{{p = ((te))-1;} ret = Parser::token::VARCHAR; {p++; goto _out; } }
	break;
	case 10:
	{{p = ((te))-1;} ret = Parser::token::STRING; {p++; goto _out; } }
	break;
	case 11:
	{{p = ((te))-1;}
            ret = Parser::token::NAME;
            Parser::semantic_type str(std::string(ts, te));
            val->move<std::string>(str);
            {p++; goto _out; }
        }
	break;
	}
	}
	break;
#line 397 "/Users/kravchukz/shad/shad-db1/shdb/src/lexer.cpp"
		}
	}

_again:
	_acts = _lexer_actions + _lexer_to_state_actions[cs];
	_nacts = (unsigned int) *_acts++;
	while ( _nacts-- > 0 ) {
		switch ( *_acts++ ) {
	case 0:
#line 1 "NONE"
	{ts = 0;}
	break;
#line 410 "/Users/kravchukz/shad/shad-db1/shdb/src/lexer.cpp"
		}
	}

	if ( cs == 0 )
		goto _out;
	if ( ++p != pe )
		goto _resume;
	_test_eof: {}
	if ( p == eof )
	{
	if ( _lexer_eof_trans[cs] > 0 ) {
		_trans = _lexer_eof_trans[cs] - 1;
		goto _eof_trans;
	}
	}

	_out: {}
	}

#line 42 "lexer.l"

    if (ret == Parser::token::END && p != pe && te != pe) {
        std::cerr << "Unexpected input: \"" << std::string(te, pe) << "\"" << std::endl;
        ret = Parser::token::ERROR;
    }

    return ret;
}

}
