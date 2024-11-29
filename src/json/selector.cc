#include "json/selector.h"
#include "json/util.h"
#include "json/json.h"
#include "json/rapidjson_includes.h"
#include <rapidjson/pointer.h>
#include <iostream>
#include <cstring>
#include <algorithm>
#include <iterator>

#ifdef INSTRUMENT_V2PATH
#define TRACE(level, msg) \
std::cout << level << " " << msg << std::endl;
#else
#define TRACE(level, msg)
#endif

#define ENABLE_V2_SYNTAX    1

static const char DOUBLE_QUOTE = '"';
static const char SINGLE_QUOTE = '\'';

typedef rapidjson::GenericPointer<RJValue, RapidJsonAllocator> RJPointer;

struct JPointer : RJPointer {
    explicit JPointer(const jsn::string &path) : RJPointer(&allocator) {
        *static_cast<RJPointer *>(this) = RJPointer(path.c_str(), path.length(), &allocator);
        if (!IsValid()) error = JSONUTIL_INVALID_JSON_PATH;
    }
    bool HasError() { return error != JSONUTIL_SUCCESS; }
    bool PathExists(JValue& doc) { return Get(doc) != nullptr; }

    // Reexport
    using RJPointer::Erase;
    using RJPointer::Get;

    JsonUtilCode error = JSONUTIL_SUCCESS;
};

thread_local int64_t current_depth = 0;  // parser's recursion depth

class RecursionDepthTracker {
 public:
    RecursionDepthTracker() {
        current_depth++;
    }
    ~RecursionDepthTracker() {
        current_depth--;
    }
    bool isTooDeep() { return current_depth > static_cast<int64_t>(json_get_max_parser_recursion_depth()); }
};

#define CHECK_RECURSION_DEPTH() \
    RecursionDepthTracker _rdtracker; \
    if (_rdtracker.isTooDeep()) return JSONUTIL_PARSER_RECURSION_DEPTH_LIMIT_EXCEEDED;

#define CHECK_RECURSIVE_DESCENT_TOKENS() \
    if (lex.getRecursiveDescentTokens() > json_get_max_recursive_descent_tokens()) \
        return JSONUTIL_RECURSIVE_DESCENT_TOKEN_LIMIT_EXCEEDED;

#define CHECK_QUERY_STRING_SIZE(path) \
    if (strlen(path) > json_get_max_query_string_size()) return JSONUTIL_QUERY_STRING_SIZE_LIMIT_EXCEEDED;

/**
 * EBNF Grammar of JSONPath:
 *   SupportedPath       ::= ["$" | "."] RelativePath
 *   RelativePath        ::= empty | RecursivePath | DotPath | BracketPath | QualifiedPath
 *   RecursivePath       ::= ".." SupportedPath
 *   DotPath             ::= "." QualifiedPath
 *   QualifiedPath       ::= QualifiedPathElement RelativePath
 *   QualifiedPathElement ::= Key | BracketPathElement
 *   Key                 ::= "*" [ [ "." ] WildcardFilter ] | UnquotedMemberName
 *   WildcardFilter      ::=  "[" "?" "(" FilterExpr ")" "]"
 *   UnquotedMemberName  ::= char { char }
 *   BracketPath         ::= BracketPathElement [ RelativePath ]
 *   BracketPathElement  ::= "[" {SPACE} ( WildcardInBrackets | ((NameInBrackets | IndexExpr) ) {SPACE} "]")
 *   WildcardInBrackets  ::= "*" {SPACE} "]" [ "[" {SPACE} "?" "(" FilterExpr ")" {SPACE} "]" ]
 *   NameInBrackets      ::= QuotedMemberName [ ({SPACE} "," {SPACE} QuotedMemberName)+ ]
 *   QuotedMemberName    ::= "\"" {char} "\"" | "'" {char} "'"
 *   IndexExpr           ::= Filter | SliceStartsWithColon | SliceOrUnionOrIndex
 *   SliceStartsWithColon ::= {SPACE} ":" {SPACE} [ ":" {SPACE} [Step] | EndAndStep ] ]
 *   EndAndStep          ::= End [{SPACE} ":" {SPACE} [Step]] ]
 *   SliceOrUnionOrIndex ::= SliceStartsWithInteger | Index | UnionOfIndexes
 *   SliceStartsWithInteger ::= Start {SPACE} ":" {SPACE} [ ":" {SPACE} [Step] | EndAndStep
 *   Index               ::= Integer
 *   Integer             ::= ["+" | "-"] digit {digit}
 *   Start               ::= Integer
 *   End                 ::= Integer
 *   Step                ::= Integer
 *   UnionOfIndexes      ::= Integer ({SPACE} "," {SPACE} Integer)+
 *   Filter              ::= "?" "(" FilterExpr ")"
 *   FilterExpr          ::= {SPACE} Term { {SPACE} "||" {SPACE} Term {SPACE} }
 *   Term                ::= Factor { {SPACE} "&&" {SPACE} Factor }
 *   Factor              ::= ( "@" ( MemberName | ( [ MemberName ] ComparisonOp ComparisonValue) ) ) |
 *                           ( ComparisonValue ComparisonOp "@" ( MemberName | ( [ MemberName ]) ) ) |
 *                           ( {SPACE} "(" FilterExpr ")" {SPACE} )
 *   MemberName          ::= ("." (UnquotedMemberName | BracketedMemberName)) | BracketedMemberName
 *   BracketedMemberName ::= "[" {SPACE} QuotedMemberName {SPACE} "]"
 *   ComparisonOp        ::= {SPACE} "<" | "<="] | ">" | ">=" | "==" | "!=" {SPACE}
 *   ComparisonValue     ::= "null" | Bool | Number | QuotedString | PartialPath
 *   Bool                ::= "true" | "false"
 *   Number              ::= Integer | MemberNameInFilter | ScientificNumber
 *   QuotedString        ::= "\"" {char} "\""
 *   PartialPath         ::= "$" RelativePath
 *   SPACE               ::= ' '
 */

void Lexer::init(const char *path) {
    p = path;
    this->path = path;
    next.type = Token::UNKNOWN;
}

Token::TokenType Lexer::peekToken() const {
    switch (*p) {
        case '\0': return Token::END;
        case '$': return Token::DOLLAR;
        case '.': {
            if (*(p+1) == '.')
                return Token::DOTDOT;
            else
                return Token::DOT;
        }
        case '*': return Token::WILDCARD;
        case ':': return Token::COLON;
        case ',': return Token::COMMA;
        case '?': return Token::QUESTION_MARK;
        case '@': return Token::AT;
        case '[': return Token::LBRACKET;
        case ']': return Token::RBRACKET;
        case '(': return Token::LPAREN;
        case ')': return Token::RPAREN;
        case '\'': return Token::SINGLE_QUOTE;
        case '"': return Token::DOUBLE_QUOTE;
        case '+': return Token::PLUS;
        case '-': return Token::MINUS;
        case '/': return Token::DIV;
        case '%': return Token::PCT;
        case ' ': return Token::SPACE;
        case '&': {
            if (*(p+1) == '&')
                return Token::AND;
            else
                return Token::SPECIAL_CHAR;
        }
        case '|': {
            if (*(p+1) == '|')
                return Token::OR;
            else
                return Token::SPECIAL_CHAR;
        }
        case '=': {
            if (*(p+1) == '=')
                return Token::EQ;
            else
                return Token::ASSIGN;
        }
        case '!': {
            if (*(p+1) == '=')
                return Token::NE;
            else
                return Token::NOT;
        }
        case '>': {
            if (*(p+1) == '=')
                return Token::GE;
            else
                return Token::GT;
        }
        case '<': {
            if (*(p+1) == '=')
                return Token::LE;
            else
                return Token::LT;
        }
        default: {
            if (std::isdigit(*p)) {
                return Token::DIGIT;
            } else if (std::isalpha(*p)) {
                return Token::ALPHA;
            } else {
                TRACE("DEBUG", "peekToken special char: " << *p)
                return Token::SPECIAL_CHAR;
            }
        }
    }
}

/**
 * Scan the next token.
 * @return next token
 */
Token Lexer::nextToken(const bool skipSpace) {
    next.type = peekToken();
    switch (next.type) {
        case Token::END: return next;
        case Token::DOTDOT:
        {
            rdTokens++;
            next.strVal = std::string_view(p, 2);
            p++;
            p++;
            break;
        }
        case Token::NE:
        case Token::GE:
        case Token::LE:
        case Token::EQ:
        case Token::AND:
        case Token::OR:
        {
            next.strVal = std::string_view(p, 2);
            p++;
            p++;
            break;
        }
        case Token::DIGIT:
        case Token::ALPHA:
        case Token::SPECIAL_CHAR:
        {
            next.strVal = std::string_view(p, 1);
            p++;
            break;
        }
        case Token::SPACE:
        {
            if (skipSpace) {
                while (*p == ' ') p++;
                return nextToken();
            } else {
                next.strVal = std::string_view(p, 1);
                p++;
                break;
            }
        }
        default:
            next.strVal = std::string_view(p, 1);
            p++;
            break;
    }
    return next;
}

/**
 * If current token matches the given token type, advance to the next token and return true.
 * Otherwise, return false.
 */
bool Lexer::matchToken(const Token::TokenType type, const bool skipSpace) {
    if (skipSpace && next.type == Token::SPACE) {
        while (*p == ' ') p++;
        nextToken();
        return matchToken(type);
    }

    if (next.type == type) {
        nextToken(skipSpace);
        return true;
    }
    return false;
}

/**
 * Scan an integer. An integer is made of the following characters: [0-9]+-.
 */
JsonUtilCode Lexer::scanInteger(int64_t &val) {
    val = 0;
    if (next.type != Token::DIGIT && next.type != Token::PLUS && next.type != Token::MINUS)
        return JSONUTIL_VALUE_NOT_NUMBER;

    if (next.type == Token::DIGIT) {
        val = scanUnsignedInteger();
    } else {
        int sign = (next.type == Token::PLUS? 1 : -1);
        nextToken();  // skip the PLUS/MINUS sign symbol
        if (next.type != Token::DIGIT) return JSONUTIL_VALUE_NOT_NUMBER;
        val = sign * scanUnsignedInteger();
    }
    nextToken();  // advance to the next token
    return JSONUTIL_SUCCESS;
}

int64_t Lexer::scanUnsignedInteger() {
    ValkeyModule_Assert(next.type == Token::DIGIT);
    int64_t val = *next.strVal.data() - '0';
    while (*p != '\0' && std::isdigit(*p)) {
        val = val * 10 + (*p - '0');
        p++;
    }
    TRACE("DEBUG", "scanUnsignedInteger(): " << val)
    return val;
}

/**
 * Scan unquoted object member name, which can contain any symbol except terminator characters.
 */
JsonUtilCode Lexer::scanUnquotedMemberName(StringViewHelper &member_name) {
    // Check if the first character is a member name terminator char
    static const char *unquotedMemberNameTerminators = ".[]()<>=!'\" |&";
    const char *p_start = next.strVal.data();
    if (strchr(unquotedMemberNameTerminators, *p_start) != nullptr) {
        TRACE("ERROR", "scanUnquotedMemberName invalid first char of an expected member name: " << p_start)
        return JSONUTIL_INVALID_MEMBER_NAME;
    }
    size_t len = 1;

    // Scan the remaining path for the first occurrence of any terminator char
    size_t length = strcspn(p, unquotedMemberNameTerminators);
    len += length;
    p += length;

    member_name.setExternalView(std::string_view(p_start, len));
    TRACE("DEBUG", "scanUnquotedMemberName token type: " << next.type << ", token val: "
        << next.strVal << ", name: " << member_name.getView())
    nextToken();  // advance to the next token
    return JSONUTIL_SUCCESS;
}

/**
 * Scan number in filter expression. A number is made of the following characters: [0-9]+-.Ee.
 */
JsonUtilCode Lexer::scanNumberInFilterExpr(StringViewHelper &number_sv) {
    // Check if the first character is a valid number character
    static const char *validNumberChars = "+-0123456789.Ee";
    const char *p_start = next.strVal.data();
    if (strchr(validNumberChars, *p_start) == nullptr) {
        TRACE("ERROR", "scanNumberInFilterExpr invalid first char of an expected number: " << p_start)
        return JSONUTIL_INVALID_NUMBER;
    }
    size_t len = 1;

    // Scan the remaining path for the prefix that consists entirely of valid number characters
    size_t length = strspn(p, validNumberChars);
    len += length;
    p += length;

    number_sv.setExternalView(std::string_view(p_start, len));
    TRACE("DEBUG", "scanNumberInFilterExpr number: " << number_sv.getView())
    nextToken();  // advance to the next token
    return JSONUTIL_SUCCESS;
}

/**
 * Scan an identifier that is an alphanumeric string.
 */
JsonUtilCode Lexer::scanIdentifier(StringViewHelper &sv) {
    // Check if the first character is alphanumeric
    const char *p_start = next.strVal.data();
    if (!std::isalnum(*p_start)) return JSONUTIL_INVALID_IDENTIFIER;
    size_t len = 1;

    // Scan the remaining path for the alphanumeric characters
    while (*p != '\0' && std::isalnum(*p)) {
        p++;
        len++;
    }
    sv.setExternalView(std::string_view(p_start, len));
    TRACE("DEBUG", "scanIdentifier identifier: " << sv.getView())
    nextToken();  // advance to the next token
    return JSONUTIL_SUCCESS;
}

/**
 * Skip whitespaces including the current token.
 */
void Lexer::skipSpaces() {
    if (next.type == Token::SPACE) {
        nextToken(true);
    } else {
        while (*p == ' ') p++;
    }
}

/**
 * Scan a path value to be fed into a selector
 */
JsonUtilCode Lexer::scanPathValue(StringViewHelper &output) {
    static const char *terminators = "]()<>=!'\" |&";
    static const char *numerics = "-+0123456789";
    static const char *quotes = "\"'";
    char current_quote = '"';
    bool in_brackets = false;
    bool in_quotes = false;
    bool scanning = true;

    const char *p_start = next.strVal.data();  // leading $
    ValkeyModule_Assert(*p_start == '$');
    size_t len = 1;

    // We only check for terminators when we are outside of brackets (and unquoted)
    // When we are inside of brackets, we check for numerics (digits or -+) or quoted values
    // We track which type of quote we are using with current_quote
    while (scanning && *p != '\0') {
        if (!in_brackets) {  // can't be in quotes without being in brackets first
            if (*p == '[') {
                in_brackets = true;
                p++;
                len++;
            } else if (strchr(terminators, *p) != nullptr) {
                scanning = false;
            } else {
                p++;
                len++;
            }
        } else {
            if (!in_quotes) {
                if (strchr(quotes, *p) != nullptr) {
                    in_quotes = true;
                    current_quote = *p;
                    p++;
                    len++;
                } else if (strchr(numerics, *p) != nullptr) {
                    p++;
                    len++;
                } else if (*p == ']') {
                    p++;
                    len++;
                    in_brackets = false;
                } else {
                    return JSONUTIL_INVALID_JSON_PATH;
                }
            } else {
                if (*p == '\\' && *(p+1) == current_quote) {
                    p++;
                    len++;
                } else if (*p == current_quote) {
                    in_quotes = false;
                }
                p++;
                len++;
            }
        }
    }

    output.setExternalView(std::string_view(p_start, len));

    nextToken();  // advance to the next token
    return JSONUTIL_SUCCESS;
}

/**
 * Scan double quoted string that may contain escaped characters.
*/
JsonUtilCode Lexer::scanDoubleQuotedString(JParser& parser) {
    const char *p_start = next.strVal.data();
    size_t len = 1;
    ValkeyModule_Assert(*p_start == DOUBLE_QUOTE);
    TRACE("DEBUG", "scanDoubleQuotedString *p_start: " << *p_start << ", p: " << p)

    const char *prev = nullptr;
    while (*p != '\0') {
        if (*p == DOUBLE_QUOTE && (prev == nullptr || *prev != '\\')) {
            // reached the end quote
            p++;
            len++;
            break;
        }
        prev = p;
        p++;
        len++;
    }
    std::string_view name = std::string_view(p_start, len);

    // unescape the content using JParser
    if (parser.Parse(name).HasParseError()) {
        TRACE("ERROR", "scanDoubleQuotedString failed to parse " << name)
        return parser.GetParseErrorCode();
    }
    TRACE("DEBUG", "scanDoubleQuotedString before unescape: " << name << ", after unescape: "
    << parser.GetJValue().GetStringView())

    nextToken();  // advance to the next token
    return JSONUTIL_SUCCESS;
}

/**
 * Scan double quoted string that may contain escaped characters.
*/
JsonUtilCode Lexer::scanDoubleQuotedString(jsn::stringstream &ss) {
    JParser parser;
    JsonUtilCode rc = scanDoubleQuotedString(parser);
    if (rc != JSONUTIL_SUCCESS) return rc;
    ss << parser.GetJValue().GetStringView();
    return JSONUTIL_SUCCESS;
}

JsonUtilCode Lexer::scanSingleQuotedStringAndConvertToDoubleQuotedString(jsn::stringstream &ss) {
    const char *p_start = p;
    size_t len = 0;

    const char *prev = nullptr;
    while (*p != '\0') {
        if (*p == SINGLE_QUOTE && (prev == nullptr || *prev != '\\')) {
            // reached the end quote
            p++;
            break;
        }
        prev = p;
        p++;
        len++;
    }
    // the string view does not include begin and end single quote
    std::string_view sv = std::string_view(p_start, len);
    ss << "\"";
    for (jsn::string::size_type i = 0; i < sv.length(); ++i) {
        switch (sv[i]) {
            case '"': {
                ss << "\\\"";
                break;
            }
            case '\\': {  // unescape single quotes
                // Since the underlying string must end with a null terminator, we can safely access
                // the next character at index i+1.
                if (sv[i+1] != '\'') {
                    ss << sv[i];
                }
                break;
            }
            default:
                ss << sv[i];
                break;
        }
    }
    ss << "\"";

    nextToken();  // advance to the next token
    return JSONUTIL_SUCCESS;
}

/**
 * Scan single quoted string that may contain escaped characters.
 */
JsonUtilCode Lexer::scanSingleQuotedString(jsn::stringstream &ss) {
    const char *p_start = p;
    size_t len = 0;

    bool escaped = false;
    const char *prev = nullptr;
    while (*p != '\0') {
        if (*p == '\\') escaped = true;
        if (*p == SINGLE_QUOTE && (prev == nullptr || *prev != '\\')) {
            // reached the end quote
            p++;
            break;
        }
        prev = p;
        p++;
        len++;
    }
    // the string view does not include begin and end single quote
    std::string_view name = std::string_view(p_start, len);

    if (escaped) {
        unescape(name, ss);
        TRACE("DEBUG", "scanSingleQuotedString before unescape: " << name << ", after unescape: "
        << ss.str())
    } else {
        ss << name;
        TRACE("DEBUG", "scanSingleQuotedString name: " << ss.str())
    }

    nextToken();  // advance to the next token
    return JSONUTIL_SUCCESS;
}

/**
 * A helper function to unescape escaped control characters. It is only used for processing single
 * quoted strings. The string view handed down does not contain begin and end single quote.
 *
 * For double quoted strings, JParser::parse is used to read escaped characters. See scanDoubleQuotedString.
 *
 * @param input string view excluding begin and end quote
 * @param ss output string stream
 */
void Lexer::unescape(const std::string_view &input, jsn::stringstream &ss) {
    static const char *ctrlChar_2ndPart = "\\tbfnr'";
    static const char *ctrlChars = "\\\t\b\f\n\r\'\0";  // internal representation of control characters
    for (jsn::string::size_type i = 0; i < input.length(); ++i) {
        switch (input[i]) {
            case '\\': {
                if (i == input.length() - 1) {
                    // reached the end of the input
                    ss << input[i];
                } else {
                    // check if the next char is an escaped control character
                    const char *ptr = strchr(ctrlChar_2ndPart, input[i+1]);
                    if (ptr != nullptr) {
                        i++;  // skip the backslash, which is used to escape the next character
                        // output the internal representation of the control character
                        ss << ctrlChars[ptr - ctrlChar_2ndPart];
                    } else {
                        // This blackslash does not represent an escaped control character.
                        ss << input[i];
                    }
                }
                break;
            }
            default:
                ss << input[i];
                break;
        }
    }
    TRACE("DEBUG", "unescape before unescape: " << input << ", after unescape: " << ss.str())
}

JsonUtilCode Selector::getValues(JValue &root, const char *path) {
    JsonUtilCode rc = init(root, path, READ);
    if (rc != JSONUTIL_SUCCESS) return rc;
    return eval();
}

struct pathCompare {
 public:
    bool operator() (const jsn::string& path1, const jsn::string& path2) const {
        // compare path depth
        JPointer ptr1 = JPointer(path1);
        JPointer ptr2 = JPointer(path2);
        size_t depth1 = ptr1.GetTokenCount();
        size_t depth2 = ptr2.GetTokenCount();
        if (depth1 != depth2) return depth1 > depth2;

        const JPointer::Token* tokenArray1 = ptr1.GetTokens();
        const JPointer::Token* tokenArray2 = ptr2.GetTokens();
        bool areBothLeavesIndex = (tokenArray1[depth1 - 1].index != rapidjson::kPointerInvalidIndex &&
                                   tokenArray2[depth2 - 1].index != rapidjson::kPointerInvalidIndex);
        if (!areBothLeavesIndex) {
            if (ptr1 == ptr2) return true;
            return !(ptr1 < ptr2);  // operator > is not available
        }

        // compare path elements up to the parent of leaf
        for (size_t i=0; i < depth1 - 1; i++) {
            if (tokenArray1[i].index != tokenArray2[i].index)
                return tokenArray1[i].index > tokenArray2[i].index;

            if (tokenArray1[i].length != tokenArray2[i].length)
                return tokenArray1[i].length > tokenArray2[i].length;

            if (int cmp = std::memcmp(tokenArray1[i].name, tokenArray2[i].name, sizeof(char) * tokenArray1[i].length))
                return cmp > 0;
        }

        // compare leaf index
        return tokenArray1[depth1 - 1].index > tokenArray2[depth2 - 1].index;
    }
};

JsonUtilCode Selector::deleteValues(JValue &root, const char *path, size_t &numValsDeleted) {
    numValsDeleted = 0;
    JsonUtilCode rc = init(root, path, DELETE);
    if (rc != JSONUTIL_SUCCESS) return rc;
    rc = eval();
    if (rc != JSONUTIL_SUCCESS) return rc;
    if (!isV2Path && !hasValues()) return JSONUTIL_JSON_PATH_NOT_EXIST;
    if (resultSet.empty()) return getError();

    TRACE("DEBUG", "deleteValues total values to delete: " << resultSet.size());
    if (json_is_instrument_enabled_delete()) {
        ValkeyModule_Log(nullptr, "warning", "deleting %zu values of doc %p at path %s",
                        resultSet.size(), static_cast<void *>(&root), path);
        if (!ValidateJValue(root)) {
            ValkeyModule_Log(nullptr, "warning", "ERROR: before delete, doc %p is NOT valid!",
                            static_cast<void *>(&root));
        }
        if (json_is_instrument_enabled_dump_doc_before()) {
            ValkeyModule_Log(nullptr, "warning", "Dump document structure before delete:");
            DumpRedactedJValue(root, nullptr, "warning");
        }
    }

    if (resultSet.size() == 1) {
        if (json_is_instrument_enabled_delete()) {
            ValkeyModule_Log(nullptr, "warning", "deleting value %p of doc %p at path %s",
                            static_cast<void *>(resultSet[0].first), static_cast<void *>(&root), path);
            if (json_is_instrument_enabled_dump_value_before_delete()) {
                DumpRedactedJValue(*resultSet[0].first, nullptr, "warning");
            }
        }
        if (deleteValue(resultSet[0].second)) numValsDeleted++;
    } else {
        jsn::set<jsn::string, pathCompare> path_set;
        for (auto &vInfo : resultSet) {
            if (json_is_instrument_enabled_delete()) {
                ValkeyModule_Log(nullptr, "warning", "preparing to delete value %p of doc %p at path %s",
                                static_cast<void *>(vInfo.first), static_cast<void *>(&root), vInfo.second.c_str());
                if (json_is_instrument_enabled_dump_value_before_delete()) {
                    DumpRedactedJValue(*vInfo.first, nullptr, "warning");
                }
            }
            path_set.insert(std::move(vInfo.second));
        }
        for (auto it = path_set.begin(); it != path_set.end(); it++) {
            if (json_is_instrument_enabled_delete()) {
                ValkeyModule_Log(nullptr, "warning", "deleting value of doc %p at path %s",
                                static_cast<void *>(&root), (*it).c_str());
            }
            if (deleteValue(*it)) numValsDeleted++;
        }
    }

    TRACE("DEBUG", "deleteValues deleted " << numValsDeleted << " values");
    if (json_is_instrument_enabled_delete()) {
        if (!ValidateJValue(root)) {
            ValkeyModule_Log(nullptr, "warning", "ERROR: after delete, doc %p is NOT valid!!",
                            static_cast<void *>(&root));
        }
        if (json_is_instrument_enabled_dump_doc_after()) {
            ValkeyModule_Log(nullptr, "warning", "Dump document structure after delete:");
            DumpRedactedJValue(root, nullptr, "warning");
        }
    }

#ifdef INSTRUMENT_V2PATH
    dom_dump_value(root);
#endif
    return JSONUTIL_SUCCESS;
}

bool Selector::deleteValue(const jsn::string &path) {
    TRACE("DEBUG", "deleteValue deleting value at " << path)
    JPointer ptr = JPointer(path);
    if (ptr.HasError() || !ptr.PathExists(*root)) return false;
    return ptr.Erase(*root);
}

/**
 * Single stage insert/update, which commits the operation.
 *
 * The set op could result in insert or update or both.
 *   Selector::resultSet - values to be updated
 *   Selector::insertPaths - set of insert paths
 *
 * Note that we don't expect Selector::resultSet and Selector::insertPaths to be both non-empty.
 */
JsonUtilCode Selector::setValues(JValue &root, const char *path, JValue &new_val) {
    JsonUtilCode rc = prepareSetValues(root, path);
    if (rc != JSONUTIL_SUCCESS) return rc;
    return commit(new_val);
}

/**
 * Prepare for a 2-stage insert/update. The 2-stage write splits a write operation into two calls: prepareSetValues and
 * commit, where prepareSetValues does not change the Valkey data. The purpose of having a 2-stage write is to
 * be able to discard the write operation if certain conditions are not satisfied.
 *
 * Use cases:
 * 1. JSON.SET with NX/XX option: We need to verify if NX/XX condition is satisfied before committing the operation.
 * 2. Document path limit check: We need to check if the max path limit is exceeded before committing the operation.
 * 3. Document size limit check: We need to check if the document size limit is exceeded before committing the
 *    operation.
 */
JsonUtilCode Selector::prepareSetValues(JValue &root, const char *path) {
    JsonUtilCode rc = init(root, path, INSERT_OR_UPDATE);
    if (rc != JSONUTIL_SUCCESS) return rc;
    rc = eval();
    if (rc != JSONUTIL_SUCCESS) return rc;
    return JSONUTIL_SUCCESS;
}

/**
 * Commit a 2-stage insert/update.
 */
JsonUtilCode Selector::commit(JValue &new_val) {
    if (resultSet.empty() && insertPaths.empty()) return getError();

    // handling update
    auto &rs = getUniqueResultSet();
    if (!rs.empty()) {
        if (json_is_instrument_enabled_update()) {
            ValkeyModule_Log(nullptr, "warning", "updating %zu values of doc %p", rs.size(), static_cast<void *>(&root));
            if (!ValidateJValue(*root)) {
                ValkeyModule_Log(nullptr, "warning", "ERROR: before update, doc %p is NOT valid!",
                                static_cast<void *>(root));
            }
            if (json_is_instrument_enabled_dump_doc_before()) {
                ValkeyModule_Log(nullptr, "warning", "Dump document structure before update:");
                DumpRedactedJValue(*root, nullptr, "warning");
            }
        }

        if (rs.size() == 1) {
            JPointer ptr = JPointer(rs[0].second);
            if (ptr.HasError()) return ptr.error;
            if (json_is_instrument_enabled_update()) {
                ValkeyModule_Log(nullptr, "warning", "updating value %p of doc %p at path %s",
                                static_cast<void *>(rs[0].first), static_cast<void *>(root), rs[0].second.c_str());
            }
            ptr.Swap(*root, new_val, allocator);
            TRACE("DEBUG", "commit updated value at " << rs[0].second);
        } else {
            for (auto &vInfo : rs) {
                // copy the new value so that it can be set at multiple paths
                JValue new_val_copy(new_val, allocator);
                JPointer ptr = JPointer(vInfo.second);
                if (ptr.HasError()) return ptr.error;
                // An existing path may not exist due to updates of other values.
                // However, JPointer will always insert the value if the path does not exist.
                // So, we'll do update only if the path still exists.
                if (ptr.PathExists(*root)) {
                    if (json_is_instrument_enabled_update()) {
                        ValkeyModule_Log(nullptr, "warning", "updating value %p of doc %p at path %s",
                                        static_cast<void *>(vInfo.first), static_cast<void *>(&root),
                                        vInfo.second.c_str());
                    }
                    ptr.Swap(*root, new_val_copy, allocator);
                    TRACE("DEBUG", "commit updated value at " << vInfo.second);
                }
            }
        }

        TRACE("DEBUG", "commit updated values: " << rs.size());
        if (json_is_instrument_enabled_update()) {
            if (!ValidateJValue(*root)) {
                ValkeyModule_Log(nullptr, "warning", "ERROR: after update, doc %p is NOT valid!",
                                static_cast<void *>(root));
            }
            if (json_is_instrument_enabled_dump_doc_after()) {
                ValkeyModule_Log(nullptr, "warning", "Dump document structure after update:");
                DumpRedactedJValue(*root, nullptr, "warning");
            }
        }
    }

    // handling insert
    if (!insertPaths.empty()) {
        if (json_is_instrument_enabled_insert()) {
            ValkeyModule_Log(nullptr, "warning", "inserting %zu values into doc %p",
                            insertPaths.size(), static_cast<void *>(root));
            if (!ValidateJValue(*root)) {
                ValkeyModule_Log(nullptr, "warning", "ERROR: before insert, doc %p is NOT valid!",
                                static_cast<void *>(root));
            }
            if (json_is_instrument_enabled_dump_doc_before()) {
                ValkeyModule_Log(nullptr, "warning", "Dump document structure before insert:");
                DumpRedactedJValue(*root, nullptr, "warning");
            }
        }

        if (insertPaths.size() == 1) {
            JPointer ptr = JPointer(*insertPaths.begin());
            if (ptr.HasError()) return ptr.error;
            if (json_is_instrument_enabled_insert()) {
                ValkeyModule_Log(nullptr, "warning", "inserting value into doc %p at path %s",
                                static_cast<void *>(root), (*insertPaths.begin()).c_str());
            }
            ptr.Set(*root, new_val, allocator);
            TRACE("DEBUG", "commit inserted value at " << *insertPaths.begin());
        } else {
            for (auto &path : insertPaths) {
                // copy the new value so that it can be set at multiple paths
                JValue new_val_copy(new_val, allocator);
                JPointer ptr = JPointer(path);
                if (ptr.HasError()) return ptr.error;
                if (json_is_instrument_enabled_insert()) {
                    ValkeyModule_Log(nullptr, "warning", "inserting value into doc %p at path %s",
                                    static_cast<void *>(root), path.c_str());
                }
                ptr.Set(*root, new_val_copy, allocator);
                TRACE("DEBUG", "commit inserted value at " << *insertPaths.begin());
            }
        }

        TRACE("DEBUG", "commit inserted values: " << insertPaths.size());
        if (json_is_instrument_enabled_insert()) {
            if (!ValidateJValue(*root)) {
                ValkeyModule_Log(nullptr, "warning", "ERROR: after insert, doc %p is NOT valid!",
                                static_cast<void *>(root));
            }
            if (json_is_instrument_enabled_dump_doc_after()) {
                ValkeyModule_Log(nullptr, "warning", "Dump document structure after insert:");
                DumpRedactedJValue(*root, nullptr, "warning");
            }
        }
    }

    return JSONUTIL_SUCCESS;
}

JsonUtilCode Selector::init(JValue &root, const char *path, const Mode mode) {
    CHECK_QUERY_STRING_SIZE(path);
    this->mode = mode;
    this->root = &root;
    node = &root;
    nodePath = "";
    lex.init(path);
    resultSet.clear();
    insertPaths.clear();
    maxPathDepth = 0;
    currPathDepth = 0;
    error = JSONUTIL_SUCCESS;

    lex.nextToken();  // initial pull
    return JSONUTIL_SUCCESS;
}

JsonUtilCode Selector::eval() {
    TRACE("DEBUG", "eval curr token: " << lex.currToken().type << ", remaining path: " << lex.p
        << ", nodePath: " << nodePath)
    CHECK_RECURSION_DEPTH();
    JsonUtilCode rc = parseSupportedPath();
    if (rc == JSONUTIL_SUCCESS && node != nullptr) {
        // select the value
        ValueInfo vInfo(node, nodePath);
        resultSet.push_back(std::move(vInfo));
    }
    return rc;
}

/**
 * Errors fall into two categories: JSONPath syntax error and non-syntax error. Non-syntax error examples are
 * path does not exist, array index out of bounds, index not a number, etc. In multi-path recursive traversals
 * (e.g., wildcard, slice, etc.), if a syntax error is detected, the entire selector process should be immediately
 * terminated (there is no need to continue exploring other paths). If a non-syntax error is detected, only the
 * current path search ends, and we should continue exploring unexplored paths.
 */
bool Selector::isSyntaxError(JsonUtilCode code) const {
    return (code == JSONUTIL_INVALID_JSON_PATH ||
            code == JSONUTIL_INVALID_MEMBER_NAME ||
            code == JSONUTIL_INVALID_NUMBER ||
            code == JSONUTIL_INVALID_IDENTIFIER ||
            code == JSONUTIL_EMPTY_EXPR_TOKEN ||
            code == JSONUTIL_ARRAY_INDEX_NOT_NUMBER ||
            code == JSONUTIL_STEP_CANNOT_NOT_BE_ZERO ||
            code == JSONUTIL_PARENT_ELEMENT_NOT_EXIST ||
            code == JSONUTIL_PARSER_RECURSION_DEPTH_LIMIT_EXCEEDED ||
            code == JSONUTIL_RECURSIVE_DESCENT_TOKEN_LIMIT_EXCEEDED ||
            code == JSONUTIL_QUERY_STRING_SIZE_LIMIT_EXCEEDED);
}

/**
 * SupportedPath       ::= ["$" | "."] RelativePath
 */
JsonUtilCode Selector::parseSupportedPath() {
    if (node == nullptr || lex.matchToken(Token::END)) return JSONUTIL_SUCCESS;

    if (lex.matchToken(Token::DOLLAR)) {
        if (!ENABLE_V2_SYNTAX) return JSONUTIL_INVALID_JSON_PATH;
        if (node != root) return JSONUTIL_DOLLAR_CANNOT_APPLY_TO_NON_ROOT;
        isV2Path = true;
    } else if (lex.matchToken(Token::DOT)) {
    }
    return parseRelativePath();
}

/**
 *  RelativePath        ::= empty | RecursivePath | DotPath | BracketPath | QualifiedPath
 */
JsonUtilCode Selector::parseRelativePath() {
    if (node == nullptr || lex.matchToken(Token::END)) return JSONUTIL_SUCCESS;

    switch (lex.currToken().type) {
        case Token::END: return JSONUTIL_SUCCESS;
        case Token::DOTDOT: return parseRecursivePath();
        case Token::DOT: return parseDotPath();
        case Token::LBRACKET: return parseBracketPath();
        default: return parseQualifiedPath();
    }
}

/**
 *  RecursivePath       ::= ".." SupportedPath
 */
JsonUtilCode Selector::parseRecursivePath() {
    isRecursiveSearch = true;
    if (!lex.matchToken(Token::DOTDOT)) ValkeyModule_Assert(false);
    CHECK_RECURSIVE_DESCENT_TOKENS();

    JsonUtilCode rc = recursiveSearch(*node, lex.p);
    if (rc != JSONUTIL_SUCCESS) return rc;
    dedupe();
    return JSONUTIL_SUCCESS;
}

/**
 * This DFS algorithm literally embodies "recursive descent":
 * 1. Run DFS on the subtree rooted from the current node (a.k.a. value).
 * 2. When each node is visited, run the selector at the node with the remaining path.
 * 3. Selector::resultSet serves as the global result holding all selected values.
 */
JsonUtilCode Selector::recursiveSearch(JValue &v, const char *p) {
    TRACE("DEBUG", "recursiveSearch curr token " << lex.currToken().type << ", curr path: "<< lex.p
        << ", nodePath: " << nodePath << ", currPathDepth: " << currPathDepth << ", maxPathDepth: " << maxPathDepth)
    if (lex.currToken().type == Token::DOTDOT || lex.currToken().type == Token::DOT) {
        TRACE("DEBUG", "We have an ambiguous (and therefore invalid) sequence of 3+ dots")
        return JSONUTIL_INVALID_DOT_SEQUENCE;
    }
    if (!v.IsObject() && !v.IsArray()) {
        // Null out the current node to signal termination of the current path search.
        node = nullptr;
        return JSONUTIL_SUCCESS;
    }

    // At the current node, run the selector by calling eval().
    State state;
    snapshotState(state);
    node = &v;  // points to the current visited value
    JsonUtilCode rc = eval();  // run the selector
    restoreState(state);
    if (isSyntaxError(rc)) return rc;

    // Descend to each child (i.e., recursive descent)
    if (v.IsObject()) {
        for (auto &m : v.GetObject()) {
            jsn::string path_copy = nodePath;
            nodePath.append("/").append(m.name.GetStringView());
            incrPathDepth();
            TRACE("DEBUG", "-> recursiveSearch descend to object member " << m.name.GetStringView()
                << ", nodePath: " << nodePath << ", currPathDepth: " << currPathDepth << ", maxPathDepth: "
                << maxPathDepth)
            rc = recursiveSearch(m.value, p);
            decrPathDepth();
            if (isSyntaxError(rc)) return rc;
            nodePath = path_copy;
        }
    } else if (v.IsArray()) {
        for (int64_t i=0; i < v.Size(); i++) {
            jsn::string path_copy = nodePath;
            nodePath.append("/").append(std::to_string(i));
            incrPathDepth();
            TRACE("DEBUG", "-> recursiveSearch descend to array index " << i << ", nodePath: " << nodePath
            << ", currPathDepth: " << currPathDepth << ", maxPathDepth: " << maxPathDepth)
            rc = recursiveSearch(v.GetArray()[i], p);
            decrPathDepth();
            if (isSyntaxError(rc)) return rc;
            nodePath = path_copy;
        }
    }

    // Null out the current node to signal we are done with the search.
    node = nullptr;
    return JSONUTIL_SUCCESS;
}

/**
 *   DotPath             ::= "." QualifiedPath
 */
JsonUtilCode Selector::parseDotPath() {
    if (!lex.matchToken(Token::DOT)) ValkeyModule_Assert(false);
    return parseQualifiedPath();
}

/**
 *   BracketPath         ::= BracketPathElement [ RelativePath ]
 */
JsonUtilCode Selector::parseBracketPath() {
    JsonUtilCode rc = parseBracketPathElement();
    if (rc != JSONUTIL_SUCCESS) return rc;
    if (lex.currToken().type == Token::END) return JSONUTIL_SUCCESS;
    return parseRelativePath();
}

/**
 *   BracketPathElement  ::= "[" {SPACE} ( WildcardInBrackets | ((NameInBrackets | IndexExpr) ) {SPACE} "]")
 *   WildcardInBrackets  ::= "*" {SPACE} "]" [ "[" {SPACE} "?" "(" FilterExpr ")" {SPACE} "]" ]
 *   NameInBrackets      ::= QuotedMemberName [ ({SPACE} "," {SPACE} QuotedMemberName)+ ]
 *   QuotedMemberName    ::= """ {char} """ | "'" {char} "'"
 *   IndexExpr           ::= Filter | SliceStartsWithColon | SliceOrUnionOrIndex
 */
JsonUtilCode Selector::parseBracketPathElement() {
    if (!lex.matchToken(Token::LBRACKET, true)) {
        TRACE("ERROR", "parseBracketPathElement token [ is not seen"  << ", nodePath: " << nodePath)
        return JSONUTIL_INVALID_JSON_PATH;
    }

    JsonUtilCode rc;
    const Token &token = lex.currToken();
    if (token.type == Token::WILDCARD) {
        rc = parseWildcardInBrackets();
    } else {
        if (token.type == Token::SINGLE_QUOTE || token.type == Token::DOUBLE_QUOTE) {
            rc = parseNameInBrackets();
        } else {
            rc = parseIndexExpr();
        }
    }
    if (rc != JSONUTIL_SUCCESS) {
        TRACE("ERROR", "parseBracketPathElement rc: " << rc << ", nodePath: " << nodePath)
        return rc;
    }
    lex.skipSpaces();
    return JSONUTIL_SUCCESS;
}

/**
 *   WildcardInBrackets  ::= "*" {SPACE} "]" [ "[" {SPACE} "?" "(" FilterExpr ")" {SPACE} "]" ]
 */
JsonUtilCode Selector::parseWildcardInBrackets() {
    if (!lex.matchToken(Token::WILDCARD, true)) return JSONUTIL_INVALID_JSON_PATH;
    if (!lex.matchToken(Token::RBRACKET, true)) return JSONUTIL_INVALID_JSON_PATH;

    if (lex.currToken().type == Token::LBRACKET && lex.peekToken() == Token::QUESTION_MARK) {
        if (!lex.matchToken(Token::LBRACKET, true)) return JSONUTIL_INVALID_JSON_PATH;
        lex.skipSpaces();
        if (!lex.matchToken(Token::QUESTION_MARK)) return JSONUTIL_INVALID_JSON_PATH;
        if (!lex.matchToken(Token::LPAREN)) return JSONUTIL_INVALID_JSON_PATH;

        jsn::vector<int64_t> result;  // subset of indexes of the current array
        JsonUtilCode rc = parseFilterExpr(result);
        if (rc != JSONUTIL_SUCCESS) return rc;

        if (!lex.matchToken(Token::RPAREN, true)) return JSONUTIL_INVALID_JSON_PATH;
        if (!lex.matchToken(Token::RBRACKET, true)) return JSONUTIL_INVALID_JSON_PATH;
        return processFilterResult(result);
    } else {
        return processWildcard();
    }
}

/**
 *   NameInBrackets      ::= QuotedMemberName { {SPACE} "," {SPACE} QuotedMemberName }
 *   QuotedMemberName    ::= """ {char} """ | "'" {char} "'"
 */
JsonUtilCode Selector::parseNameInBrackets() {
    jsn::vector<jsn::string> member_names;
    jsn::stringstream ss;
    JsonUtilCode rc = parseQuotedMemberName(ss);
    if (rc != JSONUTIL_SUCCESS) return rc;
    member_names.push_back(ss.str());
    TRACE("DEBUG", "parseNameInBrackets added member: " << ss.str())

    while (lex.matchToken(Token::COMMA, true)) {
        lex.skipSpaces();
        ss.str(jsn::string());
        rc = parseQuotedMemberName(ss);
        if (rc != JSONUTIL_SUCCESS) return rc;
        member_names.push_back(ss.str());
        TRACE("DEBUG", "parseNameInBrackets added member: " << ss.str())
    }

    if (!lex.matchToken(Token::RBRACKET, true)) return JSONUTIL_INVALID_JSON_PATH;
    return processUnionOfMembers(member_names);
}

JsonUtilCode Selector::processUnionOfMembers(const jsn::vector<jsn::string> &member_names) {
    if (member_names.size() == 1) {
        StringViewHelper member_name;
        member_name.setInternalString(member_names[0]);
        TRACE("DEBUG", "processUnionOfMembers member: " << member_name.getView())
        return traverseToObjectMember(member_name);
    }

    if (!node->IsObject()) {
        if (mode != READ) return JSONUTIL_CANNOT_INSERT_MEMBER_INTO_NON_OBJECT_VALUE;
        // Null out the current node to signal termination of the current path search.
        node = nullptr;
        return JSONUTIL_SUCCESS;
    }

    for (auto &s : member_names) {
        TRACE("DEBUG", "processUnionOfMembers finding member " << s)
        JValue::MemberIterator it = node->FindMember(s);
        if (it != node->MemberEnd()) {
            StringViewHelper member_name;
            member_name.setInternalString(s);
            JsonUtilCode rc = evalObjectMember(member_name, it->value);
            if (isSyntaxError(rc)) return rc;
        }
    }

    // We are done. Null out the current node to signal end of selection.
    node = nullptr;
    return JSONUTIL_SUCCESS;
}

/**
 *   QuotedMemberName   ::= """ {char} """ | "'" {char} "'"
 */
JsonUtilCode Selector::parseQuotedMemberName(jsn::stringstream &ss) {
    const Token &token = lex.currToken();
    if (token.type == Token::DOUBLE_QUOTE) {
        JsonUtilCode rc = lex.scanDoubleQuotedString(ss);
        if (rc != JSONUTIL_SUCCESS) return rc;
    } else if (token.type == Token::SINGLE_QUOTE) {
        JsonUtilCode rc = lex.scanSingleQuotedString(ss);
        if (rc != JSONUTIL_SUCCESS) return rc;
    } else {
        return JSONUTIL_INVALID_JSON_PATH;
    }
    TRACE("DEBUG", "parseQuotedMemberName member_name: " << ss.str())
    return JSONUTIL_SUCCESS;
}

/**
 *   QualifiedPath       ::= QualifiedPathElement RelativePath
 */
JsonUtilCode Selector::parseQualifiedPath() {
    TRACE("DEBUG", "parseQualifiedPath curr token: " << lex.currToken().type << ", nodePath: " << nodePath)
    JsonUtilCode rc = parseQualifiedPathElement();
    if (rc != JSONUTIL_SUCCESS) return rc;
    return parseRelativePath();
}

/**
 *   QualifiedPathElement ::= Key | BracketPathElement
 */
JsonUtilCode Selector::parseQualifiedPathElement() {
    if (lex.currToken().type == Token::LBRACKET)
        return parseBracketPathElement();
    else
        return parseKey();
}

/**
 *   Key                 ::= "*" [ [ "." ] WildcardFilter ] | UnquotedMemberName
 *   WildcardFilter  ::=  "[" "?" "(" FilterExpr ")" "]"
 */
JsonUtilCode Selector::parseKey() {
    if (lex.matchToken(Token::WILDCARD)) {
        if (lex.currToken().type == Token::DOT) lex.nextToken();  // skip DOT
        if (lex.currToken().type == Token::LBRACKET && lex.peekToken() == Token::QUESTION_MARK) {
            return parseWildcardFilter();
        } else {
            return processWildcard();
        }
    } else {
        StringViewHelper name;
        JsonUtilCode rc = parseUnquotedMemberName(name);
        if (rc != JSONUTIL_SUCCESS) return rc;
        return traverseToObjectMember(name);
    }
}

/**
 *   UnquotedMemberName  ::= char { char }
 */
JsonUtilCode Selector::parseUnquotedMemberName(StringViewHelper &name) {
    JsonUtilCode rc = lex.scanUnquotedMemberName(name);
    if (rc != JSONUTIL_SUCCESS) return rc;
    TRACE("DEBUG", "parseUnquotedMemberName name: " << name.getView() << ", nodePath: " << nodePath)
    return JSONUTIL_SUCCESS;
}

/**
 *   WildcardFilter  ::= "[" "?" "(" FilterExpr ")" "]"
 */
JsonUtilCode Selector::parseWildcardFilter() {
    if (!node->IsArray()) return JSONUTIL_INVALID_JSON_PATH;

    if (!lex.matchToken(Token::LBRACKET)) return JSONUTIL_INVALID_JSON_PATH;
    if (!lex.matchToken(Token::QUESTION_MARK)) return JSONUTIL_INVALID_JSON_PATH;
    if (!lex.matchToken(Token::LPAREN, true)) return JSONUTIL_INVALID_JSON_PATH;

    jsn::vector<int64_t> result;  // subset of indexes of the current array
    JsonUtilCode rc = parseFilterExpr(result);
    if (rc != JSONUTIL_SUCCESS) return rc;

    if (!lex.matchToken(Token::RPAREN, true)) return JSONUTIL_INVALID_JSON_PATH;
    if (!lex.matchToken(Token::RBRACKET)) return JSONUTIL_INVALID_JSON_PATH;

    return processFilterResult(result);
}

JsonUtilCode Selector::processWildcard() {
    if (node->IsObject()) {
        return processWildcardKey();
    } else if (node->IsArray()) {
        return processWildcardIndex();
    } else {
        // v1 path syntax: return json syntax error that will fail the command.
        // v2 path syntax: return a non-syntax error that will not fail the command. Only the
        // current path search ends, and we should continue exploring other paths.
        return isV2Path ? JSONUTIL_INVALID_USE_OF_WILDCARD : JSONUTIL_INVALID_JSON_PATH;
    }
}

JsonUtilCode Selector::processWildcardKey() {
    JsonUtilCode rc;
    for (auto &m : node->GetObject()) {
        TRACE("DEBUG", "processWildcardKey continue parsing object member "
            << m.name.GetStringView() << ", curr token: " << lex.currToken().type << ", remaining path: " << lex.p)
        State state;
        snapshotState(state);
        StringViewHelper member_name;
        member_name.setInternalView(m.name.GetStringView());
        rc = evalObjectMember(member_name, m.value);
        restoreState(state);
        if (isSyntaxError(rc)) return rc;
    }
    // We are done. Null out the current node to signal done of value collection.
    node = nullptr;
    return JSONUTIL_SUCCESS;
}

JsonUtilCode Selector::processWildcardIndex() {
    JsonUtilCode rc;
    for (int64_t i=0; i < node->Size(); i++) {
        TRACE("DEBUG", "processWildcardIndex continue parsing array index " << i
            << ", curr token: " << lex.currToken().type << ", remaining path: " << lex.p << ", nodePath: " << nodePath)
        rc = evalArrayMember(i);
        if (isSyntaxError(rc)) return rc;
    }
    // We are done. Null out the current node to signal done of value collection.
    node = nullptr;
    return JSONUTIL_SUCCESS;
}

/**
 * Recursively call eval to evaluate the member and continue parsing the remaining path.
 * Multiple wildcards/filter expressions are handled through recursion. e.g.,
 *
 * $.a[?(@.price < $.expensive)].[*].[?(@.x > 15]
 *
 * 1. The first filter expression is evaluated, resulting in a slice of the current array ($.a).
 * 2. The search path forks out, one per element of the slice. We'll explore each path.
 * 3. For each path, continue parsing the remaining json path. When the wildcard is processed, each
 *    path forks out n subpaths, where n is the length of the current array.
 * 4. For each subpath, the 2nd filter expression is evaluated, which results in a slice of the current array.
 * 5. Again, each subpath continues to fork out, one per element of the slice.
 */
JsonUtilCode Selector::evalMember(JValue &m, const char *path_start) {
    CHECK_RECURSION_DEPTH();
    incrPathDepth();
    resetPointers(m, path_start);
    return eval();
}

JsonUtilCode Selector::evalObjectMember(const StringViewHelper &member_name, JValue &val) {
    if (!node->IsObject()) {
        TRACE("DEBUG", "evalObjectMember Current node is not object. Cannot eval member "
        << member_name.getView())
        return JSONUTIL_JSON_ELEMENT_NOT_OBJECT;
    }

    State state;
    snapshotState(state);

    nodePath.append("/").append(member_name.getView());
    TRACE("DEBUG", "evalObjectMember object member " << member_name.getView()
    << ", curr token: " << lex.currToken().type << ", remaining path: " << lex.p << ", nodePath: " << nodePath)
    JsonUtilCode rc =  evalMember(val, lex.p);

    restoreState(state);
    return rc;
}

JsonUtilCode Selector::evalArrayMember(int64_t idx) {
    if (!node->IsArray()) {
        TRACE("DEBUG", "evalArrayMember Current node is not array. Cannot eval array index " << idx)
        return JSONUTIL_JSON_ELEMENT_NOT_ARRAY;
    }
    if (idx < 0 || idx >= static_cast<long long>(node->Size())) return JSONUTIL_INDEX_OUT_OF_ARRAY_BOUNDARIES;

    State state;
    snapshotState(state);

    nodePath.append("/").append(std::to_string(idx));
    TRACE("DEBUG", "evalArrayMember array index " << idx << ", curr token: " << lex.currToken().type
        << ", remaining path: " << lex.p << ", nodePath: " << nodePath)
    JsonUtilCode rc = evalMember(node->GetArray()[idx], lex.p);

    restoreState(state);
    return rc;
}

JsonUtilCode Selector::traverseToObjectMember(const StringViewHelper &member_name) {
    if (!node->IsObject()) {
        // We should not assert node must be an object, because this could just be a user error.
        // e.g., path: $.phoneNumbers.city, where phoneNumbers is actually an array not object. An assertion would
        // have crashed the server. The correct way to treat such user error is termination the current path search
        // with no matching values found. Neither should we indicate a syntax error that will fail the entire search,
        // because this is just a termination of one search path. Other path searches should continue.
        TRACE("DEBUG", "traverseToObjectMember Current node is not object. Cannot traverse to member "
        << member_name.getView() << ", nodePath: " << nodePath)
        if (mode != READ) return JSONUTIL_CANNOT_INSERT_MEMBER_INTO_NON_OBJECT_VALUE;

        // Null out the current node to signal termination of the current path search.
        node = nullptr;
        return JSONUTIL_SUCCESS;
    }

    JValue::MemberIterator it = node->FindMember(member_name.getView());
    if (it == node->MemberEnd()) {
         TRACE("DEBUG", "traverseToObjectMember Member not found: "
         << member_name.getView() << " len: "
         << member_name.getView().length() << " mode:" << mode
         << " cur node isObj? " << node->IsObject() << ", nodePath: " << nodePath)
#ifdef INSTRUMENT_V2PATH
         dom_dump_value(*node);
#endif

        if ((mode == INSERT || mode == INSERT_OR_UPDATE) && !isRecursiveSearch) {
            // A new key can be appended to an object if and only if it is the last child in the path
            TRACE("DEBUG", "traverseToObjectMember insert mode, peek next token: " << lex.peekToken()
                << ", nodePath: " << nodePath);
            if (lex.peekToken() == Token::END) {
                jsn::string insert_path = nodePath;
                insert_path.append("/").append(member_name.getView());
                TRACE("DEBUG", "traverseToObjectMember add insert path: " << insert_path)
                insertPaths.insert(std::move(insert_path));
                incrPathDepth();
            } else {
                TRACE("DEBUG", "traverseToObjectMember insert mode, cannot insert because current "
                               "node is not the last child in the path, nodePath: " << nodePath)
                setError(JSONUTIL_JSON_PATH_NOT_EXIST);
                return JSONUTIL_JSON_PATH_NOT_EXIST;
            }
        }

        // Null out the current node to signal end of search.
        node = nullptr;
        return JSONUTIL_SUCCESS;
    }

    nodePath.append("/").append(member_name.getView());
    TRACE("DEBUG", "traverseToObjectMember traversed to object member "
    << member_name.getView() << ". remaining path: " << lex.p
    << ", nodePath: " << nodePath)
    node = &it->value;
    incrPathDepth();
    return JSONUTIL_SUCCESS;
}

JsonUtilCode Selector::traverseToArrayIndex(int64_t idx) {
    if (!node->IsArray()) {
        // We should not assert node must be an array, because this could just be a user error.
        // e.g., path: $.address[0], where address is actually an object not array. An assertion would
        // have crashed the server. The correct way to treat such user error is termination the current path
        // search with no matching values found. Neither should we indicate a syntax error that will fail
        // the entire search, because this is just a termination of one search path. Other path searches should
        // continue.

        TRACE("DEBUG", "traverseToArrayIndex Current node is not array. Cannot traverse to index " << idx
            << ", nodePath: " << nodePath)
        // Null out the current node to signal termination of the current path search.
        node = nullptr;
        return JSONUTIL_SUCCESS;
    }

    // handle negative index
    if (idx < 0) idx += node->Size();

    // check index bounds
    if (idx >= static_cast<long long>(node->Size()) || idx < 0) {
        return JSONUTIL_INDEX_OUT_OF_ARRAY_BOUNDARIES;
    }

    nodePath.append("/").append(std::to_string(idx));
    TRACE("DEBUG", "traverseToArrayIndex traversed to array index " << idx << ", nodePath: " << nodePath)
    node = &node->GetArray()[idx];
    incrPathDepth();
    return JSONUTIL_SUCCESS;
}

/**
 *   parseIndexExpr() is called from parseBracketPathElement().
 *
 *   BracketPathElement  ::= "[" {SPACE} ( WildcardInBrackets | ((NameInBrackets | IndexExpr) {SPACE} ) "]")
 *   WildcardInBrackets  ::= "*" {SPACE} "]" [ "[" {SPACE} "?" "(" FilterExpr ")" {SPACE} "]" ]
 *   IndexExpr           ::= Filter | SliceStartsWithColon | SliceOrUnionOrIndex
 *   Filter              ::= "?" "(" "@" "." FilterExpr ")"
 *   SliceStartsWithColon ::= {SPACE} ":" {SPACE} [ ":" {SPACE} [Step] | EndAndStep ] ]
 *   EndAndStep          ::= End [{SPACE} ":" {SPACE} [Step]] ]
 *   SliceOrUnionOrIndex ::= SliceStartsWithInteger | Index | UnionOfIndexes
 *   SliceStartsWithInteger ::= Start {SPACE} ":" {SPACE} [ ":" {SPACE} [Step] | EndAndStep
 *   EndAndStep          ::= End [{SPACE} ":" {SPACE} [Step]] ]
 *   Index               ::= Integer
 *   Integer             ::= ["+" | "-"] digit {digit}
 *   Start               ::= Integer
 *   End                 ::= Integer
 *   Step                ::= Integer
 *   UnionOfIndexes      ::= Integer ({SPACE} "," {SPACE} Integer)+
 */
JsonUtilCode Selector::parseIndexExpr() {
    switch (lex.currToken().type) {
        case Token::END:
            return JSONUTIL_EMPTY_EXPR_TOKEN;
        case Token::QUESTION_MARK:
            return parseFilter();
        case Token::COLON:
            return parseSliceStartsWithColon();
        case Token::COMMA:
            return JSONUTIL_INVALID_JSON_PATH;  // union cannot start with comma
        default:
            return parseSliceOrUnionOrIndex();
    }
}

/**
 *   SliceStartsWithColon ::= {SPACE} ":" [ {SPACE} ":" [Step] | {SPACE} EndAndStep ] ]
 *   EndAndStep           ::= End [{SPACE} ":" {SPACE} [Step]] ]
 *   End                  ::= Integer
 *   Step                 ::= Integer
 *   Integer              ::= ["+" | "-"] digit {digit}
 */
JsonUtilCode Selector::parseSliceStartsWithColon() {
    if (!node->IsArray()) return JSONUTIL_JSON_ELEMENT_NOT_ARRAY;

    lex.nextToken(true);  // skip COLON
    switch (lex.currToken().type) {
        case Token::RBRACKET:
            return processSlice(0, node->Size());
        case Token::COLON: {
            lex.nextToken(true);  // skip COLON
            return parseStep(0, node->Size());
        }
        default:
            return parseEndAndStep(0);
    }
}

/**
 *   SliceOrUnionOrIndex    ::= SliceStartsWithInteger | UnionOfIndexes | Index
 *   SliceStartsWithInteger ::= Start {SPACE} ":" {SPACE} [ ":" {SPACE} [Step] | EndAndStep
 *   EndAndStep             ::= End [{SPACE} ":" {SPACE} [Step]] ]
 *   UnionOfIndexes         ::= Integer ({SPACE} "," {SPACE} Integer)+
 *   Index                  ::= Integer
 */
JsonUtilCode Selector::parseSliceOrUnionOrIndex() {
    int64_t start;
    JsonUtilCode rc = parseIndex(start);
    if (rc != JSONUTIL_SUCCESS) return rc;

    lex.skipSpaces();
    switch (lex.currToken().type) {
        case Token::COLON:
            return parseSliceStartsWithInteger(start);
        case Token::COMMA:
            return parseUnionOfIndexes(start);
        default:
            return processSubscript(start);
    }
}

/**
 *   SliceStartsWithInteger ::= Start {SPACE} ":" {SPACE} [ ":" {SPACE} [Step] | EndAndStep
 *   EndAndStep             ::= End [{SPACE} ":" {SPACE} [Step]] ]
 */
JsonUtilCode Selector::parseSliceStartsWithInteger(const int64_t start) {
    if (!node->IsArray()) return JSONUTIL_JSON_ELEMENT_NOT_ARRAY;
    lex.nextToken(true);  // skip COLON

    lex.skipSpaces();
    switch (lex.currToken().type) {
        case Token::RBRACKET:
            return processSlice(start, node->Size());
        case Token::COLON: {
            lex.nextToken();  // skip COLON
            return parseStep(start, node->Size());
        }
        default:
            return parseEndAndStep(start);
    }
}

/**
 *   EndAndStep ::= End [{SPACE} ":" {SPACE} [Step]] ]
 */
JsonUtilCode Selector::parseEndAndStep(const int64_t start) {
    int64_t end;
    JsonUtilCode rc = parseIndex(end);
    if (rc != JSONUTIL_SUCCESS) return rc;

    lex.skipSpaces();
    if (lex.currToken().type == Token::COLON) {
        lex.nextToken();  // skip COLON
        return parseStep(start, end);
    } else {
        return processSlice(start, end);
    }
}

JsonUtilCode Selector::parseStep(const int64_t start, const int64_t end) {
    lex.skipSpaces();
    if (lex.currToken().type == Token::RBRACKET) {
        return processSlice(start, end);
    } else {
        int64_t step;
        JsonUtilCode rc = parseIndex(step);
        if (rc != JSONUTIL_SUCCESS) return rc;
        return processSlice(start, end, step);
    }
}

JsonUtilCode Selector::parseIndex(int64_t &val) {
    JsonUtilCode rc = lex.scanInteger(val);
    if (rc == JSONUTIL_VALUE_NOT_NUMBER) rc = JSONUTIL_ARRAY_INDEX_NOT_NUMBER;
    return rc;
}

JsonUtilCode Selector::processSubscript(const int64_t idx) {
    if (!lex.matchToken(Token::RBRACKET, true)) return JSONUTIL_INVALID_JSON_PATH;
    if (!node->IsArray()) return JSONUTIL_JSON_ELEMENT_NOT_ARRAY;
    return traverseToArrayIndex(idx);
}

JsonUtilCode Selector::processSlice(int64_t start, int64_t end, const int64_t step) {
    if (!node->IsArray()) return JSONUTIL_JSON_ELEMENT_NOT_ARRAY;
    if (!lex.matchToken(Token::RBRACKET, true)) return JSONUTIL_INVALID_JSON_PATH;
    TRACE("DEBUG", "processSlice start: " << start << " end: " << end << " step: "
            << step << ", p: " << lex.p << ", nodePath: " << nodePath)
    // handle negative index
    if (start < 0) start += node->Size();
    if (end < 0) end += node->Size();
    // Verify step cannot be 0.
    if (step == 0) {
        return JSONUTIL_STEP_CANNOT_NOT_BE_ZERO;
    }

    // if the index is out of bounds, round it to the respective bound.
    if (start < 0)
        start = 0;
    else if (start > node->Size())
        start = node->Size();
    if (end < 0)
        end = 0;
    else if (end > node->Size())
        end = node->Size();

    JsonUtilCode rc = JSONUTIL_SUCCESS;
    if (step > 0) {
        for (int i = start; i < end; i += step) {
            rc = evalArrayMember(i);
            if (isSyntaxError(rc)) return rc;
        }
    } else {
        for (int i = start; i > end; i += step) {
            rc = evalArrayMember(i);
            if (isSyntaxError(rc)) return rc;
        }
    }

    // We are done. Null out the current node to signal done of value collection.
    node = nullptr;
    return JSONUTIL_SUCCESS;
}

/**
 *   Filter              ::= "?" "(" FilterExpr ")"
 *   FilterExpr          ::= {SPACE} Term { {SPACE} "||" {SPACE} Term {SPACE} }
 *   Term                ::= Factor { {SPACE} "&&" {SPACE} Factor }
 *   Factor              ::= ( "@" ( MemberName | ( [ MemberName ] ComparisonOp ComparisonValue) ) ) |
 *                           ( ComparisonValue ComparisonOp "@" ( MemberName | ( [ MemberName ]) ) ) |
 *                           ( {SPACE} "(" FilterExpr ")" {SPACE} )
 *   MemberName          ::= ("." (UnquotedMemberName | BracketedMemberName)) | BracketedMemberName
 *   BracketedMemberName ::= "[" {SPACE} QuotedMemberName {SPACE} "]"
 *   ComparisonOp        ::= {SPACE} "<" | "<="] | ">" | ">=" | "==" | "!=" {SPACE}
 *   ComparisonValue     ::= "null" | Bool | Number | QuotedString | PartialPath
 *   Bool                ::= "true" | "false"
 *   Number              ::= Integer | MemberNameInFilter | ScientificNumber
 *   QuotedString        ::= "\"" {char} "\""
 *   PartialPath         ::= "$" RelativePath
 */
JsonUtilCode Selector::parseFilter() {
    lex.nextToken();  // skip QUESTION_MARK
    if (!lex.matchToken(Token::LPAREN)) return JSONUTIL_INVALID_JSON_PATH;
    jsn::vector<int64_t> result;  // subset of indexes of the current array
    JsonUtilCode rc = parseFilterExpr(result);
    if (rc != JSONUTIL_SUCCESS) return rc;
    if (!lex.matchToken(Token::RPAREN)) return JSONUTIL_INVALID_JSON_PATH;

    if (!lex.matchToken(Token::RBRACKET, true)) return JSONUTIL_INVALID_JSON_PATH;
    return processFilterResult(result);
}

JsonUtilCode Selector::processFilterResult(jsn::vector<int64_t> &result) {
    JsonUtilCode rc;
    if (node->IsArray()) {
        for (auto idx : result) {
            TRACE("DEBUG", "processFilterResult proceed to array index " << idx << ". remaining path: "
                << lex.p << ", nodePath: " << nodePath)
            rc = evalArrayMember(idx);
            if (isSyntaxError(rc)) return rc;
        }

        // We are done. Null out the current node to signal done of value selections.
        node = nullptr;
        return JSONUTIL_SUCCESS;
    } else if (node->IsObject()) {
        if (result.empty()) {
            // Null out the current node to signal the node is not selected.
            node = nullptr;
        }
        return JSONUTIL_SUCCESS;
    } else {
        if (!result.empty()) {
            ValkeyModule_Assert(result.size() == 1);
            rc = evalMember(*node, lex.p);
            if (isSyntaxError(rc)) return rc;
        }
        node = nullptr;  // We are done addressing this single element.
        return JSONUTIL_SUCCESS;
    }
}

/**
 *   FilterExpr          ::= {SPACE} Term { {SPACE} "||" {SPACE} Term {SPACE} }
 */
JsonUtilCode Selector::parseFilterExpr(jsn::vector<int64_t> &result) {
    CHECK_RECURSION_DEPTH();
    lex.skipSpaces();
    JsonUtilCode rc = parseTerm(result);
    TRACE("DEBUG", "parseFilterExpr parsed first term, rc: " << rc << ", nodePath: " << nodePath)
    if (rc != JSONUTIL_SUCCESS) return rc;

    jsn::unordered_set<int64_t> set;
    bool set_initialized = false;
    jsn::vector<int64_t> v;
    while (lex.matchToken(Token::OR, true)) {
        v.clear();
        rc = parseTerm(v);
        TRACE("DEBUG", "parseFilterExpr parsed OR term, rc: " << rc << ", nodePath: " << nodePath)
        if (rc != JSONUTIL_SUCCESS) return rc;

        if (!set_initialized) {
            set.insert(result.begin(), result.end());
            set_initialized = true;
        }
        vectorUnion(v, result, set);
    }
    lex.skipSpaces();
    return JSONUTIL_SUCCESS;
}

/**
 *   Term                ::= Factor { {SPACE} "&&" {SPACE} Factor }
 */
JsonUtilCode Selector::parseTerm(jsn::vector<int64_t> &result) {
    CHECK_RECURSION_DEPTH();
    JsonUtilCode rc = parseFactor(result);
    if (rc != JSONUTIL_SUCCESS) return rc;
    jsn::vector<int64_t> vec1;
    jsn::vector<int64_t> vec2;
    while (lex.matchToken(Token::AND, true)) {
        vec1.clear();
        vec1.insert(vec1.end(), result.begin(), result.end());
        result.clear();

        vec2.clear();
        rc = parseFactor(vec2);
        if (rc != JSONUTIL_SUCCESS) return rc;
        vectorIntersection(vec1, vec2, result);
    }
    return JSONUTIL_SUCCESS;
}

/**
 *   Factor              ::= ( "@" ( MemberName | ( [ MemberName ] ComparisonOp ComparisonValue) ) ) |
 *                           ( ComparisonValue ComparisonOp "@" ( MemberName | ( [ MemberName ]) ) ) |
 *                           ( {SPACE} "(" FilterExpr ")" {SPACE} )
 *   MemberName          ::= ("." (UnquotedMemberName | BracketedMemberName)) | BracketedMemberName
 *   BracketedMemberName ::= "[" {SPACE} QuotedMemberName {SPACE} "]"
 *   ComparisonOp        ::= {SPACE} "<" | "<="] | ">" | ">=" | "==" | "!=" {SPACE}
 *   ComparisonValue     ::= "null" | Bool | Number | QuotedString | PartialPath
 *   Bool                ::= "true" | "false"
 *   Number              ::= Integer | MemberNameInFilter | ScientificNumber
 *   QuotedString        ::= "\"" {char} "\""
 *   PartialPath         ::= "$" RelativePath
*/
JsonUtilCode Selector::parseFactor(jsn::vector<int64_t> &result) {
    CHECK_RECURSION_DEPTH();
    JsonUtilCode rc;
    lex.skipSpaces();
    if (lex.currToken().type == Token::LPAREN) {
        lex.nextToken(true);  // skip LPAREN
        rc = parseFilterExpr(result);
        if (rc != JSONUTIL_SUCCESS) return rc;
        if (!lex.matchToken(Token::RPAREN, true)) return JSONUTIL_INVALID_JSON_PATH;
    } else {
        if (lex.matchToken(Token::AT)) {
            if (lex.currToken().type == Token::DOT || lex.currToken().type == Token::LBRACKET) {
                // The next token must be member name
                StringViewHelper member_name;
                rc = parseMemberName(member_name);
                if (rc != JSONUTIL_SUCCESS) return rc;

                lex.skipSpaces();
                Token::TokenType tokenType = lex.currToken().type;
                if (tokenType == Token::LT || tokenType == Token::LE ||
                    tokenType == Token::GT || tokenType == Token::GE ||
                    tokenType == Token::EQ || tokenType == Token::NE) {
                    Token::TokenType op = Token::UNKNOWN;
                    rc = parseComparisonOp(op);
                    if (rc != JSONUTIL_SUCCESS) return rc;

                    JValue v;
                    rc = parseComparisonValue(v);
                    if (rc != JSONUTIL_SUCCESS) return rc;

                    return processComparisonExpr(false, member_name, op, v, result);
                } else if (tokenType == Token::LBRACKET) {
                    lex.nextToken(true);  // skip LBRACKET
                    if (lex.currToken().type == Token::QUESTION_MARK) {
                        lex.nextToken(true);  // skip QUESTIONMARK
                        if (!lex.matchToken(Token::LPAREN)) return JSONUTIL_INVALID_JSON_PATH;
                        JValue v;
                        Token::TokenType op = Token::UNKNOWN;
                        if (lex.currToken().type == Token::AT) {
                            lex.nextToken(true);  // skip AT

                            rc = parseComparisonOp(op);
                            if (rc != JSONUTIL_SUCCESS) return rc;

                            rc = parseComparisonValue(v);
                            if (rc != JSONUTIL_SUCCESS) return rc;
                        } else {
                            rc = parseComparisonValue(v);
                            if (rc != JSONUTIL_SUCCESS) return rc;

                            rc = parseComparisonOp(op);
                            if (rc != JSONUTIL_SUCCESS) return rc;
                            rc = swapComparisonOpSide(op);
                            if (rc != JSONUTIL_SUCCESS) return rc;

                            if (!lex.matchToken(Token::AT)) return JSONUTIL_INVALID_JSON_PATH;
                        }
                        return processArrayContains(member_name, op, v, result);
                    } else {
                        int64_t index;
                        rc = parseIndex(index);
                        if (rc != JSONUTIL_SUCCESS) return rc;

                        if (!lex.matchToken(Token::RBRACKET)) return JSONUTIL_INVALID_JSON_PATH;

                        JValue v;
                        Token::TokenType op = Token::UNKNOWN;

                        rc = parseComparisonOp(op);
                        if (rc != JSONUTIL_SUCCESS) return rc;

                        rc = parseComparisonValue(v);
                        if (rc != JSONUTIL_SUCCESS) return rc;

                        return processComparisonExprAtIndex(index, member_name, op, v, result);
                    }
                } else {
                    return processAttributeFilter(member_name, result);
                }
            } else {
                // The next token must be comparison operator
                Token::TokenType op = Token::UNKNOWN;
                rc = parseComparisonOp(op);
                if (rc != JSONUTIL_SUCCESS) return rc;

                JValue v;
                rc = parseComparisonValue(v);
                if (rc != JSONUTIL_SUCCESS) return rc;

                return processComparisonExpr(true, StringViewHelper(), op, v, result);
            }
        } else {  // see if the @.member_name is on the right, do an inverted comparison
            JValue v;
            rc = parseComparisonValue(v);
            if (rc != JSONUTIL_SUCCESS) return rc;

            lex.skipSpaces();
            // The next token must be comparison operator
            Token::TokenType op = Token::UNKNOWN;
            rc = parseComparisonOp(op);
            if (rc != JSONUTIL_SUCCESS) return rc;
            rc = swapComparisonOpSide(op);
            if (rc != JSONUTIL_SUCCESS) return rc;

            if (!lex.matchToken(Token::AT)) return JSONUTIL_INVALID_JSON_PATH;

            lex.skipSpaces();
            if (lex.currToken().type == Token::RPAREN || lex.currToken().type == Token::AND ||
                    lex.currToken().type == Token::OR) {
                return processComparisonExpr(true, StringViewHelper(), op, v, result);
            } else {
                // The next token must be member name
                StringViewHelper member_name;
                rc = parseMemberName(member_name);
                if (rc != JSONUTIL_SUCCESS) return rc;

                if (lex.currToken().type == Token::LBRACKET) {
                    lex.nextToken(true);  // skip LBRACKET
                    int64_t index;
                    rc = parseIndex(index);
                    if (rc != JSONUTIL_SUCCESS) return rc;

                    if (!lex.matchToken(Token::RBRACKET)) return JSONUTIL_INVALID_JSON_PATH;

                    return processComparisonExprAtIndex(index, member_name, op, v, result);
                } else {
                    return processComparisonExpr(false, member_name, op, v, result);
                }
            }
        }
    }
    return JSONUTIL_SUCCESS;
}

/**
 *   MemberName         ::= ("." (UnquotedMemberName | BracketedMemberName)) | BracketedMemberName
 */
JsonUtilCode Selector::parseMemberName(StringViewHelper &name) {
    if (lex.matchToken(Token::DOT)) {
        if (lex.matchToken(Token::LBRACKET))
            return parseBracketedMemberName(name);
        else
            return parseUnquotedMemberName(name);
    } else if (lex.matchToken(Token::LBRACKET)) {
        return parseBracketedMemberName(name);
    } else {
        return JSONUTIL_INVALID_JSON_PATH;
    }
}

/**
 *  BracketedMemberName ::= "[" {SPACE} QuotedMemberName {SPACE} "]"
 */
JsonUtilCode Selector::parseBracketedMemberName(StringViewHelper &member_name) {
    lex.skipSpaces();
    jsn::stringstream ss;
    JsonUtilCode rc = parseQuotedMemberName(ss);
    if (rc != JSONUTIL_SUCCESS) return rc;
    if (!lex.matchToken(Token::RBRACKET, true)) return JSONUTIL_INVALID_JSON_PATH;
    member_name.setInternalString(ss.str());
    return JSONUTIL_SUCCESS;
}

/**
 *   ComparisonValue     ::= "null" | Bool | Number | QuotedString | PartialPath
 *   Bool                ::= "true" | "false"
 *   Number              ::= Integer | MemberNameInFilter | ScientificNumber
 *   QuotedString        ::= "\"" {char} "\""
 *   PartialPath         ::= "$" RelativePath
 */
JsonUtilCode Selector::parseComparisonValue(JValue &v) {
    CHECK_RECURSION_DEPTH();
    StringViewHelper sv;
    const Token &token = lex.currToken();
    if (token.type == Token::DOLLAR) {  // parse and process member name
        Selector selector;
        JsonUtilCode rc = lex.scanPathValue(sv);
        if (rc != JSONUTIL_SUCCESS) return rc;
        jsn::string path = {sv.getView().data(), sv.getView().length()};
        rc = selector.getValues(*root, path.c_str());
        if (rc != JSONUTIL_SUCCESS) return rc;
        if (selector.resultSet.size() != 1 || selector.resultSet[0].first->IsObject() ||
            selector.resultSet[0].first->IsArray()) {
            return JSONUTIL_INVALID_JSON_PATH;
        }
        v.CopyFrom(*selector.resultSet[0].first, allocator);

        return JSONUTIL_SUCCESS;
    } else {  // parse value directly
        JParser parser;
        if (token.type == Token::DOUBLE_QUOTE) {
            JsonUtilCode rc = lex.scanDoubleQuotedString(parser);
            if (rc != JSONUTIL_SUCCESS) return rc;
            v = parser.GetJValue();
            TRACE("DEBUG", "parseComparisonValue ComparisonValue: " << v.GetString())
            return JSONUTIL_SUCCESS;
        } else if (token.type == Token::SINGLE_QUOTE) {
            jsn::stringstream ss;
            JsonUtilCode rc = lex.scanSingleQuotedStringAndConvertToDoubleQuotedString(ss);
            if (rc != JSONUTIL_SUCCESS) return rc;
            sv.setInternalString(ss.str());
        } else if (token.type == Token::ALPHA && (token.strVal == "n")) {
            JsonUtilCode rc = lex.scanIdentifier(sv);
            if (rc != JSONUTIL_SUCCESS) return rc;
            if (sv.getView() != "null") return JSONUTIL_INVALID_IDENTIFIER;
        } else if (token.type == Token::ALPHA && (token.strVal == "t" || token.strVal == "f")) {
            JsonUtilCode rc = lex.scanIdentifier(sv);
            if (rc != JSONUTIL_SUCCESS) return rc;
            if (sv.getView() != "true" && sv.getView() != "false") return JSONUTIL_INVALID_IDENTIFIER;
        } else {
            JsonUtilCode rc = lex.scanNumberInFilterExpr(sv);
            if (rc != JSONUTIL_SUCCESS) return rc;
        }

        if (parser.Parse(sv.getView()).HasParseError()) {
            TRACE("DEBUG", "parseComparisonValue failed to parse " << sv.getView() << ", nodePath: " << nodePath)
            return parser.GetParseErrorCode();
        }
        TRACE("DEBUG", "parseComparisonValue ComparisonValue: ")
#ifdef INSTRUMENT_V2PATH
        dom_dump_value(parser.GetJValue());
#endif
        v = parser.GetJValue();
        return JSONUTIL_SUCCESS;
    }
}

/**
 *   ComparisonOp        := {SPACE} "<" | "<="] | ">" | ">=" | "==" | "!=" {SPACE}
 */
JsonUtilCode Selector::parseComparisonOp(Token::TokenType &op) {
    lex.skipSpaces();
    Token::TokenType tokenType = lex.currToken().type;
    if (tokenType != Token::EQ && tokenType != Token::NE &&
        tokenType != Token::LT && tokenType != Token::LE &&
        tokenType != Token::GT && tokenType != Token::GE)
        return JSONUTIL_INVALID_JSON_PATH;
    op = tokenType;
    lex.skipSpaces();

    lex.nextToken(true);  // advance to the next token
    TRACE("DEBUG", "parseComparisonOp op: " << op << ", curr path: " << lex.p << ", nodePath: " << nodePath)
    return JSONUTIL_SUCCESS;
}

JsonUtilCode Selector::swapComparisonOpSide(Token::TokenType &op) {
    switch (op) {
        case Token::EQ:
        case Token::NE:
            return JSONUTIL_SUCCESS;
        case Token::GT:
            op = Token::LT;
            return JSONUTIL_SUCCESS;
        case Token::LT:
            op = Token::GT;
            return JSONUTIL_SUCCESS;
        case Token::GE:
            op = Token::LE;
            return JSONUTIL_SUCCESS;
        case Token::LE:
            op = Token::GE;
            return JSONUTIL_SUCCESS;
        default:
            return JSONUTIL_INVALID_JSON_PATH;
    }
}

// We can enter an array and see if it contains an object that matches a condition.
// This only looks down one level.
// Further recursion can be investigated in the future.
JsonUtilCode Selector::processArrayContains(const StringViewHelper &member_name, const Token::TokenType op,
                                            const JValue &comparison_value, jsn::vector<int64_t> &result) {
    if (node->IsArray()) {
        for (int64_t i = 0; i < node->Size(); i++) {
            JValue &m = node->GetArray()[i];
            JValue *v;
            if (!m.IsObject()) continue;  // not object, skip
            JValue::MemberIterator it = m.FindMember(member_name.getView());
            if (it == m.MemberEnd()) continue;  // does not have the attribute, skip
            v = &it->value;
            if (v->IsArray()) {
                bool found = false;
                for (int64_t j = 0; j < v->Size() && !found; j++) {
                    JValue &n = v->GetArray()[j];
                    JValue *w = &n;
                    if (evalOp(w, op, comparison_value)) {
                        // note that here we push back parent array element
                        result.push_back(i);
                        found = true;
                    }
                }
            }
        }
    }
    if (!lex.matchToken(Token::RPAREN, true)) return JSONUTIL_INVALID_JSON_PATH;
    if (!lex.matchToken(Token::RBRACKET)) return JSONUTIL_INVALID_JSON_PATH;
    return JSONUTIL_SUCCESS;
}

JsonUtilCode Selector::processComparisonExprAtIndex(const int64_t idx, const StringViewHelper &member_name,
                                                    const Token::TokenType op, const JValue &comparison_value,
                                                    jsn::vector<int64_t> &result) {
    if (node->IsArray()) {
        for (int64_t i = 0; i < node->Size(); i++) {
            JValue &m = node->GetArray()[i];
            JValue *v;
            if (!m.IsObject()) continue;  // not object, skip
            JValue::MemberIterator it = m.FindMember(member_name.getView());
            if (it == m.MemberEnd()) continue;  // does not have the attribute, skip
            v = &it->value;
            if (v->IsArray()) {
                int64_t inner_index = idx;
                // handle negative index
                if (inner_index < 0) inner_index += v->Size();
                // check index bounds
                if (inner_index < static_cast<long long>(v->Size()) && inner_index >= 0) {
                    JValue &n = v->GetArray()[inner_index];
                    JValue *w = &n;
                    if (evalOp(w, op, comparison_value)) {
                        // note that here we push back parent array element
                        result.push_back(i);
                    }
                }
            }
        }
    }
    return JSONUTIL_SUCCESS;
}


JsonUtilCode Selector::processComparisonExpr(const bool is_self, const StringViewHelper &member_name,
                                             const Token::TokenType op, const JValue &comparison_value,
                                             jsn::vector<int64_t> &result) {
    if (node->IsArray()) {
        for (int64_t i = 0; i < node->Size(); i++) {
            JValue &m = node->GetArray()[i];
            JValue *v;
            if (is_self) {
                v = &m;
            } else {
                if (!m.IsObject()) continue;  // not object, skip
                JValue::MemberIterator it = m.FindMember(member_name.getView());
                if (it == m.MemberEnd()) continue;  // does not have the attribute, skip
                v = &it->value;
            }
            if (evalOp(v, op, comparison_value)) result.push_back(i);
        }
    } else if (node->IsObject()){
        JValue::MemberIterator it = node->FindMember(member_name.getView());
        if (it != node->MemberEnd()) {
            if (evalOp(&it->value, op, comparison_value)) result.push_back(0);
        }
    } else if (is_self)  {
        if (evalOp(node, op, comparison_value)) result.push_back(0);
    }
    return JSONUTIL_SUCCESS;
}

bool Selector::evalOp(const JValue *v, const Token::TokenType op, const JValue &comparison_value) {
    // We return false on LHS and RHS value type mismatch, but also treat kTrueType and kFalseType as the same type
    if (v->GetType() != comparison_value.GetType() && !(
            (v->GetType() == rapidjson::kTrueType || v->GetType() == rapidjson::kFalseType) &&
            (comparison_value.GetType() == rapidjson::kTrueType || comparison_value.GetType() == rapidjson::kFalseType))
       ) {
        return false;
    }
    bool satisfied = false;
    switch (op) {
        case Token::EQ: {
            switch (v->GetType()) {
                case rapidjson::kNullType:
                    satisfied = true;
                    break;
                case rapidjson::kTrueType:
                case rapidjson::kFalseType:
                    satisfied = (v->GetBool() == comparison_value.GetBool());
                    break;
                case rapidjson::kStringType:
                    satisfied = (v->GetStringView() == comparison_value.GetStringView());
                    break;
                case rapidjson::kNumberType: {
                    if (v->IsDouble() || comparison_value.IsDouble()) {
                        // It's unsafe to compare floating points using == or !=. Doing so will incur compiler errors.
                        satisfied = (v->GetDouble() <= comparison_value.GetDouble() &&
                                     v->GetDouble() >= comparison_value.GetDouble());
                    } else if (v->IsUint64() && comparison_value.IsUint64()) {
                        satisfied = (v->GetUint64() == comparison_value.GetUint64());
                    } else {
                        satisfied = (v->GetInt64() == comparison_value.GetInt64());
                    }
                    break;
                }
                default:
                    break;
            }
            break;
        }
        case Token::NE: {
            switch (v->GetType()) {
                case rapidjson::kTrueType:
                case rapidjson::kFalseType:
                    satisfied = (v->GetBool() != comparison_value.GetBool());
                    break;
                case rapidjson::kStringType:
                    satisfied = (v->GetStringView() != comparison_value.GetStringView());
                    break;
                case rapidjson::kNumberType: {
                    if (v->IsDouble() || comparison_value.IsDouble()) {
                        // It's unsafe to compare floating points using == or !=. Doing so will incur compiler errors.
                        satisfied = (v->GetDouble() < comparison_value.GetDouble() ||
                                     v->GetDouble() > comparison_value.GetDouble());
                    } else if (v->IsUint64() && comparison_value.IsUint64()) {
                        satisfied = (v->GetUint64() != comparison_value.GetUint64());
                    } else {
                        satisfied = (v->GetInt64() != comparison_value.GetInt64());
                    }
                    break;
                }
                default:
                    break;
            }
            break;
        }
        case Token::LT: {
            switch (v->GetType()) {
                case rapidjson::kTrueType:
                case rapidjson::kFalseType:
                    satisfied = (v->GetBool() < comparison_value.GetBool());
                    break;
                case rapidjson::kStringType:
                    satisfied = (v->GetStringView() < comparison_value.GetStringView());
                    break;
                case rapidjson::kNumberType: {
                    if (v->IsDouble() || comparison_value.IsDouble()) {
                        satisfied = (v->GetDouble() < comparison_value.GetDouble());
                    } else if (v->IsUint64() && comparison_value.IsUint64()) {
                        satisfied = (v->GetUint64() < comparison_value.GetUint64());
                    } else {
                        satisfied = (v->GetInt64() < comparison_value.GetInt64());
                    }
                    break;
                }
                default:
                    break;
            }
            break;
        }
        case Token::LE: {
            switch (v->GetType()) {
                case rapidjson::kTrueType:
                case rapidjson::kFalseType:
                    satisfied = (v->GetBool() <= comparison_value.GetBool());
                    break;
                case rapidjson::kStringType:
                    satisfied = (v->GetStringView() <= comparison_value.GetStringView());
                    break;
                case rapidjson::kNumberType: {
                    if (v->IsDouble() || comparison_value.IsDouble()) {
                        satisfied = (v->GetDouble() <= comparison_value.GetDouble());
                    } else if (v->IsUint64() && comparison_value.IsUint64()) {
                        satisfied = (v->GetUint64() <= comparison_value.GetUint64());
                    } else {
                        satisfied = (v->GetInt64() <= comparison_value.GetInt64());
                    }
                    break;
                }
                default:
                    break;
            }
            break;
        }
        case Token::GT: {
            switch (v->GetType()) {
                case rapidjson::kTrueType:
                case rapidjson::kFalseType:
                    satisfied = (v->GetBool() > comparison_value.GetBool());
                    break;
                case rapidjson::kStringType:
                    satisfied = (v->GetStringView() > comparison_value.GetStringView());
                    break;
                case rapidjson::kNumberType: {
                    if (v->IsDouble() || comparison_value.IsDouble()) {
                        satisfied = (v->GetDouble() > comparison_value.GetDouble());
                    } else if (v->IsUint64() && comparison_value.IsUint64()) {
                        satisfied = (v->GetUint64() > comparison_value.GetUint64());
                    } else {
                        satisfied = (v->GetInt64() > comparison_value.GetInt64());
                    }
                    break;
                }
                default:
                    break;
            }
            break;
        }
        case Token::GE: {
            switch (v->GetType()) {
                case rapidjson::kTrueType:
                case rapidjson::kFalseType:
                    satisfied = (v->GetBool() >= comparison_value.GetBool());
                    break;
                case rapidjson::kStringType:
                    satisfied = (v->GetStringView() >= comparison_value.GetStringView());
                    break;
                case rapidjson::kNumberType: {
                    if (v->IsDouble() || comparison_value.IsDouble()) {
                        satisfied = (v->GetDouble() >= comparison_value.GetDouble());
                    } else if (v->IsUint64() && comparison_value.IsUint64()) {
                        satisfied = (v->GetUint64() >= comparison_value.GetUint64());
                    } else {
                        satisfied = (v->GetInt64() >= comparison_value.GetInt64());
                    }
                    break;
                }
                default:
                    break;
            }
            break;
        }
        default:
            break;
    }
    return satisfied;
}

JsonUtilCode Selector::processAttributeFilter(const StringViewHelper &member_name, jsn::vector<int64_t> &result) {
    if (node->IsArray()) {
        for (int64_t i = 0; i < node->Size(); i++) {
            JValue &m = node->GetArray()[i];
            if (!m.IsObject()) continue;  // skip non-object values
            JValue::MemberIterator it = m.FindMember(member_name.getView());
            if (it == m.MemberEnd()) continue;  // does not have the attribute, skip
            result.push_back(i);
        }
    } else if (node->IsObject()) {
        if (node->FindMember(member_name.getView()) != node->MemberEnd())
            result.push_back(0);
    } else {
        return JSONUTIL_INVALID_JSON_PATH;
    }
    return JSONUTIL_SUCCESS;
}

/**
 * Union vector v with result r, with element order preserved.
 * The final result will be r. Unique elements are stored in the set.
 *
 * This method is optimized for being called multiple times to union n vectors.
 * The caller is responsible for initially syncing up set with r.
 */
void Selector::vectorUnion(const jsn::vector<int64_t> &v, jsn::vector<int64_t> &r,
                           jsn::unordered_set<int64_t> &set) {
    for (auto e : v) {
        auto res = set.emplace(e);
        if (res.second) r.push_back(e);
    }
}

/**
 * Intersect v1 with v2 and store the result in r, with v1's element order preserved.
 */
void Selector::vectorIntersection(const jsn::vector<int64_t> &v1, const jsn::vector<int64_t> &v2,
                                  jsn::vector<int64_t> &r) {
    jsn::unordered_set<int> set(v2.begin(), v2.end());
    for (auto e : v1) {
        if (set.find(e) != set.end()) {
            r.push_back(e);
        }
    }
}

/**
 *   UnionOfIndexes      ::= Integer ({SPACE} "," {SPACE} Integer)+
 */
JsonUtilCode Selector::parseUnionOfIndexes(const int64_t start) {
    if (!node->IsArray()) return JSONUTIL_JSON_ELEMENT_NOT_ARRAY;
    jsn::vector<int64_t> union_indices = {start};
    bool comma = false;
    int64_t index;
    JsonUtilCode rc;

    lex.skipSpaces();
    while (lex.currToken().type != Token::RBRACKET) {
        switch (lex.currToken().type) {
            case Token::COMMA: {
                // cannot have multiple commas in a row
                if (comma) return JSONUTIL_INVALID_JSON_PATH;
                comma = true;
                lex.nextToken(true);  // skip comma
                break;
            }
            default: {
                // integer must follow comma
                if (!comma) return JSONUTIL_INVALID_JSON_PATH;
                comma = false;
                lex.skipSpaces();
                rc = parseIndex(index);
                if (rc != JSONUTIL_SUCCESS) return rc;
                union_indices.push_back(index);
                break;
            }
        }
        lex.skipSpaces();
    }
    // cannot end with comma
    if (comma) return JSONUTIL_INVALID_JSON_PATH;

    if (!lex.matchToken(Token::RBRACKET, true)) return JSONUTIL_INVALID_JSON_PATH;
    rc = processUnion(union_indices);
    if (rc != JSONUTIL_SUCCESS) return rc;

    return JSONUTIL_SUCCESS;
}

JsonUtilCode Selector::processUnion(jsn::vector<int64_t> union_indices) {
    JsonUtilCode rc;
    for (int64_t i : union_indices) {
        // handle negative index
        if (i < 0) i += node->Size();
        // if the index is out of bounds, skip
        if (i < 0 || i > node->Size()-1) continue;
        rc = evalArrayMember(i);
        if (rc != JSONUTIL_SUCCESS) return rc;
    }

    // We are done. Null out the current node to signal completion of value collection.
    node = nullptr;

    return JSONUTIL_SUCCESS;
}

/**
 * Collect values from the result set.
 * @param values OUTPUT parameter, stores collected values.
 */
void Selector::getSelectedValues(jsn::vector<JValue*> &values) const {
    std::transform(resultSet.begin(),
                   resultSet.end(),
                   std::back_inserter(values),
                   [](const std::pair<JValue*, jsn::string> &p) { return p.first; });
}

/**
 * Get unique result set with order preserved.
 */
const jsn::vector<Selector::ValueInfo>& Selector::getUniqueResultSet() {
    if (resultSet.size() <= 1) return resultSet;

    TRACE("DEBUG", "getUniqueResultSet total values: " << resultSet.size());
    uniqueResultSet.clear();
    jsn::unordered_set<JValue*> set;
    for (auto &v : resultSet) {
        auto res = set.emplace(v.first);
        if (res.second) uniqueResultSet.push_back(v);
    }
    TRACE("DEBUG", "getUniqueResultSet unique values: " << uniqueResultSet.size());
    return uniqueResultSet;
}

/**
 * Remove duplicate values from the result set.
 */
void Selector::dedupe() {
    if (resultSet.size() <= 1) return;

    TRACE("DEBUG", "dedupe resultSet size before dedupe: " << resultSet.size());
    auto &rs = getUniqueResultSet();
    resultSet.clear();
    resultSet.insert(resultSet.end(), rs.begin(), rs.end());
    TRACE("DEBUG", "dedupe resultSet size after dedupe: " << resultSet.size());
}
