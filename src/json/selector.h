#ifndef VALKEYJSONMODULE_JSON_SELECTOR_H_
#define VALKEYJSONMODULE_JSON_SELECTOR_H_

#include "json/dom.h"
#include "json/rapidjson_includes.h"
#include <string_view>

struct Token {
    enum TokenType {
        UNKNOWN = 0,
        DOLLAR, DOT, DOTDOT, WILDCARD,
        COLON, COMMA, AT, QUESTION_MARK,
        LBRACKET, RBRACKET, LPAREN, RPAREN,
        SINGLE_QUOTE, DOUBLE_QUOTE,
        PLUS, MINUS, DIV, PCT,
        EQ, NE, GT, LT, GE, LE, NOT, ASSIGN,
        ALPHA, DIGIT, SPACE,
        TRUE, FALSE, AND, OR,
        SPECIAL_CHAR,
        END
    };

    Token()
            : type(Token::UNKNOWN)
            , strVal()
    {}
    TokenType type;
    std::string_view strVal;
};

/**
 * A helper class that contains a string view and an optional internal string. The caller decides if the view
 * is a view of an external string or the internal string.
 * If StringViewHelper::str is empty, the underlying string is owned by an external resource.
 * Otherwise, the underlying string is owned by StringViewHelper.
 */
struct StringViewHelper {
    StringViewHelper() : str(), view() {}
    StringViewHelper(const StringViewHelper &svh) {
        str = svh.str;
        if (str.empty())
            view = svh.view;
        else
            view = std::string_view(str.c_str(), str.length());
    }
    const std::string_view& getView() const { return view; }
    void setInternalString(const jsn::string &s) {
        str = s;
        view = std::string_view(str.c_str(), str.length());
    }
    void setInternalView(const std::string_view &v) {
        str = jsn::string(v);
        view = std::string_view(str.c_str(), str.length());
    }
    void setExternalView(const std::string_view &sv) {
        view = sv;
    }

 private:
    jsn::string str;
    std::string_view view;
    StringViewHelper& operator=(const StringViewHelper&);  // disable assignment operator
};

class Lexer {
 public:
    Lexer()
            : p(nullptr)
            , next()
            , path(nullptr)
            , rdTokens(0)
    {}
    void init(const char *path);
    Token::TokenType peekToken() const;
    Token nextToken(const bool skipSpace = false);
    const Token& currToken() const { return next; }
    bool matchToken(const Token::TokenType type, const bool skipSpace = false);
    JsonUtilCode scanInteger(int64_t &val);
    JsonUtilCode scanUnquotedMemberName(StringViewHelper &member_name);
    JsonUtilCode scanPathValue(StringViewHelper &output);
    JsonUtilCode scanDoubleQuotedString(JParser& parser);
    JsonUtilCode scanDoubleQuotedString(jsn::stringstream &ss);
    JsonUtilCode scanSingleQuotedString(jsn::stringstream &ss);
    JsonUtilCode scanSingleQuotedStringAndConvertToDoubleQuotedString(jsn::stringstream &ss);
    JsonUtilCode scanNumberInFilterExpr(StringViewHelper &number_sv);
    JsonUtilCode scanIdentifier(StringViewHelper &sv);
    void skipSpaces();
    void unescape(const std::string_view &input, jsn::stringstream &ss);
    size_t getRecursiveDescentTokens() { return rdTokens; }
    const char *p;  // current position in path
    Token next;

 private:
    Lexer(const Lexer &t);  // disable copy constructor
    Lexer& operator=(const Lexer &rhs);  // disable assignment constructor
    int64_t scanUnsignedInteger();
    const char *path;
    size_t rdTokens;  // number of recursive descent tokens
};

/**
 * A JSONPath parser and evaluator that supports both v2 JSONPath and the legacy path syntax, and operates in either
 * READ or WRITE mode. It is named Selector because:
 * a) For READ, evaluation means selecting a list of values that match the query.
 * b) For WRITE, evaluation means selecting a list of values to be updated and places to insert into.
 *
 * The selector is designed to work with a vector of values instead of a single value, and support both v1 and v2
 * path syntax.
 *
 * Internally, it maintains two pointers. One points to the current node (value) in the JSON tree. The other points
 * to the current position in the path string.
 *
 * The selector automatically detects if the input path is v1 or v2 syntax, and sets the member isV2Path.
 * Member mode indicates READ/INSERT/UPDATE/DELETE mode, which is automatically set based on the entry point method
 * being invoked, which is getValues or setValues or deleteValues.
 *
 * 1. READ mode:
 *    Selector selector;
 *    JsonUtilCode rc = selector.getValues(doc, path);
 *
 *    The outcome is a result set (selector.resultSet) that matches the query. Each entry is a (value, valuePath) pair.
 *
 * 2. WRITE mode:
 * 2.1. Insert/Update:
 *    Selector selector;
 *    JsonUtilCode rc = selector.setValues(doc, path, new_val);
 *
 *    The outcome is 2 collections:
 *    a) selector.resultSet: values to update. Each entry is a (value, valuePath) pair.
 *    b) selector.insertPaths: set of insert paths.
 *
 *    Note that setValues takes care of everything (update/insert). As an option, the caller can inspect these vectors
 *    for verification purpose.
 *
 * 2.2. Delete:
 *    Selector selector;
 *    JsonUtilCode rc = selector.deleteValues(doc, path, numValsDeleted);
 *
 *    The outcome is selector.resultSet, representing values to delete. Each entry is a (value, valuePath) pair.
 *
 * NOTE:
 *    a) Inserting into an array value is not allowed. (That's the job of JSON.ARRINSERT and JSON.ARRAPPEND)
 *    b) A new key can be appended to an object if and only if the key is the last child in the path.
 */
class Selector {
 public:
    explicit Selector(bool force_v2_path_behavior = false)
            : isV2Path(force_v2_path_behavior)
            , root(nullptr)
            , node(nullptr)
            , nodePath()
            , lex()
            , maxPathDepth(0)
            , currPathDepth(0)
            , resultSet()
            , insertPaths()
            , uniqueResultSet()
            , mode(READ)
            , isRecursiveSearch(false)
            , error(JSONUTIL_SUCCESS)
    {}

    // ValueInfo - (value, path) pair.
    //   first:  JValue pointer
    //   second: path to the value, which is in json pointer format.
    typedef std::pair<JValue*, jsn::string> ValueInfo;

    /**
     * Entry point for READ query.
     * The outcome is selector.resultSet that matches the query. Each entry is a (value, valuePath) pair.
     */
    JsonUtilCode getValues(JValue &root, const char *path);

    /**
     * Entry point for DELETE.
     * The outcome is selector.resultSet that matches the query. Each entry is a (value, valuePath) pair.
     */
    JsonUtilCode deleteValues(JValue &root, const char *path, size_t &numValsDeleted);

    /**
     * Entry point for a single stage INSERT/UPDATE, which commits the operation.
     * The outcome is 2 vectors:
     *   1) selector.resultSet: values to update. Each entry is a (value, valuePath) pair.
     *   2) selector.insertPaths: set of insert paths.
     */
    JsonUtilCode setValues(JValue &root, const char *path, JValue &new_val);
    /**
     * Prepare for a 2-stage INSERT/UPDATE. The 2-stage write splits a write operation into two calls:
     * prepareSetValues and commit, where prepareSetValues does not change the Valkey data.
     * The purpose of having a 2-stage write is to be able to discard the write operation if
     * certain conditions are not satisfied.
     */
    JsonUtilCode prepareSetValues(JValue &root, const char *path);
    /**
     * Commit a 2-stage INSERT/UPDATE.
     */
    JsonUtilCode commit(JValue &new_val);
    bool isLegacyJsonPathSyntax() const { return !isV2Path; }
    bool isSyntaxError(JsonUtilCode code) const;

    /**
     * Given a list of paths, check if there is at least one path that is v2 JSONPath.
     */
    static bool has_at_least_one_v2path(const char **paths, const int num_paths) {
        for (int i = 0; i < num_paths; i++) {
            if (*paths[i] == '$') return true;
        }
        return false;
    }

    bool hasValues() const { return !resultSet.empty(); }
    bool hasUpdates() const { return !resultSet.empty(); }
    bool hasInserts() const { return !insertPaths.empty(); }
    size_t getMaxPathDepth() const { return maxPathDepth; }
    const jsn::vector<ValueInfo>& getResultSet() const { return resultSet; }
    void getSelectedValues(jsn::vector<JValue*> &values) const;
    const jsn::vector<Selector::ValueInfo>& getUniqueResultSet();
    void dedupe();

    bool isV2Path;  // if false, it's legacy syntax

 private:
    enum Mode {
        READ,
        INSERT,
        UPDATE,
        INSERT_OR_UPDATE,  // JSON.SET could be insert or update or both
        DELETE
    };

    struct State {
        State()
                : currNode(nullptr)
                , nodePath()
                , currPathPtr(nullptr)
                , currToken()
                , currPathDepth(0)
        {}
        JValue *currNode;
        jsn::string nodePath;
        const char *currPathPtr;
        Token currToken;
        size_t currPathDepth;
    };
    void snapshotState(State &state) {
        state.currNode = node;
        state.nodePath = nodePath;
        state.currPathPtr = lex.p;
        state.currToken = lex.next;
        state.currPathDepth = currPathDepth;
    }
    void restoreState(const State &state) {
        node = state.currNode;
        nodePath = state.nodePath;
        lex.p = state.currPathPtr;
        lex.next = state.currToken;
        currPathDepth = state.currPathDepth;
    }

    /***
     * Initialize the selector.
     */
    JsonUtilCode init(JValue &root, const char *path, const Mode mode);

    void resetPointers(JValue &currVal, const char *currPath) {
        node = &currVal;
        lex.p = currPath;
    }

    void incrPathDepth() {
        currPathDepth++;
        maxPathDepth = std::max(maxPathDepth, currPathDepth);
    }

    void decrPathDepth() {
        ValkeyModule_Assert(currPathDepth > 0);
        currPathDepth--;
    }

    /***
     * Evaluate the path, which includes parsing and evaluating the path.
     */
    JsonUtilCode eval();
    JsonUtilCode evalMember(JValue &m, const char *path_start);
    JsonUtilCode evalObjectMember(const StringViewHelper &member_name, JValue &val);
    JsonUtilCode evalArrayMember(int64_t idx);
    JsonUtilCode traverseToObjectMember(const StringViewHelper &member_name);
    JsonUtilCode traverseToArrayIndex(int64_t idx);
    JsonUtilCode parseSupportedPath();
    JsonUtilCode parseRelativePath();
    JsonUtilCode parseRecursivePath();
    JsonUtilCode parseDotPath();
    JsonUtilCode parseBracketPath();
    JsonUtilCode parseQualifiedPath();
    JsonUtilCode parseQualifiedPathElement();
    JsonUtilCode parseKey();
    JsonUtilCode parseBracketPathElement();
    JsonUtilCode parseWildcardInBrackets();
    JsonUtilCode parseNameInBrackets();
    JsonUtilCode parseQuotedMemberName(jsn::stringstream &ss);
    JsonUtilCode parseUnquotedMemberName(StringViewHelper &name);
    JsonUtilCode parseIndexExpr();
    JsonUtilCode parseSliceStartsWithColon();
    JsonUtilCode parseSliceStartsWithInteger(const int64_t start);
    JsonUtilCode parseSliceOrUnionOrIndex();
    JsonUtilCode parseEndAndStep(const int64_t start);
    JsonUtilCode parseStep(const int64_t start, const int64_t end);
    JsonUtilCode parseIndex(int64_t &val);
    JsonUtilCode parseFilter();
    JsonUtilCode parseFilterExpr(jsn::vector<int64_t> &result);
    JsonUtilCode parseTerm(jsn::vector<int64_t> &result);
    JsonUtilCode parseFactor(jsn::vector<int64_t> &result);
    JsonUtilCode parseMemberName(StringViewHelper &name);
    JsonUtilCode parseBracketedMemberName(StringViewHelper &member_name);
    JsonUtilCode parseComparisonValue(JValue &v);
    JsonUtilCode parseComparisonOp(Token::TokenType &op);
    JsonUtilCode swapComparisonOpSide(Token::TokenType &op);
    JsonUtilCode processUnionOfMembers(const jsn::vector<jsn::string> &member_names);
    JsonUtilCode parseUnionOfIndexes(const int64_t fistIndex);
    JsonUtilCode processWildcard();
    JsonUtilCode processWildcardKey();
    JsonUtilCode processWildcardIndex();
    JsonUtilCode parseWildcardFilter();
    JsonUtilCode processSubscript(const int64_t idx);
    JsonUtilCode processSlice(int64_t start, int64_t end, const int64_t step = 1);
    JsonUtilCode processUnion(jsn::vector<int64_t> union_indices);
    JsonUtilCode processFilterResult(jsn::vector<int64_t> &result);
    JsonUtilCode processArrayContains(const StringViewHelper &member_name, const Token::TokenType op,
                                      const JValue &comparison_value, jsn::vector<int64_t> &result);
    JsonUtilCode processComparisonExpr(const bool is_self, const StringViewHelper &member_name,
                                       const Token::TokenType op, const JValue &comparison_value,
                                       jsn::vector<int64_t> &result);
    JsonUtilCode processComparisonExprAtIndex(const int64_t idx, const StringViewHelper &member_name,
                                              const Token::TokenType op, const JValue &comparison_value,
                                              jsn::vector<int64_t> &result);
    JsonUtilCode processAttributeFilter(const StringViewHelper &member_name, jsn::vector<int64_t> &result);
    bool evalOp(const JValue *v, const Token::TokenType op, const JValue &comparison_value);
    bool deleteValue(const jsn::string &path);
    void vectorUnion(const jsn::vector<int64_t> &v, jsn::vector<int64_t> &r, jsn::unordered_set<int64_t> &set);
    void vectorIntersection(const jsn::vector<int64_t> &v1, const jsn::vector<int64_t> &v2,
                            jsn::vector<int64_t> &result);
    JsonUtilCode recursiveSearch(JValue &v, const char *p);
    void setError(const JsonUtilCode error_code) { error = error_code; }
    JsonUtilCode getError() const { return error; }

    JValue *root;          // the root value, aka the document
    JValue *node;          // current node (value) in the JSON tree
    jsn::string nodePath;  // current node's path, which is in json pointer format
    Lexer lex;
    size_t maxPathDepth;
    size_t currPathDepth;

    // resultSet - selected values that match the query. In write mode, these are the source values to update
    // or delete.
    jsn::vector<ValueInfo> resultSet;

    // insertPaths - set of insert paths, which is in json pointer path format.
    // Only used for write operations that generate INSERTs.
    jsn::unordered_set<jsn::string> insertPaths;

    // data structure to assist dedupe
    jsn::vector<ValueInfo> uniqueResultSet;

    Mode mode;
    bool isRecursiveSearch;  // if we are doing a recursive search we do not wish to add new fields
    JsonUtilCode error;  // JSONUTIL_SUCCESS indicates no error
};

#endif
