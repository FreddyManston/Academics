// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef TURTLETOKENIZER_H_
#define TURTLETOKENIZER_H_

#include "../../util/CodePoint.h"
#include "../InputSource.h"

class TurtleTokenizer : private Unmovable {

public:

    enum TokenType { NO_TOKEN, ERROR_TOKEN, EOF_TOKEN, NUMBER, SYMBOL, PNAME_NS, PNAME_LN, LANGUAGE_TAG, NON_SYMBOL, VARIABLE, QUOTED_IRI, QUOTED_STRING, BLANK_NODE };

protected:

    const static size_t TOKEN_BUFFER_INCREMENT = 1024;

    InputSource* m_inputSource;

    // Current position
    bool m_isAtEOF;
    CodePoint m_currentCodePoint;
    size_t m_currentCodePointLine;
    size_t m_currentCodePointColumn;

    // Saved position
    std::unique_ptr<InputSource::Position> m_savedInputSourcePosition;
    bool m_savedIsAtEOF;
    CodePoint m_savedCurrentCodePoint;
    size_t m_savedCurrentCodePointLine;
    size_t m_savedCurrentCodePointColumn;

    // Token stuff
    TokenType m_tokenType;
    std::unique_ptr<uint8_t[]> m_tokenBuffer;
    size_t m_tokenBufferSize;
    size_t m_tokenBufferAfterLast;
    size_t m_tokenStartLine;
    size_t m_tokenStartColumn;

    void savePosition();

    void restorePosition();

    void extendTokenBuffer();

    void clearTokenBuffer();

    void appendToToken(const CodePoint codePoint);

    bool addNextByteToCodePoint(const uint8_t shift);

    void loadNextCodePoint();

    void nextCodePoint();

    always_inline CodePoint getCurrentCodePoint() const {
        return m_currentCodePoint;
    }

    always_inline bool isAtEOF() const {
        return m_isAtEOF;
    }
    
    CodePoint readHexEncodedCodePoint(const uint8_t numberOfDigits);

    bool parseQuotedIRI();

    bool parseQuotedString();

    void readTokenWithDots(const uint8_t* const codePointClass);

    bool readCodePoint(const CodePoint codePoint);

    bool readCodePoint(const uint8_t* const codePointClass);

    bool read_HEX(CodePoint& codePoint);

    bool read_PERCENT();

    bool read_PN_LOCAL_ESC();

public:

    TurtleTokenizer();

    always_inline InputSource& getInputSource() {
        return *m_inputSource;
    }

    always_inline bool isGood() const {
        return m_tokenType != EOF_TOKEN && m_tokenType != ERROR_TOKEN;
    }

    always_inline bool isNoToken() const {
        return m_tokenType == NO_TOKEN;
    }

    always_inline bool isErrorToken() const {
        return m_tokenType == ERROR_TOKEN;
    }

    always_inline bool isEOF() const {
        return m_tokenType == EOF_TOKEN;
    }

    always_inline bool isNumber() const {
        return m_tokenType == NUMBER;
    }

    always_inline bool isSymbol() const {
        return m_tokenType == SYMBOL;
    }

    always_inline bool is_PNAME_NS() const {
        return m_tokenType == PNAME_NS;
    }

    always_inline bool is_PNAME_LN() const {
        return m_tokenType == PNAME_LN;
    }

    always_inline bool isLanguageTag() const {
        return m_tokenType == LANGUAGE_TAG;
    }

    always_inline bool isNonSymbol() const {
        return m_tokenType == NON_SYMBOL;
    }

    always_inline bool isVariable() const {
        return m_tokenType == VARIABLE;
    }

    always_inline bool isQuotedIRI() const {
        return m_tokenType == QUOTED_IRI;
    }

    always_inline bool isQuotedString() const {
        return m_tokenType == QUOTED_STRING;
    }

    always_inline bool isBlankNode() const {
        return m_tokenType == BLANK_NODE;
    }

    always_inline TokenType getTokenType() const {
        return m_tokenType;
    }

    always_inline uint8_t getTokenByte(const size_t offset = 0) {
        return m_tokenBuffer[offset];
    }

    always_inline std::string getToken(const size_t offset = 0) {
        std::string token;
        getToken(token, offset);
        return token;
    }

    always_inline void getToken(std::string& token, const size_t offset = 0) const {
        token.clear();
        token.append(reinterpret_cast<const char*>(m_tokenBuffer.get() + offset), m_tokenBufferAfterLast - offset);
    }

    always_inline void appendToken(std::string& token, const size_t offset = 0) const {
        token.append(reinterpret_cast<const char*>(m_tokenBuffer.get() + offset), m_tokenBufferAfterLast - offset);
    }

    always_inline bool tokenEquals(const TokenType tokenType, const char c) const {
        return m_tokenType == tokenType && tokenEqualsNoType(c);
    }

    always_inline bool tokenEqualsNoType(const char c) const {
        return m_tokenBufferAfterLast == 1 &&  m_tokenBuffer[0] == c;
    }

    always_inline bool tokenEquals(const TokenType tokenType, const char* const string) const {
        return m_tokenType == tokenType && tokenEqualsNoType(string);
    }

    always_inline bool tokenEqualsNoType(const char* string) const {
        const uint8_t* scan = m_tokenBuffer.get();
        for (size_t index = 0; index < m_tokenBufferAfterLast; index++) {
            if (*(scan++) != *(string++))
                return false;
        }
        return *string == 0;
    }

    always_inline bool lowerCaseTokenEquals(const TokenType tokenType, const char* const string) const {
        return m_tokenType == tokenType && lowerCaseTokenEqualsNoType(string);
    }

    always_inline bool lowerCaseTokenEqualsNoType(const char* string) const {
        const uint8_t* scan = m_tokenBuffer.get();
        for (size_t index = 0; index < m_tokenBufferAfterLast; index++) {
            if (::tolower(*(scan++)) != *(string++))
                return false;
        }
        return *string == 0;
    }

    always_inline bool nonSymbolTokenEquals(const char c) const {
        return tokenEquals(NON_SYMBOL, c);
    }

    always_inline bool nonSymbolTokenEquals(const char* const string) const {
        return tokenEquals(NON_SYMBOL, string);
    }

    always_inline bool symbolTokenEquals(const char* const string) const {
        return tokenEquals(SYMBOL, string);
    }

    always_inline bool symbolLowerCaseTokenEquals(const char* const string) const {
        return lowerCaseTokenEquals(SYMBOL, string);
    }

    always_inline size_t getCurrentCodePointLine() const {
        return m_currentCodePointLine;
    }

    always_inline size_t getCurrentCodePointColumn() const {
        return m_currentCodePointColumn;
    }

    always_inline size_t getTokenStartLine() const {
        return m_tokenStartLine;
    }

    always_inline size_t getTokenStartColumn() const {
        return m_tokenStartColumn;
    }

    void initialize(InputSource& inputSource);

    void deinitialize();

    void nextToken();

    void doNextCodePoint();

    void recover();

};

#endif // TURTLETOKENIZER_H_
