// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "TurtleTokenizer.h"
#include "TurtleSyntax.h"

always_inline static bool isNumber(const CodePoint codePoint) {
    return '0' <= codePoint && codePoint <= '9';
}

always_inline static bool isAlpha(const CodePoint codePoint) {
    return ('A' <= codePoint && codePoint <= 'Z') || ('a' <= codePoint && codePoint <= 'z');
}

always_inline static bool isAlphaNum(const CodePoint codePoint) {
    return ::isAlpha(codePoint) || ::isNumber(codePoint);
}

always_inline void TurtleTokenizer::savePosition() {
    m_inputSource->savePosition(*m_savedInputSourcePosition);
    m_savedIsAtEOF = m_isAtEOF;
    m_savedCurrentCodePoint = m_currentCodePoint;
    m_savedCurrentCodePointLine = m_currentCodePointLine;
    m_savedCurrentCodePointColumn = m_currentCodePointColumn;
}

always_inline void TurtleTokenizer::restorePosition() {
    m_inputSource->restorePosition(*m_savedInputSourcePosition);
    m_isAtEOF = m_savedIsAtEOF;
    m_currentCodePoint = m_savedCurrentCodePoint;
    m_currentCodePointLine = m_savedCurrentCodePointLine;
    m_currentCodePointColumn = m_savedCurrentCodePointColumn;
}

void TurtleTokenizer::extendTokenBuffer() {
    size_t newTokenBufferSize = m_tokenBufferSize + TOKEN_BUFFER_INCREMENT;
    std::unique_ptr<uint8_t[]> newTokenBuffer(new uint8_t[newTokenBufferSize]);
    ::memcpy(newTokenBuffer.get(), m_tokenBuffer.get(), m_tokenBufferSize);
    m_tokenBuffer.swap(newTokenBuffer);
    m_tokenBufferSize = newTokenBufferSize;
}

always_inline void TurtleTokenizer::clearTokenBuffer() {
    m_tokenBufferAfterLast = 0;
}

always_inline void TurtleTokenizer::appendToToken(const CodePoint codePoint) {
    if (m_tokenBufferAfterLast + MAX_BYTES_PER_CODE_POINT >= m_tokenBufferSize)
        extendTokenBuffer();
    m_tokenBufferAfterLast += ::writeCodePoint(m_tokenBuffer.get() + m_tokenBufferAfterLast, codePoint);
}

always_inline bool TurtleTokenizer::addNextByteToCodePoint(const uint8_t shift) {
    if (!m_inputSource->isAtEOF()) {
        const uint8_t currentByte = m_inputSource->getCurrentByte();
        if ((currentByte & 0xC0u) == 0x80u) {
            m_currentCodePoint |= (static_cast<CodePoint>(currentByte & 0x3Fu) << shift);
            m_inputSource->advanceByOne();
            return false;
        }
    }
    return true;
}

always_inline void TurtleTokenizer::loadNextCodePoint() {
    if (m_inputSource->isAtEOF())
        m_isAtEOF = true;
    else {
        const uint8_t leadByte = m_inputSource->getCurrentByte();
        m_inputSource->advanceByOne();
        if ((leadByte & 0x80u) == 0)
            m_currentCodePoint = leadByte;
        else {
            bool error;
            if ((leadByte & 0xE0u) == 0xC0u) {
                m_currentCodePoint = (static_cast<CodePoint>(leadByte & 0x1Fu) << 6);
                error = addNextByteToCodePoint(0);
            }
            else if ((leadByte & 0xF0u) == 0xE0u) {
                m_currentCodePoint = (static_cast<CodePoint>(leadByte & 0x0Fu) << 12);
                error = addNextByteToCodePoint(6) || addNextByteToCodePoint(0);
            }
            else if ((leadByte & 0xF8u) == 0xF0u) {
                m_currentCodePoint = (static_cast<CodePoint>(leadByte & 0x07u) << 18);
                error = addNextByteToCodePoint(12) || addNextByteToCodePoint(6) || addNextByteToCodePoint(0);
            }
            else
                error = true;
            if (error) {
                m_currentCodePoint = leadByte;
                while (!m_inputSource->isAtEOF() && (m_inputSource->getCurrentByte() & 0x80u) != 0)
                    m_inputSource->advanceByOne();
            }
        }
    }
}

always_inline void TurtleTokenizer::nextCodePoint() {
    assert(!isAtEOF());
    if (getCurrentCodePoint() == '\n') {
        ++m_currentCodePointLine;
        m_currentCodePointColumn = 1;
    }
    else
        ++m_currentCodePointColumn;
    loadNextCodePoint();
}

always_inline CodePoint TurtleTokenizer::readHexEncodedCodePoint(const uint8_t numberOfDigits) {
    CodePoint codePoint = 0;
    for (uint8_t digitIndex = 0; digitIndex < numberOfDigits; ++digitIndex) {
        if (isAtEOF()) {
            m_tokenType = ERROR_TOKEN;
            return 0;
        }
        const CodePoint digitCodePoint = getCurrentCodePoint();
        nextCodePoint();
        if (::isNumber(digitCodePoint))
            codePoint = codePoint * 16 + (digitCodePoint - '0');
        else if ('a' <= digitCodePoint && digitCodePoint <= 'f')
            codePoint = codePoint * 16 + (digitCodePoint - 'a') + 10;
        else if ('A' <= digitCodePoint && digitCodePoint <= 'F')
            codePoint = codePoint * 16 + (digitCodePoint - 'A') + 10;
        else {
            m_tokenType = ERROR_TOKEN;
            return 0;
        }
    }
    return codePoint;
}

always_inline bool TurtleTokenizer::parseQuotedIRI() {
    while (!isAtEOF()) {
        const CodePoint current = getCurrentCodePoint();
        nextCodePoint();
        if (current == '>') {
            m_tokenType = QUOTED_IRI;
            return true;
        }
        else if (current == '\\') {
            if (isAtEOF()) {
                m_tokenType = ERROR_TOKEN;
                return true;
            }
            else {
                const CodePoint escaped = getCurrentCodePoint();
                nextCodePoint();
                switch (escaped) {
                case 'u':
                    appendToToken(readHexEncodedCodePoint(4));
                    break;
                case 'U':
                    appendToToken(readHexEncodedCodePoint(8));
                    break;
                default:
                    m_tokenType = ERROR_TOKEN;
                    return true;
                }
            }
        }
        else if (!TurtleSyntax::is_IRIREF(current))
            return false;
        else
            appendToToken(current);
    }
    return false;
}

bool TurtleTokenizer::parseQuotedString() {
    const CodePoint quoteType = getCurrentCodePoint();
    nextCodePoint();
    bool inLongString;
    if (isAtEOF())
        return false;
    if (getCurrentCodePoint() == quoteType) {
        nextCodePoint();
        if (isAtEOF() || getCurrentCodePoint() != quoteType)
            return true;
        nextCodePoint();
        inLongString = true;
    }
    else
        inLongString = false;
    while (!isAtEOF()) {
        CodePoint current = getCurrentCodePoint();
        nextCodePoint();
        switch (current) {
        case '\'':
        case '\"':
            if (current == quoteType) {
                if (inLongString) {
                    if (isAtEOF())
                        return false;
                    if (getCurrentCodePoint() == quoteType) {
                        nextCodePoint();
                        if (isAtEOF())
                            return false;
                        if (getCurrentCodePoint() == quoteType) {
                            nextCodePoint();
                            return true;
                        }
                        else {
                            appendToToken(current);
                            appendToToken(current);
                        }
                    }
                    else
                        appendToToken(current);
                }
                else
                    return true;
            }
            else
                appendToToken(current);
            break;
        case '\\':
            if (isAtEOF())
                return false;
            current = getCurrentCodePoint();
            nextCodePoint();
            switch (current) {
            case 't':
                appendToToken('\t');
                break;
            case 'b':
                appendToToken('\b');
                break;
            case 'n':
                appendToToken('\n');
                break;
            case 'r':
                appendToToken('\r');
                break;
            case 'f':
                appendToToken('\f');
                break;
            case '\"':
                appendToToken('\"');
                break;
            case '\'':
                appendToToken('\'');
                break;
            case '\\':
                appendToToken('\\');
                break;
            case 'u':
                appendToToken(readHexEncodedCodePoint(4));
                break;
            case 'U':
                appendToToken(readHexEncodedCodePoint(8));
                break;
            default:
                return false;
            }
            break;
        case '\r':
        case '\n':
            if (!inLongString)
                return false;
            // Deliberate fall-through!
        default:
            appendToToken(current);
            break;
        }
    }
    return false;
}

always_inline void TurtleTokenizer::readTokenWithDots(const uint8_t* const codePointClass) {
    while (!isAtEOF()) {
        if (TurtleSyntax::isInCodePointClass(codePointClass, getCurrentCodePoint())) {
            appendToToken(getCurrentCodePoint());
            nextCodePoint();
        }
        else if (getCurrentCodePoint() == '.') {
            savePosition();
            size_t numberOfDots = 0;
            while (!isAtEOF() && getCurrentCodePoint() == '.') {
                nextCodePoint();
                ++numberOfDots;
            }
            if (!isAtEOF() && TurtleSyntax::isInCodePointClass(codePointClass, getCurrentCodePoint())) {
                for (size_t index = 0; index < numberOfDots; ++index)
                    appendToToken('.');
            }
            else {
                restorePosition();
                return;
            }
        }
        else
            return;
    }

}

always_inline bool TurtleTokenizer::readCodePoint(const CodePoint codePoint) {
    if (!isAtEOF() && getCurrentCodePoint() == codePoint) {
        appendToToken(codePoint);
        nextCodePoint();
        return true;
    }
    else
        return false;
}

always_inline bool TurtleTokenizer::readCodePoint(const uint8_t* codePointClass) {
    if (!isAtEOF() && TurtleSyntax::isInCodePointClass(codePointClass, getCurrentCodePoint())) {
        appendToToken(getCurrentCodePoint());
        nextCodePoint();
        return true;
    }
    else
        return false;
}

always_inline bool TurtleTokenizer::read_HEX(CodePoint& codePoint) {
    if (!isAtEOF() && (::isNumber(getCurrentCodePoint()) || ('a' <= getCurrentCodePoint() && getCurrentCodePoint() <= 'f') || ('A' <= getCurrentCodePoint() && getCurrentCodePoint() <= 'F'))) {
        codePoint = getCurrentCodePoint();
        nextCodePoint();
        return true;
    }
    else
        return false;
}

always_inline bool TurtleTokenizer::read_PERCENT() {
    if (!isAtEOF() && getCurrentCodePoint() == '%') {
        savePosition();
        nextCodePoint();
        CodePoint codePoint1;
        CodePoint codePoint2;
        if (read_HEX(codePoint1) && read_HEX(codePoint2)) {
            appendToToken('%');
            appendToToken(codePoint1);
            appendToToken(codePoint2);
            return true;
        }
        restorePosition();
    }
    return false;
}

always_inline bool TurtleTokenizer::read_PN_LOCAL_ESC() {
    if (getCurrentCodePoint() == '\\') {
        savePosition();
        nextCodePoint();
        if (TurtleSyntax::is_PN_LOCAL_ESC(getCurrentCodePoint())) {
            appendToToken('\\');
            appendToToken(getCurrentCodePoint());
            nextCodePoint();
            return true;
        }
        restorePosition();
    }
    return false;
}

TurtleTokenizer::TurtleTokenizer() :
    m_inputSource(),
    m_isAtEOF(true),
    m_currentCodePoint(0),
    m_currentCodePointLine(0),
    m_currentCodePointColumn(0),
    m_savedInputSourcePosition(),
    m_savedIsAtEOF(true),
    m_savedCurrentCodePoint(0),
    m_savedCurrentCodePointLine(0),
    m_savedCurrentCodePointColumn(0),
    m_tokenType(NO_TOKEN),
    m_tokenBuffer(new uint8_t[TOKEN_BUFFER_INCREMENT]),
    m_tokenBufferSize(TOKEN_BUFFER_INCREMENT),
    m_tokenBufferAfterLast(0),
    m_tokenStartLine(0),
    m_tokenStartColumn(0)
{
}

void TurtleTokenizer::initialize(InputSource& inputSource) {
    m_inputSource = &inputSource;
    m_isAtEOF = false;
    m_currentCodePoint = 0;
    m_currentCodePointLine = 1;
    m_currentCodePointColumn = 1;
    m_savedInputSourcePosition = m_inputSource->createPosition();
    m_tokenType = NO_TOKEN;
    m_tokenBufferAfterLast = 0;
    m_tokenStartLine = 0;
    m_tokenStartColumn = 0;
    loadNextCodePoint();
}

void TurtleTokenizer::deinitialize() {
    m_inputSource = 0;
    m_savedInputSourcePosition.reset(0);
    m_tokenType = NO_TOKEN;
    m_tokenBufferAfterLast = 0;
    m_tokenStartLine = 0;
    m_tokenStartColumn = 0;
}

void TurtleTokenizer::nextToken() {
    clearTokenBuffer();
    // skip whitespace first
    bool isInComment = false;
    while (true) {
        if (isAtEOF()) {
            m_tokenType = EOF_TOKEN;
            return;
        }
        if (isInComment) {
            if (getCurrentCodePoint() == '\n')
                isInComment = false;
        }
        else if (getCurrentCodePoint() == '#')
            isInComment = true;
        else if (!TurtleSyntax::isWhitespace(getCurrentCodePoint()))
            break;
        nextCodePoint();
    }
    // now process the token
    m_tokenStartLine = m_currentCodePointLine;
    m_tokenStartColumn = m_currentCodePointColumn;
    switch (getCurrentCodePoint()) {
    case '<':
        nextCodePoint();
        if (isAtEOF()) {
            appendToToken('<');
            m_tokenType = NON_SYMBOL;
        }
        else {
            savePosition();
            if (!parseQuotedIRI()) {
                clearTokenBuffer();
                appendToToken('<');
                restorePosition();
                if (getCurrentCodePoint() == '=') {
                    appendToToken('=');
                    nextCodePoint();
                }
                m_tokenType = NON_SYMBOL;
            }
        }
        break;
    case '\'':
    case '\"':
        if (parseQuotedString())
            m_tokenType = QUOTED_STRING;
        else
            m_tokenType = ERROR_TOKEN;
        break;
    case '!':
        appendToToken('!');
        nextCodePoint();
        readCodePoint('=');
        m_tokenType = NON_SYMBOL;
        break;
    case '>':
        appendToToken('>');
        nextCodePoint();
        readCodePoint('=');
        m_tokenType = NON_SYMBOL;
        break;
    case '^':
        appendToToken('^');
        nextCodePoint();
        readCodePoint('^');
        m_tokenType = NON_SYMBOL;
        break;
    case '|':
        appendToToken('|');
        nextCodePoint();
        if (readCodePoint('|'))
            m_tokenType = NON_SYMBOL;
        else
            m_tokenType = ERROR_TOKEN;
        break;
    case '&':
        appendToToken('&');
        nextCodePoint();
        if (readCodePoint('&'))
            m_tokenType = NON_SYMBOL;
        else
            m_tokenType = ERROR_TOKEN;
        break;
    case '.':
    case '0':
    case '1':
    case '2':
    case '3':
    case '4':
    case '5':
    case '6':
    case '7':
    case '8':
    case '9':
        appendToToken(getCurrentCodePoint());
        // A dot at the beginning of a token followed by a digit starts a numeric constant, so if a dot is not followed by a digit,
        // it is interpreted as a token on its own.
        if (getCurrentCodePoint() == '.') {
            nextCodePoint();
            if (isAtEOF() || !::isNumber(getCurrentCodePoint())) {
                m_tokenType = NON_SYMBOL;
                return;
            }
            m_tokenType = NUMBER;
            while (!isAtEOF() && ::isNumber(getCurrentCodePoint())) {
                appendToToken(getCurrentCodePoint());
                nextCodePoint();
            }
        }
        else {
            nextCodePoint();
            m_tokenType = NUMBER;
            while (!isAtEOF() && ::isNumber(getCurrentCodePoint())) {
                appendToToken(getCurrentCodePoint());
                nextCodePoint();
            }
            if (!isAtEOF() && getCurrentCodePoint() == '.') {
                savePosition();
                nextCodePoint();
                if (!isAtEOF() && ::isNumber(getCurrentCodePoint())) {
                    appendToToken('.');
                    while (!isAtEOF() && ::isNumber(getCurrentCodePoint())) {
                        appendToToken(getCurrentCodePoint());
                        nextCodePoint();
                    }
                }
                else {
                    restorePosition();
                    return;
                }
            }
        }
        if (!isAtEOF() && (getCurrentCodePoint() == 'e' || getCurrentCodePoint() == 'E')) {
            savePosition();
            CodePoint e = getCurrentCodePoint();
            nextCodePoint();
            CodePoint sign = 0;
            if (getCurrentCodePoint() == '-' || getCurrentCodePoint() == '+') {
                sign = getCurrentCodePoint();
                nextCodePoint();
            }
            if (!isAtEOF() && ::isNumber(getCurrentCodePoint())) {
                appendToToken(e);
                if (sign != 0)
                    appendToToken(sign);
                while (!isAtEOF() && ::isNumber(getCurrentCodePoint())) {
                    appendToToken(getCurrentCodePoint());
                    nextCodePoint();
                }
            }
            else {
                restorePosition();
                return;
            }
        }
        break;
    case '=':
    case ';':
    case ',':
    case '-': // '-' cannot occur at the start of a symbol, so is is matched as a non-symbol
    case '+':
    case '*':
    case '/':
    case '(':
    case ')':
    case '[':
    case ']':
    case '{':
    case '}':
        appendToToken(getCurrentCodePoint());
        nextCodePoint();
        m_tokenType = NON_SYMBOL;
        break;
    case '@':
        appendToToken(getCurrentCodePoint());
        nextCodePoint();
        if (isAtEOF() || !::isAlpha(getCurrentCodePoint()))
            m_tokenType = ERROR_TOKEN;
        else {
            while (!isAtEOF() && ::isAlpha(getCurrentCodePoint())) {
                appendToToken(getCurrentCodePoint());
                nextCodePoint();
            }
            while (!isAtEOF() && getCurrentCodePoint() == '-') {
                savePosition();
                nextCodePoint();
                if (!isAtEOF() && ::isAlphaNum(getCurrentCodePoint()))
                    appendToToken('-');
                else {
                    restorePosition();
                    break;
                }
                while (!isAtEOF() && ::isAlphaNum(getCurrentCodePoint())) {
                    appendToToken(getCurrentCodePoint());
                    nextCodePoint();
                }
            }
            m_tokenType = LANGUAGE_TAG;
        }
        break;
    case '_':
        appendToToken('_');
        nextCodePoint();
        if (readCodePoint(':') && readCodePoint(TurtleSyntax::PN_CHARS_U_NUM)) {
            readTokenWithDots(TurtleSyntax::PN_CHARS);
            m_tokenType = BLANK_NODE;
        }
        else
            m_tokenType = ERROR_TOKEN;

        break;
    case '$':
    case '?':
        appendToToken(getCurrentCodePoint());
        nextCodePoint();
        if (readCodePoint(TurtleSyntax::PN_CHARS_U_NUM)) {
            while (readCodePoint(TurtleSyntax::PN_CHARS_VAR2)) {
            }
            m_tokenType = VARIABLE;
        }
        else
            m_tokenType = ERROR_TOKEN;
        break;
    default:
        m_tokenType = SYMBOL;
        // First try to match PN_PREFIX. If the match stops there, return a token of type SYMBOL.
        if (readCodePoint(TurtleSyntax::PN_CHARS_BASE))
            readTokenWithDots(TurtleSyntax::PN_CHARS);
        // We are past PN_PREFIX at this point
        if (readCodePoint(':')) {
            // Special case for ':-'. Note that local name cannot start with '-' (it can if escaped), so there is no ambiguity here.
            if (m_tokenBufferAfterLast == 1 && readCodePoint('-'))
                m_tokenType = NON_SYMBOL;
            else if (readCodePoint(TurtleSyntax::PN_CHARS_U_NUM_COLON) || read_PERCENT() || read_PN_LOCAL_ESC()) {
                // We know that there is at least one character in the token now.
                m_tokenType = PNAME_LN;
                while (!isAtEOF()) {
                    if (readCodePoint(TurtleSyntax::PN_CHARS_COLON) || read_PERCENT() || read_PN_LOCAL_ESC()) {
                    }
                    else if (getCurrentCodePoint() == '.') {
                        savePosition();
                        size_t numberOfDots = 0;
                        while (!isAtEOF() && getCurrentCodePoint() == '.') {
                            nextCodePoint();
                            ++numberOfDots;
                        }
                        if (!isAtEOF()) {
                            if (TurtleSyntax::is_PN_CHARS_COLON(getCurrentCodePoint())) {
                                const CodePoint codePoint = getCurrentCodePoint();
                                nextCodePoint();
                                for (size_t index = 0; index < numberOfDots; ++index)
                                    appendToToken('.');
                                appendToToken(codePoint);
                                continue;
                            }
                            else if (getCurrentCodePoint() == '%') {
                                nextCodePoint();
                                CodePoint codePoint1;
                                CodePoint codePoint2;
                                if (read_HEX(codePoint1) && read_HEX(codePoint2)) {
                                    for (size_t index = 0; index < numberOfDots; ++index)
                                        appendToToken('.');
                                    appendToToken('%');
                                    appendToToken(codePoint1);
                                    appendToToken(codePoint2);
                                    continue;
                                }
                            }
                            else if (getCurrentCodePoint() == '\\') {
                                nextCodePoint();
                                if (!isAtEOF() && TurtleSyntax::is_PN_LOCAL_ESC(getCurrentCodePoint())) {
                                    const CodePoint codePoint = getCurrentCodePoint();
                                    nextCodePoint();
                                    for (size_t index = 0; index < numberOfDots; ++index)
                                        appendToToken('.');
                                    appendToToken('\\');
                                    appendToToken(codePoint);
                                    continue;
                                }
                            }
                        }
                        restorePosition();
                        break;
                    }
                    else
	                    break;
                }

            }
            else
                m_tokenType = PNAME_NS;
        }
        else {
            if (m_tokenBufferAfterLast == 0)
                m_tokenType = ERROR_TOKEN;
            else
                m_tokenType = SYMBOL;
        }
        break;
    }
}

void TurtleTokenizer::doNextCodePoint() {
    nextCodePoint();
}

void TurtleTokenizer::recover() {
    while (m_tokenType == ERROR_TOKEN) {
        if (isAtEOF())
            m_tokenType = EOF_TOKEN;
        else if (TurtleSyntax::isWhitespace(getCurrentCodePoint()))
            m_tokenType = NO_TOKEN;
        else
            nextCodePoint();
    }
}
