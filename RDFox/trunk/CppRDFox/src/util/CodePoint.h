// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef CODEPOINT_H_
#define CODEPOINT_H_

#include "../all.h"

typedef uint32_t CodePoint;

const CodePoint INVALID_CODE_POINT = static_cast<CodePoint>(-1);

const uint8_t MAX_BYTES_PER_CODE_POINT = 4;

always_inline bool isCodePointStart(const uint8_t byte) {
    return ((byte & 0x80u) == 0) || ((byte & 0xE0u) == 0xC0u) || ((byte & 0xF0u) == 0xE0u) || ((byte & 0xF8u) == 0xF0u);
}

always_inline uint8_t appendCodePoint(std::string& string, const CodePoint codePoint) {
    if (codePoint <= 0x007Fu) {
        string.push_back(static_cast<char>(codePoint));
        return 1;
    }
    else if (codePoint <= 0x07FFu) {
        string.push_back(static_cast<char>(0xC0u) | static_cast<uint8_t>(codePoint >> 6));
        string.push_back(static_cast<char>(0x80u) | static_cast<uint8_t>(codePoint & 0x3Fu));
        return 2;
    }
    else if (codePoint <= 0xFFFFu) {
        string.push_back(static_cast<char>(0xE0u) | static_cast<uint8_t>(codePoint >> 12));
        string.push_back(static_cast<char>(0x80u) | static_cast<uint8_t>((codePoint >> 6) & 0x3Fu));
        string.push_back(static_cast<char>(0x80u) | static_cast<uint8_t>(codePoint & 0x3Fu));
        return 3;
    }
    else if (codePoint <= 0x10FFFFu) {
        string.push_back(static_cast<char>(0xF0u) | static_cast<uint8_t>(codePoint >> 18));
        string.push_back(static_cast<char>(0x80u) | static_cast<uint8_t>((codePoint >> 12) & 0x3Fu));
        string.push_back(static_cast<char>(0x80u) | static_cast<uint8_t>((codePoint >> 6) & 0x3Fu));
        string.push_back(static_cast<char>(0x80u) | static_cast<uint8_t>(codePoint & 0x3Fu));
        return 4;
    }
    else
        return 0;
}

always_inline uint8_t writeCodePoint(uint8_t* const buffer, const CodePoint codePoint) {
    if (codePoint <= 0x007Fu) {
        *buffer = static_cast<uint8_t>(codePoint);
        return 1;
    }
    else if (codePoint <= 0x07FFu) {
        *buffer = static_cast<uint8_t>(0xC0u) | static_cast<uint8_t>(codePoint >> 6);
        *(buffer + 1) = static_cast<uint8_t>(0x80u) | static_cast<uint8_t>(codePoint & 0x3Fu);
        return 2;
    }
    else if (codePoint <= 0xFFFFu) {
        *buffer = static_cast<uint8_t>(0xE0u) | static_cast<uint8_t>(codePoint >> 12);
        *(buffer + 1) = static_cast<uint8_t>(0x80u) | static_cast<uint8_t>((codePoint >> 6) & 0x3Fu);
        *(buffer + 2) = static_cast<uint8_t>(0x80u) | static_cast<uint8_t>(codePoint & 0x3Fu);
        return 3;
    }
    else if (codePoint <= 0x10FFFFu) {
        *buffer = static_cast<uint8_t>(0xF0u) | static_cast<uint8_t>(codePoint >> 18);
        *(buffer + 1) = static_cast<uint8_t>(0x80u) | static_cast<uint8_t>((codePoint >> 12) & 0x3Fu);
        *(buffer + 2) = static_cast<uint8_t>(0x80u) | static_cast<uint8_t>((codePoint >> 6) & 0x3Fu);
        *(buffer + 3) = static_cast<uint8_t>(0x80u) | static_cast<uint8_t>(codePoint & 0x3Fu);
        return 4;
    }
    else
        return 0;
}

always_inline bool combinePiece(CodePoint& codePoint, const uint8_t* current, const uint8_t shift) {
    const uint8_t pieceByte = *current;
    if ((pieceByte & 0xC0u) == 0x80u) {
        codePoint |= (static_cast<CodePoint>(pieceByte & 0x3Fu) << shift);
        return true;
    }
    else
        return false;
}

always_inline CodePoint peekNextCodePoint(const uint8_t* const current, const uint8_t* afterLast, uint8_t& codePointSize) {
    const uint8_t leadByte = *current;
    if ((leadByte & 0x80u) == 0) {
        codePointSize = 1;
        return leadByte;
    }
    else if ((leadByte & 0xE0u) == 0xC0u) {
        CodePoint codePoint = (static_cast<CodePoint>(leadByte & 0x1Fu) << 6);
        if (current + 1 < afterLast && combinePiece(codePoint, current + 1, 0)) {
            codePointSize = 2;
            return codePoint;
        }
    }
    else if ((leadByte & 0xF0u) == 0xE0u) {
        CodePoint codePoint = (static_cast<CodePoint>(leadByte & 0x0Fu) << 12);
        if (current + 2 < afterLast && combinePiece(codePoint, current + 1, 6) && combinePiece(codePoint, current + 2, 0)) {
            codePointSize = 3;
            return codePoint;
        }
    }
    else if ((leadByte & 0xF8u) == 0xF0u) {
        CodePoint codePoint = (static_cast<CodePoint>(leadByte & 0x07u) << 18);
        if (current + 3 < afterLast && combinePiece(codePoint, current + 1, 12) && combinePiece(codePoint, current + 2, 6)  && combinePiece(codePoint, current + 3, 6)) {
            codePointSize = 4;
            return codePoint;
        }
    }
    codePointSize = 0;
    return INVALID_CODE_POINT;
}

always_inline CodePoint getNextCodePoint(const uint8_t* & current, const uint8_t* afterLast) {
    uint8_t codePointSize;
    const CodePoint codePoint = peekNextCodePoint(current, afterLast, codePointSize);
    current += codePointSize;
    return codePoint;
}

always_inline bool isASCII(const uint8_t leadByte) {
    return (leadByte & 0x80u) == 0;
}

always_inline const uint8_t* begin(const std::string& string) {
    return reinterpret_cast<const uint8_t*>(string.data());
}

always_inline const uint8_t* end(const std::string& string) {
    return reinterpret_cast<const uint8_t*>(string.data() + string.length());
}

#endif /* CODEPOINT_H_ */
