// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef TURTLESYNTAX_H_
#define TURTLESYNTAX_H_

#include "../../util/CodePoint.h"

class TurtleSyntax {

protected:

    static TurtleSyntax s_turtleSyntax;

    TurtleSyntax();

public:

    static bool IRIREF[256];
    static bool PN_LOCAL_ESC[256];
    static uint8_t PN_CHARS_BASE[65536 / sizeof(uint8_t)];
    static uint8_t PN_CHARS_U_NUM[65536 / sizeof(uint8_t)];
    static uint8_t PN_CHARS_U_NUM_COLON[65536 / sizeof(uint8_t)];
    static uint8_t PN_CHARS[65536 / sizeof(uint8_t)];
    static uint8_t PN_CHARS_COLON[65536 / sizeof(uint8_t)];
    static uint8_t PN_CHARS_VAR2[65536 / sizeof(uint8_t)];

    always_inline static bool is_IRIREF(const CodePoint codePoint) {
        return codePoint > 0xFFu || IRIREF[codePoint];
    }

    always_inline static bool is_PN_LOCAL_ESC(const CodePoint codePoint) {
        return codePoint <= 0xFFu && PN_LOCAL_ESC[codePoint];
    }

    always_inline static bool isInCodePointClass(const uint8_t* const codePointClass, const CodePoint codePoint) {
        if (codePoint <= 0xFFFFu)
            return (codePointClass[codePoint >> 3] & (0x1 << (codePoint & 0x7))) != 0;
        else
            return 0x10000u <= codePoint && codePoint <= 0xEFFFFu;
    }

    always_inline static bool is_PN_CHARS_BASE(const CodePoint codePoint) {
        return isInCodePointClass(PN_CHARS_BASE, codePoint);
    }

    always_inline static bool is_PN_CHARS_U_NUM(const CodePoint codePoint) {
        return isInCodePointClass(PN_CHARS_U_NUM, codePoint);
    }

    always_inline static bool is_PN_CHARS_U_NUM_COLON(const CodePoint codePoint) {
        return isInCodePointClass(PN_CHARS_U_NUM_COLON, codePoint);
    }

    always_inline static bool is_PN_CHARS(const CodePoint codePoint) {
        return isInCodePointClass(PN_CHARS, codePoint);
    }

    always_inline static bool is_PN_CHARS_COLON(const CodePoint codePoint) {
        return isInCodePointClass(PN_CHARS_COLON, codePoint);
    }

    always_inline static bool is_PN_CHARS_VAR2(const CodePoint codePoint) {
        return isInCodePointClass(PN_CHARS_VAR2, codePoint);
    }

    always_inline static bool isWhitespace(const CodePoint codePoint) {
        return codePoint == ' ' || codePoint == '\r' || codePoint == '\n' || codePoint == '\t';
    }

    static bool isPNAME_NS(const std::string& string);

};

#endif // TURTLESYNTAX_H_
