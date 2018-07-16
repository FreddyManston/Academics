// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "TurtleSyntax.h"

always_inline static void setCodePoint(uint8_t* const typeArray, const CodePoint codePoint) {
    typeArray[codePoint >> 3] |= (static_cast<uint8_t>(0x1) << (codePoint & 0x7));
}

always_inline static void setCodePoints(uint8_t* const typeArray, const CodePoint codePointStart, const CodePoint codePointEnd) {
    for (CodePoint codePoint = codePointStart; codePoint <= codePointEnd; ++codePoint)
        setCodePoint(typeArray, codePoint);
}

always_inline static void copy(const uint8_t* const sourceArray, uint8_t* const targetArray) {
    for (size_t index = 0; index < 65536 / sizeof(uint8_t); ++index)
        targetArray[index] = sourceArray[index];
}

bool TurtleSyntax::IRIREF[256];
bool TurtleSyntax::PN_LOCAL_ESC[256];
uint8_t TurtleSyntax::PN_CHARS_BASE[65536 / sizeof(uint8_t)];
uint8_t TurtleSyntax::PN_CHARS_U_NUM[65536 / sizeof(uint8_t)];
uint8_t TurtleSyntax::PN_CHARS_U_NUM_COLON[65536 / sizeof(uint8_t)];
uint8_t TurtleSyntax::PN_CHARS[65536 / sizeof(uint8_t)];
uint8_t TurtleSyntax::PN_CHARS_COLON[65536 / sizeof(uint8_t)];
uint8_t TurtleSyntax::PN_CHARS_VAR2[65536 / sizeof(uint8_t)];
TurtleSyntax TurtleSyntax::s_turtleSyntax;

always_inline void set(bool* array, const char c, const bool value) {
    array[static_cast<uint16_t>(c)] = value;
}

TurtleSyntax::TurtleSyntax()  {
    // IRIREF
    for (size_t index = 33; index < sizeof(IRIREF); ++index)
        IRIREF[index] = true;
    for (size_t index = 0; index <= 32; ++index)
        IRIREF[index] = false;
    set(IRIREF, '<', false);
    set(IRIREF, '>', false);
    set(IRIREF, '"', false);
    set(IRIREF, '{', false);
    set(IRIREF, '}', false);
    set(IRIREF, '|', false);
    set(IRIREF, '^', false);
    set(IRIREF, '`', false);
    set(IRIREF, '\\', false);

    // PN_LOCAL_ESC
    for (size_t index = 0; index < sizeof(PN_LOCAL_ESC); ++index)
        PN_LOCAL_ESC[index] = false;
    set(PN_LOCAL_ESC, '_', true);
    set(PN_LOCAL_ESC, '~', true);
    set(PN_LOCAL_ESC, '.', true);
    set(PN_LOCAL_ESC, '-', true);
    set(PN_LOCAL_ESC, '!', true);
    set(PN_LOCAL_ESC, '$', true);
    set(PN_LOCAL_ESC, '&', true);
    set(PN_LOCAL_ESC, '\'', true);
    set(PN_LOCAL_ESC, '(', true);
    set(PN_LOCAL_ESC, ')', true);
    set(PN_LOCAL_ESC, '*', true);
    set(PN_LOCAL_ESC, '+', true);
    set(PN_LOCAL_ESC, ',', true);
    set(PN_LOCAL_ESC, ';', true);
    set(PN_LOCAL_ESC, '=', true);
    set(PN_LOCAL_ESC, '/', true);
    set(PN_LOCAL_ESC, '?', true);
    set(PN_LOCAL_ESC, '#', true);
    set(PN_LOCAL_ESC, '@', true);
    set(PN_LOCAL_ESC, '%', true);

    // PN_CHARS_BASE ::= [A-Z] | [a-z] | [#x00C0-#x00D6] | [#x00D8-#x00F6] | [#x00F8-#x02FF] | [#x0370-#x037D] | [#x037F-#x1FFF] | [#x200C-#x200D] | [#x2070-#x218F] | [#x2C00-#x2FEF] | [#x3001-#xD7FF] | [#xF900-#xFDCF] | [#xFDF0-#xFFFD] | [#x10000-#xEFFFF]
    for (size_t index = 0; index < sizeof(PN_CHARS_BASE); ++index)
        PN_CHARS_BASE[index] = 0;
    setCodePoints(PN_CHARS_BASE, 'A', 'Z');
    setCodePoints(PN_CHARS_BASE, 'a', 'z');
    setCodePoints(PN_CHARS_BASE, 0xC0u, 0xD6u);
    setCodePoints(PN_CHARS_BASE, 0xD8u, 0xF6u);
    setCodePoints(PN_CHARS_BASE, 0xF8u, 0x02FFu);
    setCodePoints(PN_CHARS_BASE, 0x0370u, 0x037Du);
    setCodePoints(PN_CHARS_BASE, 0x037Fu, 0x1FFFu);
    setCodePoints(PN_CHARS_BASE, 0x200Cu, 0x200Du);
    setCodePoints(PN_CHARS_BASE, 0x2070u, 0x218Fu);
    setCodePoints(PN_CHARS_BASE, 0x2C00u, 0x2FEFu);
    setCodePoints(PN_CHARS_BASE, 0x3001u, 0xD7FFu);
    setCodePoints(PN_CHARS_BASE, 0xF900u, 0xFDCFu);
    setCodePoints(PN_CHARS_BASE, 0xFDF0u, 0xFFFDu);

    // PN_CHARS_U_NUM ::= PN_CHARS_BASE | '_' | [0-9]
    copy(PN_CHARS_BASE, PN_CHARS_U_NUM);
    setCodePoint(PN_CHARS_U_NUM, '_');
    setCodePoints(PN_CHARS_U_NUM, '0', '9');

    // PN_CHARS_U_NUM_COLON ::= PN_CHARS_U_NUM | ':'
    copy(PN_CHARS_U_NUM, PN_CHARS_U_NUM_COLON);
    setCodePoint(PN_CHARS_U_NUM_COLON, ':');

    // PN_CHARS ::== PN_CHARS_U_NUM | '-' | #x00B7 | [#x0300-#x036F] | [#x203F-#x2040]
    copy(PN_CHARS_U_NUM, PN_CHARS);
    setCodePoint(PN_CHARS, '-');
    setCodePoint(PN_CHARS, 0xB7u);
    setCodePoints(PN_CHARS, 0x0300u, 0x036Fu);
    setCodePoints(PN_CHARS, 0x203Fu, 0x2040u);

    // PN_CHARS_COLON ::== PN_CHARS | ':'
    copy(PN_CHARS, PN_CHARS_COLON);
    setCodePoint(PN_CHARS_COLON, ':');

    // PN_CHARS_VAR2 ::= PN_CHARS_U_NUM | #x00B7 | [#x0300-#x036F] | [#x203F-#x2040]
    copy(PN_CHARS_U_NUM, PN_CHARS_VAR2);
    setCodePoint(PN_CHARS_VAR2, 0xB7u);
    setCodePoints(PN_CHARS_VAR2, 0x0300u, 0x036Fu);
    setCodePoints(PN_CHARS_VAR2, 0x203Fu, 0x2040u);
}
