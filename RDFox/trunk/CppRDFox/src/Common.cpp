// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "RDFStoreException.h"
#include "Common.h"
#include "dictionary/Dictionary.h"
#include "util/Prefixes.h"
#include "util/Vocabulary.h"

// ResourceText

std::string ResourceText::toString(const Prefixes& prefixes) const {
    switch (m_resourceType) {
    case UNDEFINED_RESOURCE:
        return "UNDEF";
    case IRI_REFERENCE:
        return prefixes.encodeIRI(m_lexicalForm);
    case BLANK_NODE:
        return "_:" + m_lexicalForm;
    case LITERAL:
        if (m_datatypeIRI == XSD_INTEGER)
            return m_lexicalForm;
        if (m_datatypeIRI == XSD_STRING)
            return '\"' + m_lexicalForm + '\"';
        if (m_datatypeIRI == RDF_PLAIN_LITERAL) {
            const size_t atPosition = m_lexicalForm.find_last_of('@');
            if (atPosition != std::string::npos && atPosition != m_lexicalForm.length() - 1)
                return '\"' + m_lexicalForm.substr(0, atPosition) + '\"' + m_lexicalForm.substr(atPosition);
        }
        return '\"' + m_lexicalForm + "\"^^" + prefixes.encodeIRI(m_datatypeIRI);
    default:
        UNREACHABLE;
    }
}

std::ostream& operator<<(std::ostream& output, const ResourceText& resourceText) {
    output << resourceText.toString(Prefixes::s_defaultPrefixes);
    return output;
}

// ResourceValue

std::string ResourceValue::toString(const Prefixes& prefixes) const {
    std::string literalText;
    Dictionary::toTurtleLiteral(*this, prefixes, literalText);
    return literalText;
}

std::ostream& operator<<(std::ostream& output, const ResourceValue& resourceValue) {
    output << resourceValue.toString(Prefixes::s_defaultPrefixes);
    return output;
}

// Number formatting

size_t getNumberOfDigits(uint64_t value) {
    size_t numberOfDigits = 0;
    while (value != 0) {
        ++numberOfDigits;
        value /= 10;
    }
    if (numberOfDigits == 0)
        numberOfDigits = 1;
    return numberOfDigits;
}

size_t getNumberOfDigitsFormated(uint64_t value) {
    size_t numberOfDigits = 0;
    while (value >= 1000) {
        numberOfDigits += 4;
        value /= 1000;
    }
    if (value <= 9)
        numberOfDigits += 1;
    else if (value <= 99)
        numberOfDigits += 2;
    else
        numberOfDigits += 3;
    return numberOfDigits;
}

void printNumberFormatted(std::ostream& output, uint64_t value, const size_t width) {
    size_t usedSpaces = static_cast<size_t>(-4);
    uint64_t mask;
    uint64_t newMask = 1;
    do {
        mask = newMask;
        usedSpaces += 4;
        newMask *= 1000;
    } while (newMask <= value);
    const size_t firstGroup = value / mask;
    if (firstGroup <= 9)
        usedSpaces++;
    else if (firstGroup <= 99)
        usedSpaces += 2;
    else
        usedSpaces += 3;
    for (size_t index = usedSpaces; index < width; ++index)
        output << ' ';
    output << value / mask;
    value %= mask;
    mask /= 1000;
    while (mask > 0) {
        uint64_t group = value / mask;
        output << ',';
        if (group < 10)
            output << "00";
        else if (group < 100)
            output << '0';
        output << group;
        value %= mask;
        mask /= 1000;
    }
}

void printNumberAbbreviated(std::ostream& output, uint64_t value) {
    static const char* const s_suffixes[] = { " ", "k", "M", "G", "T", "P", "E", "Z", "Y" };
    uint16_t suffixIndex = 0;
    uint16_t thousands = 0;
    while (value >= 1000) {
        ++suffixIndex;
        thousands = value % 1000;
        value /= 1000;
    }
    if (value <= 9)
        output << "  ";
    else if (value <= 99)
        output << ' ';
    output << value;
    if (suffixIndex != 0)
        output << '.' << thousands/100 << s_suffixes[suffixIndex];
    else
        output << "   ";
}
