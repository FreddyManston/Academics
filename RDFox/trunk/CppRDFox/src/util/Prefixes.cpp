// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "../RDFStoreException.h"
#include "../formats/turtle/TurtleSyntax.h"
#include "CodePoint.h"
#include "Vocabulary.h"
#include "Prefixes.h"

const Prefixes Prefixes::s_defaultPrefixes(0);

const Prefixes Prefixes::s_emptyPrefixes;

always_inline static bool isHexDigit(const uint8_t digit) {
    return ('0' <= digit && digit <= '9') || ('A' <= digit && digit <= 'F') || ('a' <= digit && digit <= 'a');
}

const static char HEX[16] = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F' };

always_inline static bool appendNextCodePoint(const uint8_t* & current, const uint8_t* end, std::string& encodedIRI, const uint8_t* const codePointClass) {
    if (current < end) {
        uint8_t codePointSize;
        const CodePoint codePoint = ::peekNextCodePoint(current, end, codePointSize);
        if (TurtleSyntax::isInCodePointClass(codePointClass, codePoint)) {
            ::appendCodePoint(encodedIRI, codePoint);
            current += codePointSize;
            return true;
        }
    }
    return false;
}

always_inline static bool appendNext_PERCENT(const uint8_t* & current, const uint8_t* end, std::string& encodedIRI) {
    if (current + 2 < end && *current == '%' && ::isHexDigit(*(current + 1)) && ::isHexDigit(*(current + 2))) {
        encodedIRI.push_back('%');
        encodedIRI.push_back(*(current + 1));
        encodedIRI.push_back(*(current + 2));
        current += 3;
        return true;
    }
    else
        return false;
}

always_inline static bool appendNext_PN_LOCAL_ESC(const uint8_t* & current, const uint8_t* end, std::string& encodedIRI) {
    if (current < end && TurtleSyntax::is_PN_LOCAL_ESC(*current)) {
        encodedIRI.push_back('\\');
        encodedIRI.push_back(*current);
        ++current;
        return true;
    }
    else
        return false;
}

always_inline static bool encodeIRI(const std::string& prefixName, const std::string& prefixURI, const std::string& decodedIRI, std::string& encodedIRI) {
    encodedIRI.clear();
    encodedIRI.append(prefixName);
    const uint8_t* current = ::begin(decodedIRI) + prefixURI.length();
    const uint8_t* end = ::end(decodedIRI);
    if (!::appendNextCodePoint(current, end, encodedIRI, TurtleSyntax::PN_CHARS_U_NUM_COLON) && !::appendNext_PERCENT(current, end, encodedIRI) && !::appendNext_PN_LOCAL_ESC(current, end, encodedIRI))
        return false;
    bool lastIsDot = false;
    while (current < end) {
        if (::appendNextCodePoint(current, end, encodedIRI, TurtleSyntax::PN_CHARS_COLON) || ::appendNext_PERCENT(current, end, encodedIRI) || ::appendNext_PN_LOCAL_ESC(current, end, encodedIRI))
            lastIsDot = false;
        else if (*current == '.') {
            encodedIRI.push_back('.');
            ++current;
            lastIsDot = true;
        }
        else
            return false;
    }
    return !lastIsDot;
}

always_inline static bool containsPrefix(const std::string& string, const std::string& prefix) {
    const size_t prefixSize = prefix.size();
    if (prefixSize >= string.size())
        return false;
    std::string::const_iterator stringIterator = string.begin();
    std::string::const_iterator prefixIterator = prefix.begin();
    while (prefixIterator < prefix.end()) {
        if (*stringIterator != *prefixIterator)
            return false;
        ++stringIterator;
        ++prefixIterator;
    }
    return true;
}

always_inline void Prefixes::declarePrefixNoCheck(const std::string& prefixName, const std::string& prefixIRI) {
    m_prefixIRIsByPrefixNames[prefixName] = prefixIRI;
}

Prefixes::Prefixes(int) : m_prefixIRIsByPrefixNames() {
    declareStandardPrefixes();
}

Prefixes::Prefixes() : m_prefixIRIsByPrefixNames() {
}

Prefixes::Prefixes(const Prefixes& prefixes) : m_prefixIRIsByPrefixNames(prefixes.m_prefixIRIsByPrefixNames) {
}

Prefixes::DecodeResult Prefixes::decodeAbbreviatedIRI(std::string& iri) const {
    // We do not check whether appreviatedIRI matches PN_LOCAL. If it does, then the result of this function is
    // correct according to the Turtle specification. If it does not, then the result approximates the intent
    // as best as it can. In particular, all \ in the local name are dropped as this corresponds to unesaping
    // PN_LOCAL_ESC. This is done regardless of whether the exaped character matches PN_LOCAL_ESC.
    size_t abbreviatedIRILength = iri.length();
    for (size_t colonPosition = 0; colonPosition < abbreviatedIRILength; colonPosition++) {
        if (iri[colonPosition] == ':') {
            const std::string prefixName = iri.substr(0, colonPosition + 1);
            const std::map<std::string, std::string>::const_iterator iterator = m_prefixIRIsByPrefixNames.find(prefixName);
            if (iterator == m_prefixIRIsByPrefixNames.end())
                return DECODE_PREFIX_NAME_NOT_BOUND;
            else {
                iri.erase(0, colonPosition + 1);
                iri.insert(0, iterator->second);
                size_t current = iterator->second.length();
                while (current < iri.length()) {
                    if (iri[current] == '\\')
                        iri.erase(current, 1);
                    ++current;
                }
                return DECODE_SUCCESSFUL;
            }
        }
    }
    return DECODE_NO_PREFIX_NAME;
}

Prefixes::DecodeResult Prefixes::decodeAbbreviatedIRI(const std::string& abbreviatedIRI, std::string& iri) const {
    // We do not check whether appreviatedIRI matches PN_LOCAL. If it does, then the result of this function is
    // correct according to the Turtle specification. If it does not, then the result approximates the intent
    // as best as it can. In particular, all \ in the local name are dropped as this corresponds to unesaping
    // PN_LOCAL_ESC. This is done regardless of whether the exaped character matches PN_LOCAL_ESC.
    size_t abbreviatedIRILength = abbreviatedIRI.length();
    for (size_t colonPosition = 0; colonPosition < abbreviatedIRILength; colonPosition++) {
        if (abbreviatedIRI[colonPosition] == ':') {
            const std::string prefixName = abbreviatedIRI.substr(0, colonPosition + 1);
            const std::map<std::string, std::string>::const_iterator iterator = m_prefixIRIsByPrefixNames.find(prefixName);
            if (iterator == m_prefixIRIsByPrefixNames.end()) {
                iri = abbreviatedIRI;
                return DECODE_PREFIX_NAME_NOT_BOUND;
            }
            else {
                iri.clear();
                iri.append(iterator->second);
                iri.reserve(iri.length() + (abbreviatedIRI.length() - colonPosition - 1));
                std::string::const_iterator current = abbreviatedIRI.begin() + colonPosition + 1;
                const std::string::const_iterator end = abbreviatedIRI.end();
                while (current < end) {
                    if (*current == '\\') {
                        ++current;
                        if (current < end)
                            iri.push_back(*current);
                    }
                    else
                        iri.push_back(*current);
                    ++current;
                }
                return DECODE_SUCCESSFUL;
            }
        }
    }
    iri = abbreviatedIRI;
    return DECODE_NO_PREFIX_NAME;
}

void Prefixes::decodeIRI(const std::string& encodedIRI, std::string& decodedIRI) const {
    size_t encodedIRILength = encodedIRI.length();
    if (encodedIRILength >= 2 && encodedIRI[0] == '<' && encodedIRI[encodedIRILength - 1] == '>')
        decodedIRI = encodedIRI.substr(1, encodedIRILength - 2);
    else
        decodeAbbreviatedIRI(encodedIRI, decodedIRI);
}

std::string Prefixes::decodeIRI(const std::string& encodedIRI) const {
    std::string decodedIRI;
    decodeIRI(encodedIRI, decodedIRI);
    return decodedIRI;
}

bool Prefixes::encodeIRI(const std::string& decodedIRI, std::string& encodedIRI) const {
    for (std::map<std::string, std::string>::const_iterator iterator = m_prefixIRIsByPrefixNames.begin(); iterator != m_prefixIRIsByPrefixNames.end(); ++iterator)
        if (::containsPrefix(decodedIRI, iterator->second) && ::encodeIRI(iterator->first, iterator->second, decodedIRI, encodedIRI))
            return true;
    encodedIRI.clear();
    encodedIRI.append("<");
    const uint8_t* current = ::begin(decodedIRI);
    const uint8_t* end = ::end(decodedIRI);
    while (current < end) {
        // We do not parse the IRI as UTF8 here for speed. This is not needed because all characters that are not IRIREF
        // are ASCII; hence, we can copy all other bytes directly without needing to parse them into code points.
        const uint8_t leadByte = *current;
        if (!::isASCII(leadByte) || TurtleSyntax::is_IRIREF(leadByte))
            encodedIRI.push_back(leadByte);
        else {
            encodedIRI.push_back('\\');
            encodedIRI.push_back('u');
            encodedIRI.push_back('0');
            encodedIRI.push_back('0');
            encodedIRI.push_back(HEX[leadByte >> 4]);
            encodedIRI.push_back(HEX[leadByte & 0x0Fu]);
        }
        ++current;
    }
    encodedIRI.append(">");
    return false;
}

std::string::const_iterator Prefixes::getLocalName(const std::string& decodedIRI) const {
    for (std::map<std::string, std::string>::const_iterator iterator = m_prefixIRIsByPrefixNames.begin(); iterator != m_prefixIRIsByPrefixNames.end(); ++iterator)
        if (::containsPrefix(decodedIRI, iterator->second))
            return decodedIRI.begin() + iterator->second.length();
    return decodedIRI.begin();
}

std::string Prefixes::encodeIRI(const std::string& decodedIRI) const {
    std::string encodedIRI;
    encodeIRI(decodedIRI, encodedIRI);
    return encodedIRI;
}

void Prefixes::createAutomaticPrefix(const std::string& iri, const size_t maxPrefixNumber) {
    if (m_prefixIRIsByPrefixNames.size() >= maxPrefixNumber)
        return;
    size_t index = iri.find_last_of('#');
    if (index == std::string::npos)
        index = iri.find_last_of('/');
    if (index != std::string::npos) {
        std::string prefixIRI(iri, 0, index + 1);
        for (std::map<std::string, std::string>::iterator iterator = m_prefixIRIsByPrefixNames.begin(); iterator != m_prefixIRIsByPrefixNames.end(); ++iterator)
            if (iterator->second == prefixIRI)
                return;
        size_t count = 0;
        std::string prefixName;
        do {
            ++count;
            std::ostringstream buffer;
            buffer << "a" << count << ":";
            prefixName = buffer.str();
        } while (m_prefixIRIsByPrefixNames.find(prefixName) != m_prefixIRIsByPrefixNames.end());
        declarePrefix(prefixName, prefixIRI);
    }
}

bool Prefixes::prefixExists(const std::string& prefixName) const {
    return m_prefixIRIsByPrefixNames.find(prefixName) != m_prefixIRIsByPrefixNames.end();
}

bool Prefixes::getPrefixIRI(const std::string& prefixName, std::string& prefixIRI) const {
    std::map<std::string, std::string>::const_iterator iterator = m_prefixIRIsByPrefixNames.find(prefixName);
    if (iterator == m_prefixIRIsByPrefixNames.end())
        return false;
    else {
        prefixIRI = iterator->second;
        return true;
    }
}

bool Prefixes::declarePrefix(const std::string& prefixName, const std::string& prefixIRI) {
    if (isValidPrefixName(prefixName)) {
        declarePrefixNoCheck(prefixName, prefixIRI);
        return true;
    }
    else
        return false;
}

void Prefixes::declareStandardPrefixes() {
    declarePrefixNoCheck("rdf:", RDF_NS);
    declarePrefixNoCheck("rdfs:", RDFS_NS);
    declarePrefixNoCheck("owl:", OWL_NS);
    declarePrefixNoCheck("xsd:", XSD_NS);
    declarePrefixNoCheck("swrl:", SWRL_NS);
    declarePrefixNoCheck("swrlb:", SWRLB_NS);
    declarePrefixNoCheck("swrlx:", SWRLX_NS);
    declarePrefixNoCheck("ruleml:", RULEML_NS);
}

void Prefixes::clear() {
    m_prefixIRIsByPrefixNames.clear();
}

const std::map<std::string, std::string>& Prefixes::getPrefixIRIsByPrefixNames() const {
    return m_prefixIRIsByPrefixNames;
}

void Prefixes::loadFrom(const Prefixes& prefixes) {
    for (std::map<std::string, std::string>::const_iterator iterator = prefixes.m_prefixIRIsByPrefixNames.begin(); iterator != prefixes.m_prefixIRIsByPrefixNames.end(); ++iterator)
        declarePrefix(iterator->first, iterator->second);
}

bool Prefixes::isValidPrefixName(const std::string& prefixName) {
    const uint8_t* current = ::begin(prefixName);
    const uint8_t* const last = ::end(prefixName) - 1;
    if (last < current || *last != ':')
        return false;
    if (current < last  && !TurtleSyntax::is_PN_CHARS_BASE(::getNextCodePoint(current, last)))
        return false;
    bool lastIsDot = false;
    while (current < last) {
        const CodePoint codePoint = ::getNextCodePoint(current, last);
        if (codePoint == '.')
            lastIsDot = true;
        else if (TurtleSyntax::is_PN_CHARS(codePoint))
            lastIsDot = false;
        else
            return false;
    }
    return !lastIsDot;
}
