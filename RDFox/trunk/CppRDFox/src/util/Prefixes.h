// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef PREFIXES_H_
#define PREFIXES_H_

#include "../all.h"

class Prefixes {

protected:

    std::map<std::string, std::string> m_prefixIRIsByPrefixNames;

    Prefixes(int);

    void declarePrefixNoCheck(const std::string& prefixName, const std::string& prefixIRI);

public:

    enum DecodeResult { DECODE_SUCCESSFUL, DECODE_NO_PREFIX_NAME, DECODE_PREFIX_NAME_NOT_BOUND };

    static const Prefixes s_defaultPrefixes;

    static const Prefixes s_emptyPrefixes;

    Prefixes();

    Prefixes(const Prefixes& prefixes);

    DecodeResult decodeAbbreviatedIRI(std::string& iri) const;

    DecodeResult decodeAbbreviatedIRI(const std::string& abbreviatedIRI, std::string& iri) const;

    void decodeIRI(const std::string& encodedIRI, std::string& decodedIRI) const;

    std::string decodeIRI(const std::string& encodedIRI) const;

    bool encodeIRI(const std::string& decodedIRI, std::string& encodedIRI) const;

    std::string encodeIRI(const std::string& decodedIRI) const;

    std::string::const_iterator getLocalName(const std::string& decodedIRI) const;

    void createAutomaticPrefix(const std::string& iri, const size_t maxPrefixNumber);

    bool prefixExists(const std::string& prefixName) const;

    bool getPrefixIRI(const std::string& prefixName, std::string& prefixIRI) const;

    bool declarePrefix(const std::string& prefixName, const std::string& prefixIRI);

    void declareStandardPrefixes();

    void clear();

    const std::map<std::string, std::string>& getPrefixIRIsByPrefixNames() const;

    void loadFrom(const Prefixes& prefixes);

    static bool isValidPrefixName(const std::string& prefixName);

};

#endif // PREFIXES_H_
