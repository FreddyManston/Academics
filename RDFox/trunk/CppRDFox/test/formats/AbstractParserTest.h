// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef ABSTRACTPARSERTEST_H_
#define ABSTRACTPARSERTEST_H_

#include "../../src/Common.h"
#include "../../src/formats/InputConsumer.h"

class AbstractParserTest : public InputConsumer {

protected:

    struct ParsedTriple {
        std::string m_subject;
        ResourceType m_subjectType;
        std::string m_predicate;
        ResourceType m_predicateType;
        std::string m_object;
        std::string m_objectDatatypeIRI;
        ResourceType m_objectType;

        ParsedTriple(const std::string& subject, const ResourceType subjectType, const std::string& predicate, const ResourceType predicateType, const std::string& object, const std::string& objectDatatypeIRI, const ResourceType objectType);

    };

    typedef std::vector<ParsedTriple> TripleVector;

    TripleVector m_tripleVector;
    TripleVector::iterator m_tripleVectorIterator;

    void add(const std::string& subject, const std::string& predicate, const std::string& object);

    void add(const std::string& subject, const std::string& predicate, const std::string& object, const std::string& objectDatatypeIRI);

    void startTest();

    void endTest();

public:

    static std::string MY;
    static std::string XSD;
    static std::string RDF;

    AbstractParserTest();

    virtual void reportError(const size_t line, const size_t column, const char* const errorDescription);

    virtual void consumeTriple(const size_t line, const size_t column, const ResourceText& subject, const ResourceText& predicate, const ResourceText& object);

};

#define RT(s, p, o) \
    add(AbstractParserTest::MY + s, AbstractParserTest::MY + p, AbstractParserTest::MY + o)

#define LT(s, p, o, oi) \
    add(AbstractParserTest::MY + s, AbstractParserTest::MY + p, o, oi)

#endif // ABSTRACTPARSERTEST_H_
