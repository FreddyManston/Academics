// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef TURTLEPARSER_H_
#define TURTLEPARSER_H_

#include "AbstractParser.h"

class InputConsumer;

class TurtleParser : protected AbstractParser<TurtleParser> {

    friend class AbstractParser<TurtleParser>;

protected:

    size_t m_nextInternalBlankNodeID;
    ResourceText m_rdfFirst;
    ResourceText m_rdfRest;
    ResourceText m_rdfNil;
    InputConsumer* m_inputConsumer;

    void doReportError(const size_t line, const size_t column, const char* const errorDescription);

    void prefixMappingParsed(const std::string& prefixName, const std::string& prefixIRI);

    void consumeTriple(ResourceText& subject, ResourceText& predicate, ResourceText& object);

    void getNextInternalBlankNode(ResourceText& resourceText);

    void parseCollection(ResourceText& resourceText, bool& hasTurtle);

    bool parseNestedObject(ResourceText& resourceText, bool& hasTurtle);

    bool parseComplexResource(ResourceText& resourceText, bool& hasTurtle);
    
    void parsePredicateObject(ResourceText& currentSubject, ResourceText& currentPredicate, ResourceText& currentObject, bool& hasTurtle);

    void parseTripleWithSubject(ResourceText& currentSubject, ResourceText& currentPredicate, ResourceText& currentObject, bool& hasTurtle);

    void parseTriple(ResourceText& currentSubject, ResourceText& currentPredicate, ResourceText& currentObject, bool& hasTurtle);

    void parseTriplesBlock(ResourceText& currentSubject, ResourceText& currentPredicate, ResourceText& currentObject, bool& hasTurtle);

    void parseBase();

public:

    TurtleParser(Prefixes& prefixes);

    void parse(InputSource& inputSource, InputConsumer& inputConsumer, bool& hasTurtle, bool& hasTriG, bool& hasQuads);

};

#endif // TURTLEPARSER_H_
