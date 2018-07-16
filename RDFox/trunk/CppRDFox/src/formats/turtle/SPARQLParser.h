// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef SPARQLPARSER_H_
#define SPARQLPARSER_H_

#include "AbstractParser.h"

class SPARQLParser : protected AbstractParser<SPARQLParser> {

    friend class AbstractParser<SPARQLParser>;

protected:

    friend class ConjunctProcessor;
    friend class SPARQLParserManager;

    size_t m_nextAnonymousVariableID;

    void doReportError(const size_t line, const size_t column, const char* const errorDescription);

    void prefixMappingParsed(const std::string& prefixName, const std::string& prefixIRI);

    void parseTriples(const LogicFactory& factory, std::vector<Formula>& conjuncts);

    Formula parseExistenceExpressionArgument(const LogicFactory& factory);

    void processAdditionalArguments(const LogicFactory& factory, std::vector<Formula>& conjuncts, std::vector<Formula>& additionalArguments, FormulaType& additionalArgumentsType);

    Formula parseGroupGraphPattern(const LogicFactory& factory);

    Formula parseGroupOrUnionGraphPattern(const LogicFactory& factory);

    Negation parseNegation(const LogicFactory& factory);

    Values parseValues(const LogicFactory& factory);

    Query parseSelect(const LogicFactory& factory);

    Query parseAsk(const LogicFactory& factory);

    Query parseConstruct(const LogicFactory& factory, std::vector<Atom>& constructPattern);

public:

    SPARQLParser(Prefixes& prefixes);

    Query parse(const LogicFactory& factory, const char* const queryText, const size_t queryTextLength);

    Query parse(const LogicFactory& factory, const char* const queryText, const size_t queryTextLength, std::vector<Atom>& constructPattern, SPARQLQueryType& sparqlQueryType);

};

#endif // SPARQLPARSER_H_
