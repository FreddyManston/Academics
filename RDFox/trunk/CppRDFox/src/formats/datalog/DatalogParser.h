// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef DATALOGPARSER_H_
#define DATALOGPARSER_H_

#include "../../Common.h"
#include "../../logic/Logic.h"
#include "../turtle/AbstractParser.h"

class InputSource;
class InputConsumer;

class DatalogParser : protected AbstractParser<DatalogParser> {

    friend class AbstractParser<DatalogParser>;

protected:

    InputConsumer* m_inputConsumer;

    void doReportError(const size_t line, const size_t column, const char* const errorDescription);

    void prefixMappingParsed(const std::string& prefixName, const std::string& prefixIRI);

    Formula parseExistenceExpressionArgument(const LogicFactory& factory);

    Literal parseLiteral(const LogicFactory& factory);

    void parseAtomOrRule(const size_t line, const size_t column, const LogicFactory& factory);

    void parseBase();

public:

    DatalogParser(Prefixes& prefixes);

    void bind(InputSource& inputSource);

    void unbind();

    void parse(const LogicFactory& factory, InputConsumer& inputConsumer);

    bool isEOF() const;

    Atom parseAtom(const LogicFactory& factory);

    Rule parseRule(const LogicFactory& factory);

};

#endif /* DATALOGPARSER_H_ */
