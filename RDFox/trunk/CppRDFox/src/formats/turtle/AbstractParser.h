// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef ABSTRACTPARSER_H_
#define ABSTRACTPARSER_H_

#include "../../Common.h"
#include "../../logic/Logic.h"
#include "TurtleTokenizer.h"

class Prefixes;

template<class Derived>
class AbstractParser : private Unmovable {

public:

    typedef AbstractParser<Derived> AbstractParserType;

protected:

    struct StartErrorRecovery {
    };

    Prefixes& m_prefixes;
    TurtleTokenizer m_tokenizer;

    void nextToken();

    void reportError(const char* const errorName);

    void recoverFromErrorBySkippingAfterNext(const char c1, const char c2);

    void parsePrefixMapping();

    Variable parseVariable(const LogicFactory& factory);

    Term parseTerm(const LogicFactory& factory);

    bool parseIRI(std::string& iri);

    void parseResource(ResourceText& resourceText);

    Atom parseAtom(const LogicFactory& factory);

    Atom parseNormalAtom(const LogicFactory& factory);

    Atom parseRDFAtom(const LogicFactory& factory);

    Filter parseFilter(const LogicFactory& factory);

    Bind parseBind(const LogicFactory& factory);

    AtomicFormula parseAtomicFormula(const LogicFactory& factory);

    Negation parseNegation(const LogicFactory& factory);

    Aggregate parseAggregate(const LogicFactory& factory);

    BuiltinExpression parseConditionalOrExpression(const LogicFactory& factory);

    BuiltinExpression parseConditionalAndExpression(const LogicFactory& factory);

    BuiltinExpression parseRelationalExpression(const LogicFactory& factory);

    BuiltinExpression parseAdditiveExpression(const LogicFactory& factory);

    BuiltinExpression parseMultiplicaitveExpression(const LogicFactory& factory);

    BuiltinExpression parseUnaryExpression(const LogicFactory& factory);

    BuiltinExpression parsePrimaryExpression(const LogicFactory& factory);

    void parseExpressionList(const LogicFactory& factory, std::vector<BuiltinExpression>& arguments);

    AbstractParser(Prefixes& prefixes);

};

#endif // ABSTRACTPARSER_H_
