// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "../../RDFStoreException.h"
#include "../sources/MemorySource.h"
#include "AbstractParserImpl.h"
#include "SPARQLParser.h"

always_inline void SPARQLParser::doReportError(const size_t line, const size_t column, const char* const errorDescription) {
    std::ostringstream message;
    message << "Line " << line << ", column " << column << ": " << errorDescription;
    throw RDF_STORE_EXCEPTION(message.str());
}

always_inline void SPARQLParser::prefixMappingParsed(const std::string& prefixName, const std::string& prefixIRI) {
}

always_inline void SPARQLParser::parseTriples(const LogicFactory& factory, std::vector<Formula>& conjuncts) {
    const Term subject = parseTerm(factory);
    while (true) {
        const Term predicate = parseTerm(factory);
        while (true) {
            const Term object = parseTerm(factory);
            conjuncts.push_back(factory->getRDFAtom(subject, predicate, object));
            if (m_tokenizer.nonSymbolTokenEquals(','))
                nextToken();
            else
                break;
        }
        if (m_tokenizer.nonSymbolTokenEquals(';')) {
            nextToken();
            if (m_tokenizer.nonSymbolTokenEquals('.'))
                break;
        }
        else
            break;
    }
}

always_inline Formula SPARQLParser::parseExistenceExpressionArgument(const LogicFactory& factory) {
    return parseGroupGraphPattern(factory);
}

void SPARQLParser::processAdditionalArguments(const LogicFactory& factory, std::vector<Formula>& conjuncts, std::vector<Formula>& additionalArguments, FormulaType& additionalArgumentsType) {
    if (additionalArguments.size() > 0) {
        Formula main;
        if (conjuncts.size() == 1)
            main = conjuncts[0];
        else
            main = factory->getConjunction(conjuncts);
        conjuncts.clear();
        if (additionalArgumentsType == OPTIONAL_FORMULA)
            conjuncts.push_back(factory->getOptional(main, additionalArguments));
        else if (additionalArgumentsType == MINUS_FORMULA)
            conjuncts.push_back(factory->getMinus(main, additionalArguments));
        else
            UNREACHABLE;
        additionalArguments.clear();
        additionalArgumentsType = ATOM_FORMULA;
    }
}

Formula SPARQLParser::parseGroupGraphPattern(const LogicFactory& factory) {
    if (!m_tokenizer.nonSymbolTokenEquals('{'))
        reportError("'{' expected.");
    nextToken();
    if (m_tokenizer.symbolLowerCaseTokenEquals("select")) {
        Query subSelect = parseSelect(factory);
        if (!m_tokenizer.nonSymbolTokenEquals('}'))
            reportError("'}' expected after a nested SELECT query.");
        nextToken();
        return subSelect;
    }
    std::vector<Formula> conjuncts;
    std::vector<Formula> additionalArguments;
    std::vector<Formula> filters;
    FormulaType additionalArgumentsType = ATOM_FORMULA;
    enum ParseState { AFTER_GENERAL, AFTER_DOT, AFTER_TRIPLE_PATTERN };
    ParseState parseState = AFTER_GENERAL;
    while (m_tokenizer.isGood() && !m_tokenizer.nonSymbolTokenEquals('}')) {
        if (m_tokenizer.nonSymbolTokenEquals('.')) {
            if (parseState == AFTER_DOT)
                reportError("Two consecutive '.' were encountered.");
            else {
                nextToken();
                parseState = AFTER_DOT;
            }
        }
        else if (m_tokenizer.symbolLowerCaseTokenEquals("filter")) {
            filters.push_back(parseFilter(factory));
            parseState = AFTER_GENERAL;
        }
        else if (m_tokenizer.symbolLowerCaseTokenEquals("optional")) {
            nextToken();
            Formula optionalArgument = parseGroupGraphPattern(factory);
            parseState = AFTER_GENERAL;
            if (additionalArgumentsType != ATOM_FORMULA && additionalArgumentsType != OPTIONAL_FORMULA)
                processAdditionalArguments(factory, conjuncts, additionalArguments, additionalArgumentsType);
            additionalArguments.push_back(optionalArgument);
            additionalArgumentsType = OPTIONAL_FORMULA;
        }
        else if (m_tokenizer.symbolLowerCaseTokenEquals("minus")) {
            nextToken();
            Formula optionalArgument = parseGroupGraphPattern(factory);
            parseState = AFTER_GENERAL;
            if (additionalArgumentsType != ATOM_FORMULA && additionalArgumentsType != MINUS_FORMULA)
                processAdditionalArguments(factory, conjuncts, additionalArguments, additionalArgumentsType);
            additionalArguments.push_back(optionalArgument);
            additionalArgumentsType = MINUS_FORMULA;
        }
        else if (m_tokenizer.symbolLowerCaseTokenEquals("not")) {
            conjuncts.push_back(parseNegation(factory));
            parseState = AFTER_GENERAL;
        }
        else {
            processAdditionalArguments(factory, conjuncts, additionalArguments, additionalArgumentsType);
            if (m_tokenizer.nonSymbolTokenEquals('{')) {
                conjuncts.push_back(parseGroupOrUnionGraphPattern(factory));
                parseState = AFTER_GENERAL;
            }
            else if (m_tokenizer.symbolLowerCaseTokenEquals("bind")) {
                conjuncts.push_back(parseBind(factory));
                parseState = AFTER_GENERAL;
            }
            else if (m_tokenizer.symbolLowerCaseTokenEquals("values")) {
                conjuncts.push_back(parseValues(factory));
                parseState = AFTER_GENERAL;
            }
            else {
                if (parseState == AFTER_TRIPLE_PATTERN)
                    reportError("A triple pattern must be terminated by '.' before another triple pattern can be started.");
                parseTriples(factory, conjuncts);
                parseState = AFTER_TRIPLE_PATTERN;
            }
        }
    }
    if (!m_tokenizer.nonSymbolTokenEquals('}'))
        reportError("'}' expected at the end of a basic graph pattern.");
    nextToken();
    processAdditionalArguments(factory, conjuncts, additionalArguments, additionalArgumentsType);
    conjuncts.insert(conjuncts.end(), filters.begin(), filters.end());
    if (conjuncts.size() == 1)
        return conjuncts[0];
    else
        return factory->getConjunction(conjuncts);
}

Formula SPARQLParser::parseGroupOrUnionGraphPattern(const LogicFactory& factory) {
    std::vector<Formula> disjuncts;
    disjuncts.push_back(parseGroupGraphPattern(factory));
    while (m_tokenizer.symbolLowerCaseTokenEquals("union")) {
        nextToken();
        disjuncts.push_back(parseGroupGraphPattern(factory));
    }
    if (disjuncts.size() == 1)
        return disjuncts[0];
    else
        return factory->getDisjunction(disjuncts);
}

Negation SPARQLParser::parseNegation(const LogicFactory& factory) {
    nextToken();
    const Term subject = parseTerm(factory);
    const Term predicate = parseTerm(factory);
    const Term object = parseTerm(factory);
    std::vector<Variable> existentialVariables;
    std::vector<AtomicFormula> atomicFormulas;
    atomicFormulas.push_back(factory->getRDFAtom(subject, predicate, object));
    return factory->getNegation(existentialVariables, atomicFormulas);
}

Values SPARQLParser::parseValues(const LogicFactory& factory) {
    if (!m_tokenizer.symbolLowerCaseTokenEquals("values"))
        reportError("'VALUES' clause expected");
    nextToken();
    ResourceText resourceText;
    std::vector<Variable> variables;
    std::vector<std::vector<GroundTerm> > data;
    if (m_tokenizer.nonSymbolTokenEquals('(')) {
        nextToken();
        while (!m_tokenizer.nonSymbolTokenEquals(')'))
            variables.push_back(parseVariable(factory));
        if (!m_tokenizer.nonSymbolTokenEquals(')'))
            reportError("')' expected at the end of the variable list of a 'VALUES' pattern.");
        nextToken();
        if (!m_tokenizer.nonSymbolTokenEquals('{'))
            reportError("'{' expected in the 'VALUES' pattern.");
        nextToken();
        while (m_tokenizer.isGood() && !m_tokenizer.nonSymbolTokenEquals('}')) {
            if (!m_tokenizer.nonSymbolTokenEquals('('))
                reportError("'(' must be used to start a 'VALUES' tuple.");
            nextToken();
            data.push_back(std::vector<GroundTerm>());
            while (m_tokenizer.isGood() && !m_tokenizer.nonSymbolTokenEquals(')')) {
                if (m_tokenizer.symbolLowerCaseTokenEquals("undef")) {
                    nextToken();
                    data.back().push_back(factory->getResourceByID(INVALID_RESOURCE_ID));
                }
                else {
                    parseResource(resourceText);
                    data.back().push_back(factory->getResourceByName(resourceText));
                }
            }
            if (!m_tokenizer.nonSymbolTokenEquals(')'))
                reportError("')' must be used to end a 'VALUES' tuple.");
            nextToken();
            if (variables.size() != data.back().size())
                reportError("The number of values in a tuple differs from the number of variables.");
        }
    }
    else {
        variables.push_back(parseVariable(factory));
        if (!m_tokenizer.nonSymbolTokenEquals('{'))
            reportError("'{' expected in the 'VALUES' pattern.");
        nextToken();
        while (m_tokenizer.isGood() && !m_tokenizer.nonSymbolTokenEquals('}')) {
            data.push_back(std::vector<GroundTerm>());
            if (m_tokenizer.symbolLowerCaseTokenEquals("undef")) {
                nextToken();
                data.back().push_back(factory->getResourceByID(INVALID_RESOURCE_ID));
            }
            else {
                parseResource(resourceText);
                data.back().push_back(factory->getResourceByName(resourceText));
            }
        }
    }
    if (!m_tokenizer.nonSymbolTokenEquals('}'))
        reportError("'}' expected at the end of a 'VALUES' pattern.");
    nextToken();
    return factory->getValues(variables, data);
}

Query SPARQLParser::parseSelect(const LogicFactory& factory) {
    nextToken();
    bool distinct = false;
    if (m_tokenizer.symbolLowerCaseTokenEquals("distinct")) {
        nextToken();
        distinct = true;
    }
    else if (m_tokenizer.symbolLowerCaseTokenEquals("reduced"))
        nextToken();
    std::vector<Term> answerTerms;
    std::vector<Formula> bindFormulas;
    bool allAnswerVariables = false;
    if (m_tokenizer.nonSymbolTokenEquals('*')) {
        allAnswerVariables = true;
        nextToken();
    }
    else {
        while (m_tokenizer.isGood() && !m_tokenizer.symbolLowerCaseTokenEquals("where") && !m_tokenizer.nonSymbolTokenEquals('{')) {
            if (m_tokenizer.nonSymbolTokenEquals('(')) {
                nextToken();
                BuiltinExpression builtinExpression = parseConditionalOrExpression(factory);
                if (!m_tokenizer.symbolLowerCaseTokenEquals("as"))
                    reportError("'AS' expected in the (<function> AS <var>) expression.");
                nextToken();
                Variable boundVariable = parseVariable(factory);
                if (!m_tokenizer.nonSymbolTokenEquals(')'))
                    reportError("')' expected in the (<function> AS <var>) expression.");
                nextToken();
                answerTerms.push_back(boundVariable);
                bindFormulas.push_back(factory->getBind(builtinExpression, boundVariable));
            }
            else {
                BuiltinExpression builtinExpression = parseConditionalOrExpression(factory);
                switch (builtinExpression->getType()) {
                case VARIABLE:
                case RESOURCE_BY_ID:
                case RESOURCE_BY_NAME:
                    answerTerms.push_back(static_pointer_cast<Term>(builtinExpression));
                    break;
                case BUILTIN_FUNCTION_CALL:
                case EXISTENCE_EXPRESSION:
                    {
                        std::ostringstream buffer;
                        buffer << "anonymous_var$" << (m_nextAnonymousVariableID++);
                        Variable boundVariable = factory->getVariable(buffer.str());
                        answerTerms.push_back(boundVariable);
                        bindFormulas.push_back(factory->getBind(builtinExpression, boundVariable));
                    }
                    break;
                }
            }
        }
    }
    if (m_tokenizer.symbolLowerCaseTokenEquals("where"))
        nextToken();
    Formula queryFormula = parseGroupGraphPattern(factory);
    if (!bindFormulas.empty()) {
        std::unordered_set<Variable> freeVariables = queryFormula->getFreeVariables();
        for (std::vector<Formula>::iterator iterator = bindFormulas.begin(); iterator != bindFormulas.end(); ++iterator) {
            Bind bind = static_pointer_cast<Bind>(*iterator);
            Variable bindVariable = static_pointer_cast<Variable>(bind->getBoundTerm());
            if (freeVariables.find(bindVariable) != freeVariables.end()) {
                std::ostringstream buffer;
                buffer << "Variable '" << bindVariable->toString(m_prefixes) << "' occurs in a (<function> AS <var>) expression, as well as in the group graph pattern of the query.";
                reportError(buffer.str().c_str());
            }
        }
        if (queryFormula->getType() == CONJUNCTION_FORMULA) {
            Conjunction conjunction = static_pointer_cast<Conjunction>(queryFormula);
            bindFormulas.insert(bindFormulas.begin(), conjunction->getConjuncts().begin(), conjunction->getConjuncts().end());
        }
        else
            bindFormulas.insert(bindFormulas.begin(), queryFormula);
        queryFormula = factory->getConjunction(bindFormulas);
    }
    if (allAnswerVariables) {
        const std::unordered_set<Variable> freeVariables = queryFormula->getFreeVariables();
        for (std::unordered_set<Variable>::const_iterator iterator = freeVariables.begin(); iterator != freeVariables.end(); ++iterator)
            answerTerms.push_back(*iterator);
    }
    return factory->getQuery(distinct, answerTerms, queryFormula);
}

Query SPARQLParser::parseAsk(const LogicFactory& factory) {
    nextToken();
    if (m_tokenizer.symbolLowerCaseTokenEquals("where"))
        nextToken();
    Formula queryFormula = parseGroupGraphPattern(factory);
    return factory->getQuery(true, std::vector<Term>(), queryFormula);
}

Query SPARQLParser::parseConstruct(const LogicFactory& factory, std::vector<Atom>& constructPattern) {
    nextToken();
    std::vector<Formula> constructFormulas;
    bool isConstructWhere = false;
    if (m_tokenizer.nonSymbolTokenEquals('{')) {
        nextToken();
        enum ParseState { AFTER_GENERAL, AFTER_DOT, AFTER_TRIPLE_PATTERN };
        ParseState parseState = AFTER_GENERAL;
        while (m_tokenizer.isGood() && !m_tokenizer.nonSymbolTokenEquals('}')) {
            if (m_tokenizer.nonSymbolTokenEquals('.')) {
                if (parseState == AFTER_DOT)
                    reportError("Two consecutive '.' were encountered.");
                else {
                    nextToken();
                    parseState = AFTER_DOT;
                }
            }
            else {
                if (parseState == AFTER_TRIPLE_PATTERN)
                    reportError("A triple pattern must be terminated by '.' before another triple pattern can be started.");
                parseTriples(factory, constructFormulas);
                parseState = AFTER_TRIPLE_PATTERN;
            }
        }
        if (!m_tokenizer.nonSymbolTokenEquals('}'))
            reportError("'}' expected at the end of a basic graph pattern.");
        nextToken();
        if (m_tokenizer.symbolLowerCaseTokenEquals("where"))
            nextToken();
    }
    else if (m_tokenizer.symbolLowerCaseTokenEquals("where")) {
        nextToken();
        isConstructWhere = true;
    }
    else
        reportError("Invalid text after 'CONSTRUCT'.");
    Formula queryFormula = parseGroupGraphPattern(factory);
    if (isConstructWhere) {
        if (queryFormula->getType() == ATOM_FORMULA)
            constructFormulas.push_back(static_pointer_cast<Atom>(queryFormula));
        else if (queryFormula->getType() == CONJUNCTION_FORMULA)
            constructFormulas = to_pointer_cast<Conjunction>(queryFormula)->getConjuncts();
        else
            reportError("Invalid query body in a 'CONSTRUCT WHERE' query.");
    }
    std::vector<Term> answerTerms;
    std::unordered_set<Term> seenVariables;
    for (auto conjunctIterator = constructFormulas.begin(); conjunctIterator != constructFormulas.end(); ++conjunctIterator) {
        const FormulaType formulaType = (*conjunctIterator)->getType();
        if (formulaType == ATOM_FORMULA) {
            const Atom atom = static_pointer_cast<Atom>(*conjunctIterator);
            constructPattern.push_back(atom);
            const std::vector<Term>& arguments = atom->getArguments();
            for (auto argumentIterator = arguments.begin(); argumentIterator != arguments.end(); ++argumentIterator)
                if ((*argumentIterator)->getType() == VARIABLE && seenVariables.insert(*argumentIterator).second)
                    answerTerms.push_back(static_pointer_cast<Variable>(*argumentIterator));
        }
        else if (formulaType != BIND_FORMULA && formulaType != FILTER_FORMULA)
            reportError("Invalid formula in the CONSTRUCT clause.");
    }
    return factory->getQuery(false, answerTerms, queryFormula);
}

SPARQLParser::SPARQLParser(Prefixes& prefixes) :
    AbstractParser<SPARQLParser>(prefixes),
    m_nextAnonymousVariableID(0)
{
}

Query SPARQLParser::parse(const LogicFactory& factory, const char* const queryText, const size_t queryTextLength) {
    std::vector<Atom> constructPattern;
    SPARQLQueryType sparqlQueryType;
    Query query = parse(factory, queryText, queryTextLength, constructPattern, sparqlQueryType);
    if (sparqlQueryType == SPARQL_CONSTRUCT_QUERY)
        reportError("The query is a 'CONSTRUCT' query.");
    return query;
}

Query SPARQLParser::parse(const LogicFactory& factory, const char* const queryText, const size_t queryTextLength, std::vector<Atom>& constructPattern, SPARQLQueryType& sparqlQueryType) {
    m_nextAnonymousVariableID = 0;
    MemorySource memorySource(queryText, queryTextLength);
    m_tokenizer.initialize(memorySource);
    nextToken();
    Query query;
    while (true) {
        if (m_tokenizer.symbolLowerCaseTokenEquals("prefix")) {
            nextToken();
            parsePrefixMapping();
        }
        else if (m_tokenizer.symbolLowerCaseTokenEquals("base"))
            reportError("The 'BASE' directive is not supported yet.");
        else
            break;
    }
    if (m_tokenizer.symbolLowerCaseTokenEquals("select")) {
        query = parseSelect(factory);
        sparqlQueryType = SPARQL_SELECT_QUERY;
    }
    else if (m_tokenizer.symbolLowerCaseTokenEquals("ask")) {
        query = parseAsk(factory);
        sparqlQueryType = SPARQL_ASK_QUERY;
    }
    else if (m_tokenizer.symbolLowerCaseTokenEquals("construct")) {
        query = parseConstruct(factory, constructPattern);
        sparqlQueryType = SPARQL_CONSTRUCT_QUERY;
    }
    else {
        std::ostringstream message;
        message << "Unknown token '" << m_tokenizer.getToken() << "'.";
        reportError(message.str().c_str());
    }
    if (!m_tokenizer.isEOF())
        reportError("Unexpected characters after the end of query.");
    return query;
}
