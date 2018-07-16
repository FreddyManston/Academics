// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef ABSTRACTPARSERIMPL_H_
#define ABSTRACTPARSERIMPL_H_

#include "../../util/Vocabulary.h"
#include "../../util/Prefixes.h"
#include "AbstractParser.h"

template<class Derived>
void AbstractParser<Derived>::nextToken() {
    m_tokenizer.nextToken();
    if (m_tokenizer.isErrorToken())
        reportError("Invalid token.");
}

template<class Derived>
void AbstractParser<Derived>::reportError(const char* const errorDescription) {
    static_cast<Derived*>(this)->doReportError(m_tokenizer.getTokenStartLine(), m_tokenizer.getTokenStartColumn(), errorDescription);
    throw StartErrorRecovery();
}

template<class Derived>
void AbstractParser<Derived>::recoverFromErrorBySkippingAfterNext(const char c1, const char c2) {
    do {
        m_tokenizer.recover();
        // Skip over the current triple to the next c1 or c2
        while (m_tokenizer.isGood() && (!m_tokenizer.isNonSymbol() || (!m_tokenizer.tokenEqualsNoType(c1) && !m_tokenizer.tokenEqualsNoType(c2))))
            m_tokenizer.nextToken();
        // Skip over c1 or c2
        m_tokenizer.nextToken();
    } while (m_tokenizer.isErrorToken());
}

template<class Derived>
void AbstractParser<Derived>::parsePrefixMapping() {
    if (!m_tokenizer.is_PNAME_NS())
        reportError("Prefix name expected.");
    std::string prefixName;
    m_tokenizer.getToken(prefixName);
    nextToken();
    if (!m_tokenizer.isQuotedIRI())
        reportError("Prefix IRI of the form <IRI> expected.");
    std::string prefixIRI;
    m_tokenizer.getToken(prefixIRI);
    nextToken();
    static_cast<Derived*>(this)->prefixMappingParsed(prefixName, prefixIRI);
    if (!m_prefixes.declarePrefix(prefixName, prefixIRI)) {
        std::ostringstream message;
        message << "Could not declare prefix '" << prefixName << "' as IRI <" << prefixIRI << "'.";
        reportError(message.str().c_str());
    }
}

template<class Derived>
Variable AbstractParser<Derived>::parseVariable(const LogicFactory& factory) {
    if (!m_tokenizer.isVariable())
        reportError("Variable expected.");
    std::string variableName = m_tokenizer.getToken(1);
    nextToken();
    return factory->getVariable(variableName);
}

template<class Derived>
Term AbstractParser<Derived>::parseTerm(const LogicFactory& factory) {
    if (m_tokenizer.isVariable())
        return parseVariable(factory);
    else {
        ResourceText resourceText;
        parseResource(resourceText);
        switch (resourceText.m_resourceType) {
        case IRI_REFERENCE:
            return factory->getIRIReference(resourceText.m_lexicalForm);
        case BLANK_NODE:
            return factory->getBlankNode(resourceText.m_lexicalForm);
        case LITERAL:
            return factory->getLiteral(resourceText.m_lexicalForm, resourceText.m_datatypeIRI);
        default:
            UNREACHABLE;
        }
    }
}

template<class Derived>
bool AbstractParser<Derived>::parseIRI(std::string& iri) {
    if (m_tokenizer.isQuotedIRI()) {
        m_tokenizer.getToken(iri);
        nextToken();
        return true;
    }
    else if (m_tokenizer.is_PNAME_LN()) {
        m_tokenizer.getToken(iri);
        nextToken();
        Prefixes::DecodeResult result = m_prefixes.decodeAbbreviatedIRI(iri);
        if (result == Prefixes::DECODE_NO_PREFIX_NAME) {
            std::ostringstream message;
            message << "Token '" << iri << "' does not contain a prefix name.";
            reportError(message.str().c_str());
        }
        else if (result == Prefixes::DECODE_PREFIX_NAME_NOT_BOUND) {
            std::ostringstream message;
            message << "The prefix name in the local IRI '" << iri << "' has not been bound.";
            reportError(message.str().c_str());
        }
        return true;
    }
    else
        return false;
}

template<class Derived>
void AbstractParser<Derived>::parseResource(ResourceText& resourceText) {
    if (parseIRI(resourceText.m_lexicalForm))
        resourceText.m_resourceType = IRI_REFERENCE;
    else if (m_tokenizer.symbolTokenEquals("a")) {
        resourceText.m_lexicalForm = RDF_TYPE;
        resourceText.m_resourceType = IRI_REFERENCE;
        nextToken();
    }
    else if (m_tokenizer.symbolLowerCaseTokenEquals("true") || m_tokenizer.symbolLowerCaseTokenEquals("false")) {
        m_tokenizer.getToken(resourceText.m_lexicalForm);
        resourceText.m_datatypeIRI = XSD_BOOLEAN;
        resourceText.m_resourceType = LITERAL;
        nextToken();
    }
    else if (m_tokenizer.nonSymbolTokenEquals('-') || m_tokenizer.nonSymbolTokenEquals('+')) {
        const uint8_t sign = m_tokenizer.getTokenByte(0);
        nextToken();
        if (m_tokenizer.isNumber()) {
            m_tokenizer.getToken(resourceText.m_lexicalForm);
            resourceText.m_lexicalForm.insert(resourceText.m_lexicalForm.begin(), static_cast<char>(sign));
            resourceText.m_resourceType = LITERAL;
            if (resourceText.m_lexicalForm.find_first_of('e') != std::string::npos || resourceText.m_lexicalForm.find_first_of('E') != std::string::npos)
                resourceText.m_datatypeIRI = XSD_DOUBLE;
            else if (resourceText.m_lexicalForm.find_first_of('.') == std::string::npos)
                resourceText.m_datatypeIRI = XSD_INTEGER;
            else
                resourceText.m_datatypeIRI = XSD_DOUBLE; // This should be xsd:decimal, but we don't support that yet, so for the moment we parse it as a double.
            nextToken();
        }
        else
            reportError("Number expected after - or +.");
    }
    else if (m_tokenizer.isNumber()) {
        m_tokenizer.getToken(resourceText.m_lexicalForm);
        resourceText.m_resourceType = LITERAL;
        if (resourceText.m_lexicalForm.find_first_of('e') != std::string::npos || resourceText.m_lexicalForm.find_first_of('E') != std::string::npos)
            resourceText.m_datatypeIRI = XSD_DOUBLE;
        else if (resourceText.m_lexicalForm.find_first_of('.') == std::string::npos)
            resourceText.m_datatypeIRI = XSD_INTEGER;
        else
            resourceText.m_datatypeIRI = XSD_DOUBLE; // This should be xsd:decimal, but we don't support that yet, so for the moment we parse it as a double.
        nextToken();
    }
    else if (m_tokenizer.isQuotedString()) {
        m_tokenizer.getToken(resourceText.m_lexicalForm);
        nextToken();
        if (m_tokenizer.isLanguageTag()) {
            m_tokenizer.appendToken(resourceText.m_lexicalForm);
            nextToken();
            resourceText.m_datatypeIRI = RDF_PLAIN_LITERAL;
        }
        else if (m_tokenizer.nonSymbolTokenEquals("^^")) {
            nextToken();
            if (!parseIRI(resourceText.m_datatypeIRI)) {
                reportError("Datatype IRI of a literal is missing.");
                resourceText.m_datatypeIRI = XSD_STRING;
            }
        }
        else
            resourceText.m_datatypeIRI = XSD_STRING;
        resourceText.m_resourceType = LITERAL;
    }
    else if (m_tokenizer.isBlankNode()) {
        m_tokenizer.getToken(resourceText.m_lexicalForm, 2);
        resourceText.m_resourceType = BLANK_NODE;
        nextToken();
    }
    else
        reportError("Resource expected.");
}

template<class Derived>
Atom AbstractParser<Derived>::parseAtom(const LogicFactory& factory) {
    if (m_tokenizer.nonSymbolTokenEquals('['))
        return parseRDFAtom(factory);
    else
        return parseNormalAtom(factory);
}

template<class Derived>
Atom AbstractParser<Derived>::parseNormalAtom(const LogicFactory& factory) {
    std::string predicateIRI;
    if (!parseIRI(predicateIRI))
        reportError("The predicate of an atom is missing.");
    if (!m_tokenizer.nonSymbolTokenEquals('('))
        reportError("The predicate of an atom must be followed by '('.");
    nextToken();
    Term firstTerm = parseTerm(factory);
    Term secondTerm;
    Term thirdTerm;
    if (m_tokenizer.nonSymbolTokenEquals(')')) {
        secondTerm = factory->getIRIReference("http://www.w3.org/1999/02/22-rdf-syntax-ns#type");
        thirdTerm = factory->getIRIReference(predicateIRI);
    }
    else if (m_tokenizer.nonSymbolTokenEquals(',')) {
        nextToken();
        secondTerm = factory->getIRIReference(predicateIRI);
        thirdTerm = parseTerm(factory);
        if (!m_tokenizer.nonSymbolTokenEquals(')'))
            reportError("Expected ')': only unary and binary atoms are supported.");
    }
    else
        reportError("Unexpected symbol while reading the list of terms of an atom: expected ',' or ')'.");
    nextToken();
    return factory->getRDFAtom(firstTerm, secondTerm, thirdTerm);
}

template<class Derived>
Atom AbstractParser<Derived>::parseRDFAtom(const LogicFactory& factory) {
    nextToken();
    Term subjectTerm = parseTerm(factory);
    if (!m_tokenizer.nonSymbolTokenEquals(','))
        reportError("Terms in an RDF atom should be separated by ','.");
    nextToken();
    Term predicateTerm = parseTerm(factory);
    if (!m_tokenizer.nonSymbolTokenEquals(','))
        reportError("Terms in an RDF atom should be separated by ','.");
    nextToken();
    Term objectTerm = parseTerm(factory);
    if (!m_tokenizer.nonSymbolTokenEquals(']'))
        reportError("An RDF atom should be terminated with ']'.");
    nextToken();
    return factory->getRDFAtom(subjectTerm, predicateTerm, objectTerm);
}

template<class Derived>
Filter AbstractParser<Derived>::parseFilter(const LogicFactory& factory) {
    nextToken();
    BuiltinExpression builtinExpression = parseConditionalOrExpression(factory);
    return factory->getFilter(builtinExpression);
}

template<class Derived>
Bind AbstractParser<Derived>::parseBind(const LogicFactory& factory) {
    nextToken();
    if (!m_tokenizer.nonSymbolTokenEquals('('))
        reportError("Expected '(' after 'BIND'.");
    nextToken();
    BuiltinExpression builtinExpression = parseConditionalOrExpression(factory);
    if (!m_tokenizer.symbolLowerCaseTokenEquals("as"))
        reportError("Expected 'AS' after a bind expression.");
    nextToken();
    Term boundTerm = parseTerm(factory);
    if (!m_tokenizer.nonSymbolTokenEquals(')'))
        reportError("Expected ')' at the end of 'BIND'.");
    nextToken();
    return factory->getBind(builtinExpression, boundTerm);
}

template<class Derived>
AtomicFormula AbstractParser<Derived>::parseAtomicFormula(const LogicFactory& factory) {
    if (m_tokenizer.nonSymbolTokenEquals('['))
        return parseRDFAtom(factory);
    else if (m_tokenizer.symbolLowerCaseTokenEquals("filter"))
        return parseFilter(factory);
    else if (m_tokenizer.symbolLowerCaseTokenEquals("bind"))
        return parseBind(factory);
    else
        return parseNormalAtom(factory);
}

template<class Derived>
Negation AbstractParser<Derived>::parseNegation(const LogicFactory& factory) {
    nextToken();
    std::vector<Variable> existentialVariables;
    std::vector<AtomicFormula> atomicFormulas;
    if (m_tokenizer.symbolLowerCaseTokenEquals("exist") || m_tokenizer.symbolLowerCaseTokenEquals("exists")) {
        nextToken();
        existentialVariables.push_back(parseVariable(factory));
        while (m_tokenizer.nonSymbolTokenEquals(',')) {
            nextToken();
            existentialVariables.push_back(parseVariable(factory));
        }
        if (!m_tokenizer.symbolLowerCaseTokenEquals("in"))
            reportError("Expected 'IN' after the existential variables in a 'NOT' atom.");
        nextToken();
    }
    if (m_tokenizer.nonSymbolTokenEquals('(')) {
        nextToken();
        atomicFormulas.push_back(parseAtomicFormula(factory));
        while (m_tokenizer.nonSymbolTokenEquals(',')) {
            nextToken();
            atomicFormulas.push_back(parseAtomicFormula(factory));
        }
        if (!m_tokenizer.nonSymbolTokenEquals(')'))
            reportError("Expected ')' after the conjunction of a 'NOT' atom.");
        nextToken();
    }
    else
        atomicFormulas.push_back(parseAtomicFormula(factory));
    return factory->getNegation(existentialVariables, atomicFormulas);
}

template<class Derived>
Aggregate AbstractParser<Derived>::parseAggregate(const LogicFactory& factory) {
    nextToken();
    if (!m_tokenizer.nonSymbolTokenEquals('('))
        reportError("Expected '(' after 'AGGREGATE'.");
    nextToken();
    std::vector<AtomicFormula> atomicFormulas;
    atomicFormulas.push_back(parseAtomicFormula(factory));
    while (m_tokenizer.nonSymbolTokenEquals(',') || m_tokenizer.nonSymbolTokenEquals('&')) {
        nextToken();
        atomicFormulas.push_back(parseAtomicFormula(factory));
    }
    if (!m_tokenizer.symbolLowerCaseTokenEquals("on"))
        reportError("Expected 'ON' after aggregate atom(s).");
    nextToken();
    bool skipEmptyGroups = false;
    if (m_tokenizer.symbolLowerCaseTokenEquals("not")) {
        nextToken();
        if (m_tokenizer.symbolLowerCaseTokenEquals("empty")) {
            nextToken();
            skipEmptyGroups = true;
        }
        else
            reportError("Expected 'EMPTY' after 'ON NOT'.");
    }
    std::vector<Variable> groupingVariables;
    while (m_tokenizer.isGood() && !m_tokenizer.symbolLowerCaseTokenEquals("bind") && !m_tokenizer.nonSymbolTokenEquals(')'))
        groupingVariables.push_back(parseVariable(factory));
    std::vector<AggregateBind> aggregateBinds;
    while (m_tokenizer.symbolLowerCaseTokenEquals("bind")) {
        nextToken();
        std::string functionName;
        if (m_tokenizer.isSymbol()) {
            functionName = m_tokenizer.getToken();
            nextToken();
        }
        else {
            ResourceText resourceText;
            parseResource(resourceText);
            if (resourceText.m_resourceType == IRI_REFERENCE)
                functionName = resourceText.m_lexicalForm;
            else
                reportError("An aggregate function name was expected.");
        }
        if (!m_tokenizer.nonSymbolTokenEquals('('))
            reportError("Expected '(' after the aggregate function name.");
        nextToken();
        bool distinct = false;
        if (m_tokenizer.symbolLowerCaseTokenEquals("distinct")) {
            distinct = true;
            nextToken();
        }
        std::vector<BuiltinExpression> arguments;
        if (!m_tokenizer.nonSymbolTokenEquals(')')) {
            arguments.push_back(parseConditionalOrExpression(factory));
            while (m_tokenizer.nonSymbolTokenEquals(',')) {
                nextToken();
                arguments.push_back(parseConditionalOrExpression(factory));
            }
        }
        if (!m_tokenizer.nonSymbolTokenEquals(')'))
            reportError("Expected ')' as termination of the aggregate function arguments.");
        nextToken();
        if (!m_tokenizer.symbolLowerCaseTokenEquals("as"))
            reportError("Expected 'AS' after the aggregated expression.");
        nextToken();
        Term result = parseTerm(factory);
        aggregateBinds.push_back(factory->getAggregateBind(functionName, distinct, arguments, result));
    }
    if (!m_tokenizer.nonSymbolTokenEquals(')'))
        reportError("Expected ')' as termination of 'AGGREGATE'.");
    nextToken();
    return factory->getAggregate(atomicFormulas, skipEmptyGroups, groupingVariables, aggregateBinds);
}

template<class Derived>
BuiltinExpression AbstractParser<Derived>::parseConditionalOrExpression(const LogicFactory& factory) {
    BuiltinExpression builtinExpression = parseConditionalAndExpression(factory);
    while (m_tokenizer.nonSymbolTokenEquals("||")) {
        nextToken();
        std::vector<BuiltinExpression> arguments;
        arguments.push_back(builtinExpression);
        arguments.push_back(parseConditionalAndExpression(factory));
        builtinExpression = factory->getBuiltinFunctionCall("internal$logical-or", arguments);
    }
    return builtinExpression;
}

template<class Derived>
BuiltinExpression AbstractParser<Derived>::parseConditionalAndExpression(const LogicFactory& factory) {
    BuiltinExpression builtinExpression = parseRelationalExpression(factory);
    while (m_tokenizer.nonSymbolTokenEquals("&&")) {
        nextToken();
        std::vector<BuiltinExpression> arguments;
        arguments.push_back(builtinExpression);
        arguments.push_back(parseRelationalExpression(factory));
        builtinExpression = factory->getBuiltinFunctionCall("internal$logical-and", arguments);
    }
    return builtinExpression;
}

template<class Derived>
BuiltinExpression AbstractParser<Derived>::parseRelationalExpression(const LogicFactory& factory) {
    BuiltinExpression builtinExpression = parseAdditiveExpression(factory);
    std::vector<BuiltinExpression> arguments;
    const char* operatorIRI = 0;
    if (m_tokenizer.symbolLowerCaseTokenEquals("in") || m_tokenizer.symbolLowerCaseTokenEquals("not")) {
        if (m_tokenizer.symbolLowerCaseTokenEquals("not")) {
            nextToken();
            if (!m_tokenizer.symbolLowerCaseTokenEquals("in"))
                reportError("Expected 'IN' after 'NOT'.");
            else
                operatorIRI = "internal$not-in";
        }
        else
            operatorIRI = "internal$in";
        nextToken();
        arguments.push_back(builtinExpression);
        parseExpressionList(factory, arguments);
    }
    else {
        if (m_tokenizer.nonSymbolTokenEquals('='))
            operatorIRI = "internal$equal";
        else if (m_tokenizer.nonSymbolTokenEquals("!="))
            operatorIRI = "internal$not-equal";
        else if (m_tokenizer.nonSymbolTokenEquals('<'))
            operatorIRI = "internal$less-than";
        else if (m_tokenizer.nonSymbolTokenEquals("<="))
            operatorIRI = "internal$less-equal-than";
        else if (m_tokenizer.nonSymbolTokenEquals('>'))
            operatorIRI = "internal$greater-than";
        else if (m_tokenizer.nonSymbolTokenEquals(">="))
            operatorIRI = "internal$greater-equal-than";
        if (operatorIRI != 0) {
            nextToken();
            arguments.push_back(builtinExpression);
            arguments.push_back(parseAdditiveExpression(factory));
        }
    }
    if (operatorIRI != 0)
        builtinExpression = factory->getBuiltinFunctionCall(operatorIRI, arguments);
    return builtinExpression;

}

template<class Derived>
BuiltinExpression AbstractParser<Derived>::parseAdditiveExpression(const LogicFactory& factory) {
    BuiltinExpression builtinExpression = parseMultiplicaitveExpression(factory);
    while (m_tokenizer.isNonSymbol()) {
        const char* operatorIRI;
        if (m_tokenizer.nonSymbolTokenEquals('+'))
            operatorIRI = "internal$add";
        else if (m_tokenizer.nonSymbolTokenEquals('-'))
            operatorIRI = "internal$subtract";
        else
            break;
        nextToken();
        std::vector<BuiltinExpression> arguments;
        arguments.push_back(builtinExpression);
        arguments.push_back(parseMultiplicaitveExpression(factory));
        builtinExpression = factory->getBuiltinFunctionCall(operatorIRI, arguments);
    }
    return builtinExpression;
}

template<class Derived>
BuiltinExpression AbstractParser<Derived>::parseMultiplicaitveExpression(const LogicFactory& factory) {
    BuiltinExpression builtinExpression = parseUnaryExpression(factory);
    while (m_tokenizer.isNonSymbol()) {
        const char* operatorIRI;
        if (m_tokenizer.nonSymbolTokenEquals('*'))
            operatorIRI = "internal$multiply";
        else if (m_tokenizer.nonSymbolTokenEquals('/'))
            operatorIRI = "internal$divide";
        else
            break;
        nextToken();
        std::vector<BuiltinExpression> arguments;
        arguments.push_back(builtinExpression);
        arguments.push_back(parseUnaryExpression(factory));
        builtinExpression = factory->getBuiltinFunctionCall(operatorIRI, arguments);
    }
    return builtinExpression;
}

template<class Derived>
BuiltinExpression AbstractParser<Derived>::parseUnaryExpression(const LogicFactory& factory) {
    const char* operatorIRI = 0;
    if (m_tokenizer.nonSymbolTokenEquals('!'))
        operatorIRI = "internal$logical-not";
    else if (m_tokenizer.nonSymbolTokenEquals('+'))
        operatorIRI = "internal$numeric-unary-plus";
    else if (m_tokenizer.nonSymbolTokenEquals('-'))
        operatorIRI = "internal$numeric-unary-minus";
    if (operatorIRI != 0)
        nextToken();
    BuiltinExpression builtinExpression = parsePrimaryExpression(factory);
    if (operatorIRI != 0) {
        std::vector<BuiltinExpression> arguments;
        arguments.push_back(builtinExpression);
        builtinExpression = factory->getBuiltinFunctionCall(operatorIRI, arguments);
    }
    return builtinExpression;
}

template<class Derived>
BuiltinExpression AbstractParser<Derived>::parsePrimaryExpression(const LogicFactory& factory) {
    if (m_tokenizer.nonSymbolTokenEquals('(')) {
        nextToken();
        BuiltinExpression builtinExpression = parseConditionalOrExpression(factory);
        if (!m_tokenizer.nonSymbolTokenEquals(')'))
            reportError("Unbalanced parentheses in a built-in expression.");
        nextToken();
        return builtinExpression;
    }
    else if (m_tokenizer.isVariable())
        return parseVariable(factory);
    else if (m_tokenizer.symbolLowerCaseTokenEquals("not") || m_tokenizer.symbolLowerCaseTokenEquals("exists")) {
        bool positive;
        if (m_tokenizer.symbolLowerCaseTokenEquals("not")) {
            nextToken();
            positive = false;
        }
        else
            positive = true;
        if (!m_tokenizer.symbolLowerCaseTokenEquals("exists")) {
            reportError("'EXISTS' expected.");
            return factory->getIRIReference("expression-parse-error");
        }
        nextToken();
        Formula formula = static_cast<Derived*>(this)->parseExistenceExpressionArgument(factory);
        return factory->getExistenceExpression(positive, formula);
    }
    else if (m_tokenizer.isSymbol()) {
        if (m_tokenizer.tokenEqualsNoType('a')) {
            nextToken();
            return factory->getIRIReference(::RDF_TYPE);
        }
        else if (m_tokenizer.symbolLowerCaseTokenEquals("true")) {
            nextToken();
            return factory->getLiteral("true", ::XSD_BOOLEAN);
        }
        else if (m_tokenizer.symbolLowerCaseTokenEquals("false")) {
            nextToken();
            return factory->getLiteral("false", ::XSD_BOOLEAN);
        }
        else {
            const std::string functionName = m_tokenizer.getToken();
            nextToken();
            std::vector<BuiltinExpression> functionArguments;
            parseExpressionList(factory, functionArguments);
            return factory->getBuiltinFunctionCall(functionName, functionArguments);
        }
    }
    else {
        ResourceText resourceText;
        parseResource(resourceText);
        switch (resourceText.m_resourceType) {
        case UNDEFINED_RESOURCE:
            return factory->getResourceByID(INVALID_RESOURCE_ID);
        case IRI_REFERENCE:
            if (m_tokenizer.nonSymbolTokenEquals('(')) {
                std::vector<BuiltinExpression> functionArguments;
                parseExpressionList(factory, functionArguments);
                return factory->getBuiltinFunctionCall(resourceText.m_lexicalForm, functionArguments);
            }
            else
                return factory->getIRIReference(resourceText.m_lexicalForm);
        case BLANK_NODE:
            return factory->getBlankNode(resourceText.m_lexicalForm);
        case LITERAL:
            return factory->getLiteral(resourceText.m_lexicalForm, resourceText.m_datatypeIRI);
        default:
            UNREACHABLE;
        }
    }
}

template<class Derived>
void AbstractParser<Derived>::parseExpressionList(const LogicFactory& factory, std::vector<BuiltinExpression>& arguments) {
    if (!m_tokenizer.nonSymbolTokenEquals('('))
        reportError("Expected '('.");
    nextToken();
    if (!m_tokenizer.nonSymbolTokenEquals(')')) {
        arguments.push_back(parseConditionalOrExpression(factory));
        while (m_tokenizer.isGood() && !m_tokenizer.nonSymbolTokenEquals(')')) {
            if (!m_tokenizer.nonSymbolTokenEquals(','))
                reportError("Expressions in a list should be separated by ','.");
            nextToken();
            arguments.push_back(parseConditionalOrExpression(factory));
        }
        if (!m_tokenizer.nonSymbolTokenEquals(')'))
            reportError("Expression list should be terminated by ')'.");
    }
    nextToken();
}

template<class Derived>
AbstractParser<Derived>::AbstractParser(Prefixes& prefixes) : m_prefixes(prefixes), m_tokenizer() {
}

#endif // ABSTRACTPARSERIMPL_H_
