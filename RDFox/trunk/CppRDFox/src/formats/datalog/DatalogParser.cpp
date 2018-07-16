// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "../InputSource.h"
#include "../InputConsumer.h"
#include "../turtle/AbstractParserImpl.h"
#include "DatalogParser.h"

always_inline void DatalogParser::doReportError(const size_t line, const size_t column, const char* const errorDescription) {
    if (m_inputConsumer)
        m_inputConsumer->reportError(line, column, errorDescription);
    else {
        std::ostringstream message;
        message << "Line " << line << ", column " << column << ": " << errorDescription;
        throw RDF_STORE_EXCEPTION(message.str());
    }
}

always_inline void DatalogParser::prefixMappingParsed(const std::string& prefixName, const std::string& prefixIRI) {
    m_inputConsumer->consumePrefixMapping(prefixName, prefixIRI);
}

always_inline Formula DatalogParser::parseExistenceExpressionArgument(const LogicFactory& factory) {
    if (!m_tokenizer.nonSymbolTokenEquals('{'))
        reportError("'{' expected after [NOT] EXISTS.");
    nextToken();
    Formula result;
    std::vector<Formula> conjuncts;
    if (m_tokenizer.symbolLowerCaseTokenEquals("select")) {
        nextToken();
        std::vector<Term> answerTerms;
        while (m_tokenizer.isGood() && !m_tokenizer.symbolLowerCaseTokenEquals("where") && !m_tokenizer.nonSymbolTokenEquals('{'))
            answerTerms.push_back(parseVariable(factory));
        if (m_tokenizer.symbolLowerCaseTokenEquals("where"))
            m_tokenizer.nextToken();
        if (!m_tokenizer.nonSymbolTokenEquals('{'))
            reportError("'{' expected after SELECT in [NOT] EXISTS.");
        m_tokenizer.nextToken();
        conjuncts.push_back(parseAtom(factory));
        while (m_tokenizer.nonSymbolTokenEquals(',')) {
            m_tokenizer.nextToken();
            conjuncts.push_back(parseAtom(factory));
        }
        if (!m_tokenizer.nonSymbolTokenEquals('}'))
            reportError("'}' expected after SELECT in [NOT] EXISTS.");
        m_tokenizer.nextToken();
        Formula queryFormula;
        if (conjuncts.size() == 1)
            queryFormula = conjuncts[0];
        else
            queryFormula = factory->getConjunction(conjuncts);
        result = factory->getQuery(false, answerTerms, queryFormula);
    }
    else {
        conjuncts.push_back(parseAtom(factory));
        while (m_tokenizer.nonSymbolTokenEquals(',')) {
            m_tokenizer.nextToken();
            conjuncts.push_back(parseAtom(factory));
        }
        if (conjuncts.size() == 1)
            result = conjuncts[0];
        else
            result = factory->getConjunction(conjuncts);
    }
    if (!m_tokenizer.nonSymbolTokenEquals('}'))
        reportError("'}' expected at the end of [NOT] EXISTS.");
    nextToken();
    return result;
}

always_inline Literal DatalogParser::parseLiteral(const LogicFactory& factory) {
    if (m_tokenizer.symbolLowerCaseTokenEquals("not"))
        return parseNegation(factory);
    else if (m_tokenizer.symbolLowerCaseTokenEquals("aggregate"))
        return parseAggregate(factory);
    else
        return parseAtomicFormula(factory);
}

always_inline void DatalogParser::parseAtomOrRule(const size_t line, const size_t column, const LogicFactory& factory) {
    Atom atom = parseAtom(factory);
    if (m_tokenizer.nonSymbolTokenEquals('.')) {
        m_tokenizer.nextToken();
        if (atom->getPredicate()->getName() == "internal$rdf" && atom->getNumberOfArguments() == 3 && atom->getArgument(0)->getType() == RESOURCE_BY_NAME && atom->getArgument(1)->getType() == RESOURCE_BY_NAME && atom->getArgument(2)->getType() == RESOURCE_BY_NAME)
            m_inputConsumer->consumeTriple(line, column, to_reference_cast<ResourceByName>(atom->getArgument(0)).getResourceText(), to_reference_cast<ResourceByName>(atom->getArgument(1)).getResourceText(), to_reference_cast<ResourceByName>(atom->getArgument(2)).getResourceText());
        else
            reportError("At present, only triple atoms can be loaded from a datalog file.");
    }
    else {
        std::vector<Atom> head;
        head.push_back(atom);
        while (m_tokenizer.isGood() && !m_tokenizer.nonSymbolTokenEquals(":-")) {
            if (!m_tokenizer.nonSymbolTokenEquals(',') && !m_tokenizer.nonSymbolTokenEquals('&'))
                reportError("Atoms in a rule head should be separated by ',' or by '&'.");
            nextToken();
            head.push_back(parseAtom(factory));
        }
        if (!m_tokenizer.nonSymbolTokenEquals(":-"))
            reportError("Expected ':-' in the rule.");
        nextToken();
        std::vector<Literal> body;
        if (!m_tokenizer.nonSymbolTokenEquals('.')) {
            body.push_back(parseLiteral(factory));
            while (!m_tokenizer.nonSymbolTokenEquals('.')) {
                if (!m_tokenizer.nonSymbolTokenEquals(',') && !m_tokenizer.nonSymbolTokenEquals('&'))
                    reportError("Literals in a rule body should be separated by ',' or by '&'.");
                nextToken();
                body.push_back(parseLiteral(factory));
            }
        }
        nextToken();
        m_inputConsumer->consumeRule(line, column, factory->getRule(head, body));
    }
}

always_inline void DatalogParser::parseBase() {
    if (!m_tokenizer.isQuotedIRI())
        reportError("Base IRI of the form <IRI> expected.");
    nextToken();
}

DatalogParser::DatalogParser(Prefixes& prefixes) :
    AbstractParser<DatalogParser>(prefixes),
    m_inputConsumer(nullptr)
{
}

void DatalogParser::bind(InputSource& inputSource) {
    m_tokenizer.initialize(inputSource);
    nextToken();
}

void DatalogParser::unbind() {
    m_tokenizer.deinitialize();
}

void DatalogParser::parse(const LogicFactory& factory, InputConsumer& inputConsumer) {
    m_inputConsumer = &inputConsumer;
    m_inputConsumer->start();
    while (m_tokenizer.isGood()) {
        try {
            if (m_tokenizer.lowerCaseTokenEquals(TurtleTokenizer::LANGUAGE_TAG, "@prefix")) {
                nextToken();
                parsePrefixMapping();
                if (!m_tokenizer.nonSymbolTokenEquals('.'))
                    reportError("The prefix definition introduced by '@prefix' should be terminated by '.'.");
                nextToken();
            }
            else if (m_tokenizer.tokenEquals(TurtleTokenizer::LANGUAGE_TAG, "@base")) {
                nextToken();
                parseBase();
                if (!m_tokenizer.nonSymbolTokenEquals('.'))
                    reportError("The base declaration introduced by '@base' should be terminated by '.'.");
                nextToken();
            }
            else if (m_tokenizer.symbolLowerCaseTokenEquals("prefix")) {
                nextToken();
                parsePrefixMapping();
            }
            else if (m_tokenizer.symbolLowerCaseTokenEquals("base")) {
                nextToken();
                parseBase();
            }
            else
                parseAtomOrRule(m_tokenizer.getCurrentCodePointLine(), m_tokenizer.getCurrentCodePointColumn(), factory);
        }
        catch (const AbstractParser<DatalogParser>::StartErrorRecovery&) {
            recoverFromErrorBySkippingAfterNext('.', 0);
        }
    }
    m_inputConsumer->finish();
    m_inputConsumer = nullptr;
}

bool DatalogParser::isEOF() const {
    return m_tokenizer.isEOF();
}

Atom DatalogParser::parseAtom(const LogicFactory& factory) {
    return AbstractParser<DatalogParser>::parseAtom(factory);
}

Rule DatalogParser::parseRule(const LogicFactory& factory) {
    std::vector<Atom> head;
    head.push_back(parseAtom(factory));
    while (m_tokenizer.isGood() && !m_tokenizer.nonSymbolTokenEquals(":-")) {
        if (!m_tokenizer.nonSymbolTokenEquals(',') && !m_tokenizer.nonSymbolTokenEquals('&'))
            reportError("Atoms in a rule head should be separated by ',' or by '&'.");
        nextToken();
        head.push_back(parseAtom(factory));
    }
    if (!m_tokenizer.nonSymbolTokenEquals(":-"))
        reportError("Expected ':-' in the rule.");
    nextToken();
    std::vector<Literal> body;
    if (!m_tokenizer.nonSymbolTokenEquals('.')) {
        body.push_back(parseLiteral(factory));
        while (!m_tokenizer.nonSymbolTokenEquals('.')) {
            if (!m_tokenizer.nonSymbolTokenEquals(',') && !m_tokenizer.nonSymbolTokenEquals('&'))
                reportError("Literals in a rule body should be separated by ',' or by '&'.");
            nextToken();
            body.push_back(parseLiteral(factory));
        }
    }
    nextToken();
    return factory->getRule(head, body);
}
