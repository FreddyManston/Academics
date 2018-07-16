// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "../InputConsumer.h"
#include "../../util/Vocabulary.h"
#include "AbstractParserImpl.h"
#include "TurtleParser.h"

always_inline void TurtleParser::doReportError(const size_t line, const size_t column, const char* const errorDescription) {
    m_inputConsumer->reportError(line, column, errorDescription);
}

always_inline void TurtleParser::prefixMappingParsed(const std::string& prefixName, const std::string& prefixIRI) {
    m_inputConsumer->consumePrefixMapping(prefixName, prefixIRI);
}

always_inline void TurtleParser::consumeTriple(ResourceText& subject, ResourceText& predicate, ResourceText& object) {
    m_inputConsumer->consumeTriple(m_tokenizer.getTokenStartLine(), m_tokenizer.getTokenStartColumn(), subject, predicate, object);
}

void TurtleParser::getNextInternalBlankNode(ResourceText& resourceText) {
    std::ostringstream buffer;
    buffer << "__bnode_" << m_nextInternalBlankNodeID++;
    resourceText.m_lexicalForm = buffer.str();
    resourceText.m_resourceType = BLANK_NODE;
    resourceText.m_datatypeIRI.clear();
}

void TurtleParser::parseCollection(ResourceText& resourceText, bool& hasTurtle) {
    hasTurtle = true;
    nextToken();
    ResourceText lastNode;
    ResourceText currentNode;
    ResourceText listElement;
    bool first = true;
    while (!m_tokenizer.nonSymbolTokenEquals(')')) {
        parseComplexResource(listElement, hasTurtle);
        getNextInternalBlankNode(currentNode);
        if (first) {
            resourceText = currentNode;
            first = false;
        }
        else
            consumeTriple(lastNode, m_rdfRest, currentNode);
        consumeTriple(currentNode, m_rdfFirst, listElement);
        lastNode = currentNode;
    }
    nextToken();
    if (first)
        resourceText = m_rdfNil;
    else
        consumeTriple(lastNode, m_rdfRest, m_rdfNil);
}

bool TurtleParser::parseNestedObject(ResourceText& resourceText, bool& hasTurtle) {
    hasTurtle = true;
    nextToken();
    getNextInternalBlankNode(resourceText);
    ResourceText currentPredicate;
    ResourceText currentObject;
    bool isBlankNodePropertyList = !m_tokenizer.nonSymbolTokenEquals(']');
    while (!m_tokenizer.nonSymbolTokenEquals(']')) {
        parsePredicateObject(resourceText, currentPredicate, currentObject, hasTurtle);
        if (m_tokenizer.nonSymbolTokenEquals(';'))
            nextToken();
        else if (!m_tokenizer.nonSymbolTokenEquals(']'))
            reportError("A nested object should be terminated using the ']' character.");
    }
    nextToken();
    return isBlankNodePropertyList;
}

bool TurtleParser::parseComplexResource(ResourceText& resourceText, bool& hasTurtle) {
    if (m_tokenizer.nonSymbolTokenEquals('(')) {
        parseCollection(resourceText, hasTurtle);
        return false;
    }
    else if (m_tokenizer.nonSymbolTokenEquals('['))
        return parseNestedObject(resourceText, hasTurtle);
    else {
        parseResource(resourceText);
        return false;
    }
}

void TurtleParser::parsePredicateObject(ResourceText& currentSubject, ResourceText& currentPredicate, ResourceText& currentObject, bool& hasTurtle) {
    parseComplexResource(currentPredicate, hasTurtle);
    while (true) {
        parseComplexResource(currentObject, hasTurtle);
        consumeTriple(currentSubject, currentPredicate, currentObject);
        if (m_tokenizer.nonSymbolTokenEquals(',')) {
            hasTurtle = true;
            nextToken();
        }
        else
            break;
    }
}

void TurtleParser::parseTripleWithSubject(ResourceText& currentSubject, ResourceText& currentPredicate, ResourceText& currentObject, bool& hasTurtle) {
    currentPredicate.clear();
    currentObject.clear();
    while (true) {
        parsePredicateObject(currentSubject, currentPredicate, currentObject, hasTurtle);
        if (m_tokenizer.nonSymbolTokenEquals(';')) {
            hasTurtle = true;
            nextToken();
            if (m_tokenizer.nonSymbolTokenEquals('.'))
                break;
        }
        else
            break;
    }

}

always_inline void TurtleParser::parseTriple(ResourceText& currentSubject, ResourceText& currentPredicate, ResourceText& currentObject, bool& hasTurtle) {
    currentSubject.clear();
    if (parseComplexResource(currentSubject, hasTurtle) && m_tokenizer.nonSymbolTokenEquals('.'))
        return;
    parseTripleWithSubject(currentSubject, currentPredicate, currentObject, hasTurtle);
}

void TurtleParser::parseTriplesBlock(ResourceText& currentSubject, ResourceText& currentPredicate, ResourceText& currentObject, bool& hasTurtle) {
    if (!m_tokenizer.nonSymbolTokenEquals('{'))
        reportError("Graph name should be followed by the '{' character.");
    nextToken();
    while (!m_tokenizer.nonSymbolTokenEquals('}')) {
        if (!m_tokenizer.isGood())
            reportError("A graph block should be closed by the '}' character.");
        try {
            parseTriple(currentSubject, currentPredicate, currentObject, hasTurtle);
            if (m_tokenizer.nonSymbolTokenEquals('.'))
                nextToken();
            else if (!m_tokenizer.nonSymbolTokenEquals('}'))
                reportError("A statement in a graph block should be terminated using the '.' character.");
        }
        catch (const StartErrorRecovery&) {
            recoverFromErrorBySkippingAfterNext('.', '}');
        }
    }
    nextToken();
}

always_inline void TurtleParser::parseBase() {
    if (!m_tokenizer.isQuotedIRI())
        reportError("Base IRI of the form <IRI> expected.");
    nextToken();
}

TurtleParser::TurtleParser(Prefixes& prefixes) :
    AbstractParser<TurtleParser>(prefixes),
    m_nextInternalBlankNodeID(1),
    m_rdfFirst(IRI_REFERENCE, RDF_FIRST, ""),
    m_rdfRest(IRI_REFERENCE, RDF_REST, ""),
    m_rdfNil(IRI_REFERENCE, RDF_NIL, ""),
    m_inputConsumer(0)
{
}

void TurtleParser::parse(InputSource& inputSource, InputConsumer& inputConsumer, bool& hasTurtle, bool& hasTriG, bool& hasQuads) {
    m_inputConsumer = &inputConsumer;
    m_nextInternalBlankNodeID = 1;
    m_inputConsumer->start();
    m_tokenizer.initialize(inputSource);
    nextToken();
    ResourceText currentSubject;
    ResourceText currentPredicate;
    ResourceText currentObject;
    ResourceText currentContext;
	hasTurtle = false;
    hasTriG = false;
    hasQuads = false;
    while (m_tokenizer.isGood()) {
        try {
            if (m_tokenizer.tokenEquals(TurtleTokenizer::LANGUAGE_TAG, "@prefix")) {
                nextToken();
                parsePrefixMapping();
                hasTurtle = true;
                if (!m_tokenizer.nonSymbolTokenEquals('.'))
                    reportError("The prefix definition introduced by '@prefix' should be terminated by '.'.");
                nextToken();
            }
            else if (m_tokenizer.tokenEquals(TurtleTokenizer::LANGUAGE_TAG, "@base")) {
                nextToken();
                parseBase();
                hasTurtle = true;
                if (!m_tokenizer.nonSymbolTokenEquals('.'))
                    reportError("The base declaration introduced by '@base' should be terminated by '.'.");
                nextToken();
            }
            else if (m_tokenizer.symbolLowerCaseTokenEquals("prefix")) {
                nextToken();
                parsePrefixMapping();
                hasTurtle = true;
            }
            else if (m_tokenizer.symbolLowerCaseTokenEquals("base")) {
                nextToken();
                parseBase();
                hasTurtle = true;
            }
            else if (m_tokenizer.symbolLowerCaseTokenEquals("graph")) {
                nextToken();
                parseResource(currentContext);
                parseTriplesBlock(currentSubject, currentPredicate, currentObject, hasTurtle);
                hasTriG = true;
            }
            else if (m_tokenizer.nonSymbolTokenEquals('{')) {
                parseTriplesBlock(currentSubject, currentPredicate, currentObject, hasTurtle);
                hasTriG = true;
            }
            else {
                if (parseComplexResource(currentSubject, hasTurtle) && m_tokenizer.nonSymbolTokenEquals('.')) {
                    m_tokenizer.nextToken();
                    hasTurtle = true;
                }
                else if (m_tokenizer.nonSymbolTokenEquals('{')) {
                    parseTriplesBlock(currentSubject, currentPredicate, currentObject, hasTurtle);
                    hasTriG = true;
                }
                else {
                    parseTripleWithSubject(currentSubject, currentPredicate, currentObject, hasTurtle);
                    if (m_tokenizer.nonSymbolTokenEquals('.'))
                        nextToken();
                    else {
                        parseResource(currentContext);
                        hasQuads = true;
                        if (!m_tokenizer.nonSymbolTokenEquals('.'))
                            reportError("A statement should be terminated using the '.' character.");
                        else
                            nextToken();
                    }
                }
            }
        }
        catch (const StartErrorRecovery&) {
            recoverFromErrorBySkippingAfterNext('.', 0);
        }
    }
    m_tokenizer.deinitialize();
    m_inputConsumer->finish();
    m_inputConsumer = 0;
}
