// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifdef WITH_TEST

#include <CppTest/Checks.h>

#include "../../src/RDFStoreException.h"
#include "AbstractParserTest.h"

// AbstractParserTest::ParsedTriple

AbstractParserTest::ParsedTriple::ParsedTriple(const std::string& subject, const ResourceType subjectType, const std::string& predicate, const ResourceType predicateType, const std::string& object, const std::string& objectDatatypeIRI, const ResourceType objectType) :
    m_subject(subject), m_subjectType(subjectType), m_predicate(predicate), m_predicateType(predicateType), m_object(object), m_objectDatatypeIRI(objectDatatypeIRI), m_objectType(objectType)
{
}

// AbstractParserTest

std::string parseResourceOrBlankNode(const std::string& value, ResourceType& entityType) {
    if (value.length() > 2 && value[0] == '_' && value[1] == ':') {
        entityType = BLANK_NODE;
        return value.substr(2, value.length() - 2);
    }
    else {
        entityType = IRI_REFERENCE;
        return value;
    }
}

static void addResourceOrBlankNode(std::ostringstream& message, const std::string entity, const ResourceType entityType) {
    if (entityType == BLANK_NODE)
        message << "_:";
    message << entity;
}

static void outputTriple(std::ostringstream& message, const std::string& subject, const ResourceType subjectType, const std::string& predicate, const ResourceType predicateType, const std::string& object, const std::string& objectDatatypeIRI, const ResourceType objectType) {
    message << '[';
    addResourceOrBlankNode(message, subject, subjectType);
    message << " ";
    addResourceOrBlankNode(message, predicate, predicateType);
    message << " ";
    if (objectType == LITERAL)
        message << '"' << object << "\"^^<" << objectDatatypeIRI << '>';
    else
        addResourceOrBlankNode(message, object, objectType);
    message << ']';
}

void AbstractParserTest::add(const std::string& subject, const std::string& predicate, const std::string& object) {
    ResourceType subjectType;
    std::string subjectValue = parseResourceOrBlankNode(subject, subjectType);
    ResourceType predicateType;
    std::string predicateValue = parseResourceOrBlankNode(predicate, predicateType);
    ResourceType objectType;
    std::string objectValue = parseResourceOrBlankNode(object, objectType);
    m_tripleVector.push_back(ParsedTriple(subjectValue, subjectType, predicateValue, predicateType, objectValue, std::string(), objectType));
}

void AbstractParserTest::add(const std::string& subject, const std::string& predicate, const std::string& object, const std::string& objectDatatypeIRI) {
    ResourceType subjectType;
    std::string subjectValue = parseResourceOrBlankNode(subject, subjectType);
    ResourceType predicateType;
    std::string predicateValue = parseResourceOrBlankNode(predicate, predicateType);
    m_tripleVector.push_back(ParsedTriple(subjectValue, subjectType, predicateValue, predicateType, object, objectDatatypeIRI, LITERAL));
}

void AbstractParserTest::startTest() {
    m_tripleVectorIterator = m_tripleVector.begin();
}

void AbstractParserTest::endTest() {
    ASSERT_TRUE(m_tripleVectorIterator == m_tripleVector.end());
}

std::string AbstractParserTest::MY("http://my.com/local#");
std::string AbstractParserTest::XSD("http://www.w3.org/2001/XMLSchema#");
std::string AbstractParserTest::RDF("http://www.w3.org/1999/02/22-rdf-syntax-ns#");

AbstractParserTest::AbstractParserTest() : m_tripleVector() {
}

void AbstractParserTest::reportError(const size_t line, const size_t column, const char* const errorDescription) {
    std::ostringstream message;
    message << "Line = " << line << ", column = " << column << ": " << errorDescription;
    throw RDF_STORE_EXCEPTION(message.str());
}

void AbstractParserTest::consumeTriple(const size_t line, const size_t column, const ResourceText& subject, const ResourceText& predicate, const ResourceText& object) {
    if (m_tripleVectorIterator == m_tripleVector.end()) {
        std::ostringstream message;
        message << "The vector of control triples is too short: triple" << std::endl << std::endl << "  ";
        outputTriple(message, subject.m_lexicalForm, subject.m_resourceType, predicate.m_lexicalForm, predicate.m_resourceType, object.m_lexicalForm, object.m_datatypeIRI, object.m_resourceType);
        message << std::endl << std::endl << "is missing.";
        throw CppTest::AssertionError(__FILE__, __LINE__, message.str());

    }
    else if (subject.m_lexicalForm != m_tripleVectorIterator->m_subject || subject.m_resourceType != m_tripleVectorIterator->m_subjectType ||
        predicate.m_lexicalForm != m_tripleVectorIterator->m_predicate || predicate.m_resourceType != m_tripleVectorIterator->m_predicateType ||
        object.m_lexicalForm != m_tripleVectorIterator->m_object || object.m_resourceType != m_tripleVectorIterator->m_objectType ||
        (object.m_resourceType == LITERAL && object.m_datatypeIRI != m_tripleVectorIterator->m_objectDatatypeIRI)) {
        std::ostringstream message;
        message << "Expected triple" << std::endl << std::endl << "  ";
        outputTriple(message, m_tripleVectorIterator->m_subject, m_tripleVectorIterator->m_subjectType, m_tripleVectorIterator->m_predicate, m_tripleVectorIterator->m_predicateType, m_tripleVectorIterator->m_object, m_tripleVectorIterator->m_objectDatatypeIRI, m_tripleVectorIterator->m_objectType);
        message << std::endl << std::endl << "but got triple" << std::endl << std::endl << "  ";
        outputTriple(message, subject.m_lexicalForm, subject.m_resourceType, predicate.m_lexicalForm, predicate.m_resourceType, object.m_lexicalForm, object.m_datatypeIRI, object.m_resourceType);
        message << '.';
        throw CppTest::AssertionError(__FILE__, __LINE__, message.str());
    }
    m_tripleVectorIterator++;
}

#endif
