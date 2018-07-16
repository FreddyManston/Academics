// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "../util/Vocabulary.h"
#include "Logic.h"

_ResourceByName::_ResourceByName(_LogicFactory* const factory, const size_t hash, const ResourceType resourceType, const char* const lexicalForm, const char* const datatypeIRI) :
    _GroundTerm(factory, hash),
    m_resourceText(resourceType, lexicalForm, datatypeIRI)
{
}

size_t _ResourceByName::hashCodeFor(const ResourceType resourceType, const char* lexicalForm, const char* datatypeIRI) {
    size_t result = static_cast<size_t>(resourceType);
    result += (result << 10);
    result ^= (result >> 6);

    result += stringHashCode(lexicalForm);
    result += (result << 10);
    result ^= (result >> 6);

    result += stringHashCode(datatypeIRI);
    result += (result << 10);
    result ^= (result >> 6);

    return result;
}

bool _ResourceByName::isEqual(const ResourceType resourceType, const char* const lexicalForm, const char* const datatypeIRI) const {
    return m_resourceText.m_resourceType == resourceType && m_resourceText.m_lexicalForm == lexicalForm && m_resourceText.m_datatypeIRI == datatypeIRI;
}

LogicObject _ResourceByName::doClone(const LogicFactory& logicFactory) const {
    return logicFactory->getResourceByName(m_resourceText);
}

BuiltinExpression _ResourceByName::doApply(const Substitution& substitution, const bool renameImplicitExistentialVariables, size_t& formulaWithImplicitExistentialVariablesCounter) const {
    return ResourceByName(this);
}

_ResourceByName::~_ResourceByName() {
    m_factory->dispose(this);
}

const ResourceText& _ResourceByName::getResourceText() const {
    return m_resourceText;
}

BuiltinExpressionType _ResourceByName::getType() const {
    return RESOURCE_BY_NAME;
}

bool _ResourceByName::isRDFType() const {
    return m_resourceText.m_resourceType == IRI_REFERENCE && m_resourceText.m_lexicalForm == RDF_TYPE;
}

void _ResourceByName::getFreeVariablesEx(std::unordered_set<Variable>& freeVariables) const {
}

void _ResourceByName::accept(LogicObjectVisitor& visitor) const {
    visitor.visit(ResourceByName(this));
}

std::string _ResourceByName::toString(const Prefixes& prefixes) const {
    return m_resourceText.toString(prefixes);
}
