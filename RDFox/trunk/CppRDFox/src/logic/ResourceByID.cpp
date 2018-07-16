// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "Logic.h"

_ResourceByID::_ResourceByID(_LogicFactory* const factory, const size_t hash, const ResourceID resourceID) :
    _GroundTerm(factory, hash),
    m_resourceID(resourceID)
{
}

size_t _ResourceByID::hashCodeFor(const ResourceID resourceID) {
    return static_cast<size_t>(resourceID) * 2654435761;
}

bool _ResourceByID::isEqual(const ResourceID resourceID) const {
    return m_resourceID == resourceID;
}

LogicObject _ResourceByID::doClone(const LogicFactory& logicFactory) const {
    return logicFactory->getResourceByID(m_resourceID);
}

BuiltinExpression _ResourceByID::doApply(const Substitution& substitution, const bool renameImplicitExistentialVariables, size_t& formulaWithImplicitExistentialVariablesCounter) const {
    return ResourceByID(this);
}

_ResourceByID::~_ResourceByID() {
    m_factory->dispose(this);
}

ResourceID _ResourceByID::getResourceID() const {
    return m_resourceID;
}

BuiltinExpressionType _ResourceByID::getType() const {
    return RESOURCE_BY_ID;
}

void _ResourceByID::getFreeVariablesEx(std::unordered_set<Variable>& freeVariables) const {
}

void _ResourceByID::accept(LogicObjectVisitor& visitor) const {
    visitor.visit(ResourceByID(this));
}

std::string _ResourceByID::toString(const Prefixes& prefixes) const {
    std::ostringstream value;
    value << "|" << m_resourceID << "|";
    return value.str();
}
