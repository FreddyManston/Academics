// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "Logic.h"

_Predicate::_Predicate(_LogicFactory* const factory, const size_t hash, const char* const name) : _LogicObject(factory, hash), m_name(name) {
}

size_t _Predicate::hashCodeFor(const char* const name) {
    return stringHashCode(name);
}

bool _Predicate::isEqual(const char* const name) const {
    return m_name == name;
}

LogicObject _Predicate::doClone(const LogicFactory& logicFactory) const {
    return logicFactory->getPredicate(m_name);
}

_Predicate::~_Predicate() {
    m_factory->dispose(this);
}

const std::string& _Predicate::getName() const {
    return m_name;
}

void _Predicate::accept(LogicObjectVisitor& visitor) const {
    visitor.visit(Predicate(this));
}

std::string _Predicate::toString(const Prefixes& prefixes) const {
    return prefixes.encodeIRI(m_name);
}
