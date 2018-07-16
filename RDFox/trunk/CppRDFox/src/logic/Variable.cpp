// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "Logic.h"

_Variable::_Variable(_LogicFactory* const factory, const size_t hash, const char* const name) : _Term(factory, hash), m_name(name) {
}

size_t _Variable::hashCodeFor(const char* name) {
    return stringHashCode(name);
}

bool _Variable::isEqual(const char* const name) const {
    return m_name == name;
}

LogicObject _Variable::doClone(const LogicFactory& logicFactory) const {
    return logicFactory->getVariable(m_name);
}

BuiltinExpression _Variable::doApply(const Substitution& substitution, const bool renameImplicitExistentialVariables, size_t& formulaWithImplicitExistentialVariablesCounter) const {
    Variable thisVariable(this);
    Substitution::const_iterator iterator = substitution.find(thisVariable);
    if (iterator == substitution.end())
        return thisVariable;
    else
        return iterator->second;
}

_Variable::~_Variable() {
    m_factory->dispose(this);
}

const std::string& _Variable::getName() const {
    return m_name;
}

BuiltinExpressionType _Variable::getType() const {
    return VARIABLE;
}

bool _Variable::isGround() const {
    return false;
}

void _Variable::getFreeVariablesEx(std::unordered_set<Variable>& freeVariables) const {
    freeVariables.insert(Variable(this));
}

void _Variable::accept(LogicObjectVisitor& visitor) const {
    visitor.visit(Variable(this));
}

std::string _Variable::toString(const Prefixes& prefixes) const {
    return "?" + m_name;
}
