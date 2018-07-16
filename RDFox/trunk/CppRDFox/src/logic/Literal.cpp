// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "Logic.h"

_Literal::_Literal(_LogicFactory* const factory, const size_t hash, const std::vector<Term>& arguments) :
    _Formula(factory, hash),
    m_arguments(arguments)
{
}

_Literal::_Literal(_LogicFactory* const factory, const size_t hash) :
    _Formula(factory, hash),
    m_arguments()
{
}

_Literal::~_Literal() {
}

const std::vector<Term>& _Literal::getArguments() const {
    return m_arguments;
}

size_t _Literal::getNumberOfArguments() const {
    return m_arguments.size();
}

const Term& _Literal::getArgument(const size_t index) const {
    return m_arguments[index];
}

bool _Literal::isGround() const {
    for (auto iterator = m_arguments.begin(); iterator != m_arguments.end(); ++iterator)
        if (!(*iterator)->isGround())
            return false;
    return true;
}

void _Literal::getFreeVariablesEx(std::unordered_set<Variable>& freeVariables) const {
    for (auto iterator = m_arguments.begin(); iterator != m_arguments.end(); ++iterator)
        (*iterator)->getFreeVariablesEx(freeVariables);
}
