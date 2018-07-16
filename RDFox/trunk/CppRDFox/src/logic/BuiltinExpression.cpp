// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "Logic.h"

_BuiltinExpression::_BuiltinExpression(_LogicFactory* const factory, const size_t hash) : _LogicObject(factory, hash) {
}

_BuiltinExpression::~_BuiltinExpression() {
}

std::unordered_set<Variable> _BuiltinExpression::getFreeVariables() const {
    std::unordered_set<Variable> result;
    getFreeVariablesEx(result);
    return result;
}
