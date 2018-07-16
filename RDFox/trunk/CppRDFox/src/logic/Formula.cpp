// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "Logic.h"

_Formula::_Formula(_LogicFactory* const factory, const size_t hash) : _LogicObject(factory, hash) {
}

_Formula::~_Formula() {
}

std::unordered_set<Variable> _Formula::getFreeVariables() const {
    std::unordered_set<Variable> result;
    getFreeVariablesEx(result);
    return result;
}
