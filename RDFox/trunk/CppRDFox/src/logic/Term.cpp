// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "Logic.h"

_Term::_Term(_LogicFactory* const factory, const size_t hash) : _BuiltinExpression(factory, hash) {
}

_Term::~_Term() {
}
