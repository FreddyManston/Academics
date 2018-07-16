// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "Logic.h"

_AtomicFormula::_AtomicFormula(_LogicFactory* const factory, const size_t hash, const std::vector<Term>& arguments) :
    _Literal(factory, hash, arguments)
{
}

_AtomicFormula::_AtomicFormula(_LogicFactory* const factory, const size_t hash) :
    _Literal(factory, hash)
{
}

_AtomicFormula::~_AtomicFormula() {
}
