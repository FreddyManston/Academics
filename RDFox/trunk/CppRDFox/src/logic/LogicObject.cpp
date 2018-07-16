// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "Logic.h"

_LogicObject::_LogicObject(_LogicFactory* const factory, const size_t hash) : m_referenceCount(0), m_factory(factory), m_hash(hash) {
}

_LogicObject::~_LogicObject() {
}
