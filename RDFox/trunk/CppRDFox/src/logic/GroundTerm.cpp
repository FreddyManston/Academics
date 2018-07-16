// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "Logic.h"

_GroundTerm::_GroundTerm(_LogicFactory* const factory, const size_t hash) : _Term(factory, hash) {
}

_GroundTerm::~_GroundTerm() {
}

bool _GroundTerm::isGround() const {
    return true;
}
