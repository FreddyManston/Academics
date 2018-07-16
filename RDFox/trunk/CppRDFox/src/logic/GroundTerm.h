// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef GROUNDTERM_H_
#define GROUNDTERM_H_

#include "Term.h"

class _GroundTerm : public _Term {

protected:

    _GroundTerm(_LogicFactory* const factory, const size_t hash);

public:

    virtual ~_GroundTerm();

    always_inline GroundTerm clone(const LogicFactory& logicFactory) const {
        return static_pointer_cast<GroundTerm>(doClone(logicFactory));
    }

    virtual bool isGround() const;

    always_inline GroundTerm applyEx(const Substitution& substitution, const bool renameImplicitExistentialVariables, size_t& formulaWithImplicitExistentialVariablesCounter) const {
        return static_pointer_cast<GroundTerm>(doApply(substitution, renameImplicitExistentialVariables, formulaWithImplicitExistentialVariablesCounter));
    }

    always_inline GroundTerm apply(const Substitution& substitution) const {
        size_t formulaWithImplicitExistentialVariablesCounter = 0;
        return applyEx(substitution, false, formulaWithImplicitExistentialVariablesCounter);
    }

};

#endif /* GROUNDTERM_H_ */
