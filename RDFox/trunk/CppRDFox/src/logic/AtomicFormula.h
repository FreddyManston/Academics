// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef ATOMICFORMULA_H_
#define ATOMICFORMULA_H_

#include "Literal.h"

class _AtomicFormula : public _Literal {

protected:

    _AtomicFormula(_LogicFactory* const factory, const size_t hash, const std::vector<Term>& arguments);

    _AtomicFormula(_LogicFactory* const factory, const size_t hash);

public:

    virtual ~_AtomicFormula();

    always_inline AtomicFormula clone(const LogicFactory& logicFactory) const {
        return static_pointer_cast<AtomicFormula>(doClone(logicFactory));
    }

    always_inline AtomicFormula applyEx(const Substitution& substitution, const bool renameImplicitExistentialVariables, size_t& formulaWithImplicitExistentialVariablesCounter) const {
        return static_pointer_cast<AtomicFormula>(doApply(substitution, renameImplicitExistentialVariables, formulaWithImplicitExistentialVariablesCounter));
    }

    always_inline AtomicFormula apply(const Substitution& substitution) const {
        size_t formulaWithImplicitExistentialVariablesCounter = 0;
        return applyEx(substitution, false, formulaWithImplicitExistentialVariablesCounter);
    }

};

#endif /* ATOMICFORMULA_H_ */
