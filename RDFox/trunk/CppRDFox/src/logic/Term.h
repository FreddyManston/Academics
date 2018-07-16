// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef TERM_H_
#define TERM_H_

#include "BuiltinExpression.h"

class _Term : public _BuiltinExpression {

protected:

    _Term(_LogicFactory* const factory, const size_t hash);

public:

    virtual ~_Term();

    always_inline Term clone(const LogicFactory& logicFactory) const {
        return static_pointer_cast<Term>(doClone(logicFactory));
    }

    always_inline Term applyEx(const Substitution& substitution, const bool renameImplicitExistentialVariables, size_t& formulaWithImplicitExistentialVariablesCounter) const {
        return static_pointer_cast<Term>(doApply(substitution, renameImplicitExistentialVariables, formulaWithImplicitExistentialVariablesCounter));
    }

    always_inline Term apply(const Substitution& substitution) const {
        size_t formulaWithImplicitExistentialVariablesCounter = 0;
        return applyEx(substitution, false, formulaWithImplicitExistentialVariablesCounter);
    }

};

#endif /* TERM_H_ */
