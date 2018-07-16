// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef LITERAL_H_
#define LITERAL_H_

#include "Formula.h"

class _Literal : public _Formula {

protected:

    std::vector<Term> m_arguments;

    _Literal(_LogicFactory* const factory, const size_t hash, const std::vector<Term>& arguments);

    _Literal(_LogicFactory* const factory, const size_t hash);

public:

    virtual ~_Literal();

    always_inline Literal clone(const LogicFactory& logicFactory) const {
        return static_pointer_cast<Literal>(doClone(logicFactory));
    }

    virtual const std::vector<Term>& getArguments() const;

    virtual size_t getNumberOfArguments() const;

    virtual const Term& getArgument(const size_t index) const;

    virtual bool isGround() const;

    virtual void getFreeVariablesEx(std::unordered_set<Variable>& freeVariables) const;

    always_inline Literal applyEx(const Substitution& substitution, const bool renameImplicitExistentialVariables, size_t& formulaWithImplicitExistentialVariablesCounter) const {
        return static_pointer_cast<Literal>(doApply(substitution, renameImplicitExistentialVariables, formulaWithImplicitExistentialVariablesCounter));
    }

    always_inline Literal apply(const Substitution& substitution) const {
        size_t formulaWithImplicitExistentialVariablesCounter = 0;
        return applyEx(substitution, false, formulaWithImplicitExistentialVariablesCounter);
    }

};

#endif /* LITERAL_H_ */
