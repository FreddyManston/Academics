// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef DISJUNCTION_H_
#define DISJUNCTION_H_

#include "Formula.h"

class _Disjunction : public _Formula {

    friend class _LogicFactory::FactoryInterningManager<Disjunction>;

protected:

    std::vector<Formula> m_disjuncts;

    _Disjunction(_LogicFactory* const factory, const size_t hash, const std::vector<Formula>& disjuncts);

    _Disjunction(_LogicFactory* const factory, const size_t hash, const Formula& disjunct1, const Formula& disjunct2);

    static size_t hashCodeFor(const std::vector<Formula>& disjuncts);

    bool isEqual(const std::vector<Formula>& disjuncts) const;

    static size_t hashCodeFor(const Formula& disjunct1, const Formula& disjunct2);

    bool isEqual(const Formula& disjunct1, const Formula& disjunct2) const;

    virtual LogicObject doClone(const LogicFactory& logicFactory) const;

    virtual Formula doApply(const Substitution& substitution, const bool renameImplicitExistentialVariables, size_t& formulaWithImplicitExistentialVariablesCounter) const;

public:

    virtual ~_Disjunction();

    always_inline Disjunction clone(const LogicFactory& logicFactory) const {
        return static_pointer_cast<Disjunction>(doClone(logicFactory));
    }

    virtual const std::vector<Formula>& getDisjuncts() const;

    virtual size_t getNumberOfDisjuncts() const;

    virtual const Formula& getDisjunct(const size_t index) const;

    virtual FormulaType getType() const;

    virtual bool isGround() const;

    virtual void getFreeVariablesEx(std::unordered_set<Variable>& freeVariables) const;

    always_inline Disjunction applyEx(const Substitution& substitution, const bool renameImplicitExistentialVariables, size_t& formulaWithImplicitExistentialVariablesCounter) const {
        return static_pointer_cast<Disjunction>(doApply(substitution, renameImplicitExistentialVariables, formulaWithImplicitExistentialVariablesCounter));
    }

    always_inline Disjunction apply(const Substitution& substitution) const {
        size_t formulaWithImplicitExistentialVariablesCounter = 0;
        return applyEx(substitution, false, formulaWithImplicitExistentialVariablesCounter);
    }

    virtual void accept(LogicObjectVisitor& visitor) const;

    virtual std::string toString(const Prefixes& prefixes) const;

};

#endif /* DISJUNCTION_H_ */
