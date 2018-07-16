// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef CONJUNCTION_H_
#define CONJUNCTION_H_

#include "Formula.h"

class _Conjunction : public _Formula {

    friend class _LogicFactory::FactoryInterningManager<Conjunction>;

protected:

    std::vector<Formula> m_conjuncts;

    _Conjunction(_LogicFactory* const factory, const size_t hash, const std::vector<Formula>& conjuncts);

    _Conjunction(_LogicFactory* const factory, const size_t hash, const Formula& conjunct1, const Formula& conjunct2);

    static size_t hashCodeFor(const std::vector<Formula>& conjuncts);

    bool isEqual(const std::vector<Formula>& conjuncts) const;

    static size_t hashCodeFor(const Formula& conjunct1, const Formula& conjunct2);

    bool isEqual(const Formula& conjunct1, const Formula& conjunct2) const;

    virtual LogicObject doClone(const LogicFactory& logicFactory) const;

    virtual Formula doApply(const Substitution& substitution, const bool renameImplicitExistentialVariables, size_t& formulaWithImplicitExistentialVariablesCounter) const;

public:

    virtual ~_Conjunction();

    always_inline Conjunction clone(const LogicFactory& logicFactory) const {
        return static_pointer_cast<Conjunction>(doClone(logicFactory));
    }

    virtual const std::vector<Formula>& getConjuncts() const;

    virtual size_t getNumberOfConjuncts() const;

    virtual const Formula& getConjunct(const size_t index) const;

    virtual FormulaType getType() const;

    virtual bool isGround() const;

    virtual void getFreeVariablesEx(std::unordered_set<Variable>& freeVariables) const;

    always_inline Conjunction applyEx(const Substitution& substitution, const bool renameImplicitExistentialVariables, size_t& formulaWithImplicitExistentialVariablesCounter) const {
        return static_pointer_cast<Conjunction>(doApply(substitution, renameImplicitExistentialVariables, formulaWithImplicitExistentialVariablesCounter));
    }

    always_inline Conjunction apply(const Substitution& substitution) const {
        size_t formulaWithImplicitExistentialVariablesCounter = 0;
        return applyEx(substitution, false, formulaWithImplicitExistentialVariablesCounter);
    }

    virtual void accept(LogicObjectVisitor& visitor) const;

    virtual std::string toString(const Prefixes& prefixes) const;

};

#endif /* CONJUNCTION_H_ */
