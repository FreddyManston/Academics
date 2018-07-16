// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef MINUS_H_
#define MINUS_H_

#include "Formula.h"

class _Minus : public _Formula {

    friend class _LogicFactory::FactoryInterningManager<Minus>;

protected:

    Formula m_main;
    std::vector<Formula> m_subtrahends;

    _Minus(_LogicFactory* const factory, const size_t hash, const Formula& main, const std::vector<Formula>& subtrahends);

    _Minus(_LogicFactory* const factory, const size_t hash, const Formula& main, const Formula& subtrahend);

    static size_t hashCodeFor(const Formula& main, const std::vector<Formula>& subtrahends);

    bool isEqual(const Formula& main, const std::vector<Formula>& subtrahends) const;

    static size_t hashCodeFor(const Formula& main, const Formula& subtrahend);

    bool isEqual(const Formula& main, const Formula& subtrahend) const;

    virtual LogicObject doClone(const LogicFactory& logicFactory) const;

    virtual Formula doApply(const Substitution& substitution, const bool renameImplicitExistentialVariables, size_t& formulaWithImplicitExistentialVariablesCounter) const;

public:

    virtual ~_Minus();

    always_inline Minus clone(const LogicFactory& logicFactory) const {
        return static_pointer_cast<Minus>(doClone(logicFactory));
    }

    virtual const Formula& getMain() const;

    virtual const std::vector<Formula>& getSubtrahends() const;

    virtual size_t getNumberOfSubtrahends() const;

    virtual const Formula& getSubtrahend(const size_t index) const;

    virtual FormulaType getType() const;

    virtual bool isGround() const;

    virtual void getFreeVariablesEx(std::unordered_set<Variable>& freeVariables) const;

    always_inline Minus applyEx(const Substitution& substitution, const bool renameImplicitExistentialVariables, size_t& formulaWithImplicitExistentialVariablesCounter) const {
        return static_pointer_cast<Minus>(doApply(substitution, renameImplicitExistentialVariables, formulaWithImplicitExistentialVariablesCounter));
    }

    always_inline Minus apply(const Substitution& substitution) const {
        size_t formulaWithImplicitExistentialVariablesCounter = 0;
        return applyEx(substitution, false, formulaWithImplicitExistentialVariablesCounter);
    }

    virtual void accept(LogicObjectVisitor& visitor) const;

    virtual std::string toString(const Prefixes& prefixes) const;

};

#endif /* MINUS_H_ */
