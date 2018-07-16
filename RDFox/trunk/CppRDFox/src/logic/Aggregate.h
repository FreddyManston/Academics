// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef AGGREGATE_H_
#define AGGREGATE_H_

#include "Formula.h"

class _Aggregate : public _Literal {

    friend class _LogicFactory::FactoryInterningManager<Aggregate>;

protected:

    std::vector<AtomicFormula> m_atomicFormulas;
    const bool m_skipEmptyGroups;
    std::vector<Variable> m_groupVariables;
    std::vector<AggregateBind> m_aggregateBinds;

    _Aggregate(_LogicFactory* const factory, const size_t hash, const std::vector<AtomicFormula>& atomicFormulas, const bool skipEmptyGroups, const std::vector<Variable>& groupVariables, const std::vector<AggregateBind>& aggregateBinds);

    static size_t hashCodeFor(const std::vector<AtomicFormula>& atomicFormulas, const bool skipEmptyGroups, const std::vector<Variable>& groupVariables, const std::vector<AggregateBind>& aggregateBinds);

    bool isEqual(const std::vector<AtomicFormula>& atomicFormulas, const bool skipEmptyGroups, const std::vector<Variable>& groupVariables, const std::vector<AggregateBind>& aggregateBinds) const;

    virtual LogicObject doClone(const LogicFactory& logicFactory) const;

    virtual Formula doApply(const Substitution& substitution, const bool renameImplicitExistentialVariables, size_t& formulaWithImplicitExistentialVariablesCounter) const;

public:

    virtual ~_Aggregate();

    always_inline Aggregate clone(const LogicFactory& logicFactory) const {
        return static_pointer_cast<Aggregate>(doClone(logicFactory));
    }

    virtual const std::vector<AtomicFormula>& getAtomicFormulas() const;

    virtual size_t getNumberOfAtomicFormulas() const;

    virtual const AtomicFormula& getAtomicFormula(const size_t index) const;

    virtual bool getSkipEmptyGroups() const;

    virtual const std::vector<Variable>& getGroupVariables() const;

    virtual size_t getNumberOfGroupVariables() const;

    virtual const Variable& getGroupVariable(const size_t index) const;

    virtual const std::vector<AggregateBind>& getAggregateBinds() const;

    virtual size_t getNumberOfAggregateBinds() const;

    virtual const AggregateBind& getAggregateBind(const size_t index) const;

    virtual FormulaType getType() const;

    virtual bool isGround() const;

    virtual void getFreeVariablesEx(std::unordered_set<Variable>& freeVariables) const;

    always_inline Aggregate applyEx(const Substitution& substitution, const bool renameImplicitExistentialVariables, size_t& formulaWithImplicitExistentialVariablesCounter) const {
        return static_pointer_cast<Aggregate>(doApply(substitution, renameImplicitExistentialVariables, formulaWithImplicitExistentialVariablesCounter));
    }

    always_inline Aggregate apply(const Substitution& substitution) const {
        size_t formulaWithImplicitExistentialVariablesCounter = 0;
        return applyEx(substitution, false, formulaWithImplicitExistentialVariablesCounter);
    }

    virtual void accept(LogicObjectVisitor& visitor) const;

    virtual std::string toString(const Prefixes& prefixes) const;

};

#endif /* AGGREGATE_H_ */
