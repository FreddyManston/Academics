// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef NEGATION_H_
#define NEGATION_H_

#include "Literal.h"

class _Negation : public _Literal {

    friend class _LogicFactory::FactoryInterningManager<Negation>;

protected:
    
    std::vector<Variable> m_existentialVariables;
    std::vector<AtomicFormula> m_atomicFormulas;
    
    _Negation(_LogicFactory* const factory, const size_t hash, const std::vector<Variable>& existentialVariables, const std::vector<AtomicFormula>& atomicFormulas);
    
    static size_t hashCodeFor(const std::vector<Variable>& existentialVariables, const std::vector<AtomicFormula>& atomicFormulas);
    
    bool isEqual(const std::vector<Variable>& existentialVariables, const std::vector<AtomicFormula>& atomicFormulas) const;
    
    virtual LogicObject doClone(const LogicFactory& logicFactory) const;
    
    virtual Formula doApply(const Substitution& substitution, const bool renameImplicitExistentialVariables, size_t& formulaWithImplicitExistentialVariablesCounter) const;
    
public:
    
    virtual ~_Negation();
    
    always_inline Negation clone(const LogicFactory& logicFactory) const {
        return static_pointer_cast<Negation>(doClone(logicFactory));
    }
    
    virtual const std::vector<Variable>& getExistentialVariables() const;

    virtual size_t getNumberOfExistentialVariables() const;

    virtual const Variable& getExistentialVariable(const size_t index) const;

    virtual const std::vector<AtomicFormula>& getAtomicFormulas() const;
    
    virtual size_t getNumberOfAtomicFormulas() const;

    virtual const AtomicFormula& getAtomicFormula(const size_t index) const;

    virtual FormulaType getType() const;
    
    virtual void getFreeVariablesEx(std::unordered_set<Variable>& freeVariables) const;
    
    always_inline Negation applyEx(const Substitution& substitution, const bool renameImplicitExistentialVariables, size_t& formulaWithImplicitExistentialVariablesCounter) const {
        return static_pointer_cast<Negation>(doApply(substitution, renameImplicitExistentialVariables, formulaWithImplicitExistentialVariablesCounter));
    }
    
    always_inline Negation apply(const Substitution& substitution) const {
        size_t formulaWithImplicitExistentialVariablesCounter = 0;
        return applyEx(substitution, false, formulaWithImplicitExistentialVariablesCounter);
    }
    
    virtual void accept(LogicObjectVisitor& visitor) const;
    
    virtual std::string toString(const Prefixes& prefixes) const;
    
};

#endif /* NEGATION_H_ */
