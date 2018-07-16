// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef BIND_H_
#define BIND_H_

#include "AtomicFormula.h"
#include "Variable.h"
#include "BuiltinExpression.h"

class _Bind : public _AtomicFormula {

    friend class _LogicFactory::FactoryInterningManager<Bind>;

protected:

    BuiltinExpression m_builtinExpression;
    Term m_boundTerm;

    _Bind(_LogicFactory* const factory, const size_t hash, const BuiltinExpression& builtinExpression, const Term& boundTerm);

    static size_t hashCodeFor(const BuiltinExpression& builtinExpression, const Term& boundTerm);

    bool isEqual(const BuiltinExpression& builtinExpression, const Term& boundTerm) const;

    virtual LogicObject doClone(const LogicFactory& logicFactory) const;

    virtual Formula doApply(const Substitution& substitution, const bool renameImplicitExistentialVariables, size_t& formulaWithImplicitExistentialVariablesCounter) const;

public:

    virtual ~_Bind();

    always_inline Bind clone(const LogicFactory& logicFactory) const {
        return static_pointer_cast<Bind>(doClone(logicFactory));
    }

    virtual const BuiltinExpression& getBuiltinExpression() const;

    virtual const Term& getBoundTerm() const;

    virtual FormulaType getType() const;

    virtual bool isGround() const;

    always_inline Bind applyEx(const Substitution& substitution, const bool renameImplicitExistentialVariables, size_t& formulaWithImplicitExistentialVariablesCounter) const {
        return static_pointer_cast<Bind>(doApply(substitution, renameImplicitExistentialVariables, formulaWithImplicitExistentialVariablesCounter));
    }

    always_inline Bind apply(const Substitution& substitution) const {
        size_t formulaWithImplicitExistentialVariablesCounter = 0;
        return applyEx(substitution, false, formulaWithImplicitExistentialVariablesCounter);
    }

    virtual void accept(LogicObjectVisitor& visitor) const;

    virtual std::string toString(const Prefixes& prefixes) const;

};

#endif /* BIND_H_ */
