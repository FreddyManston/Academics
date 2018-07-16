// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef EXISTENCEEXPRESSION_H_
#define EXISTENCEEXPRESSION_H_

#include "BuiltinExpression.h"

class _ExistenceExpression : public _BuiltinExpression {

    friend class _LogicFactory::FactoryInterningManager<ExistenceExpression>;

protected:

    const bool m_positive;
    Formula m_formula;

    _ExistenceExpression(_LogicFactory* const factory, const size_t hash, const bool positive, const Formula& formula);

    static size_t hashCodeFor(const bool positive, const Formula& formula);

    bool isEqual(const bool positive, const Formula& formula) const;

    virtual LogicObject doClone(const LogicFactory& logicFactory) const;

    virtual BuiltinExpression doApply(const Substitution& substitution, const bool renameImplicitExistentialVariables, size_t& formulaWithImplicitExistentialVariablesCounter) const;

public:

    virtual ~_ExistenceExpression();

    always_inline ExistenceExpression clone(const LogicFactory& logicFactory) const {
        return static_pointer_cast<ExistenceExpression>(doClone(logicFactory));
    }

    virtual bool isPositive() const;

    virtual const Formula& getFormula() const;

    virtual BuiltinExpressionType getType() const;

    virtual bool isGround() const;

    virtual void getFreeVariablesEx(std::unordered_set<Variable>& freeVariables) const;

    always_inline ExistenceExpression applyEx(const Substitution& substitution, const bool renameImplicitExistentialVariables, size_t& formulaWithImplicitExistentialVariablesCounter) const {
        return static_pointer_cast<ExistenceExpression>(doApply(substitution, renameImplicitExistentialVariables, formulaWithImplicitExistentialVariablesCounter));
    }

    always_inline ExistenceExpression apply(const Substitution& substitution) const {
        size_t formulaWithImplicitExistentialVariablesCounter = 0;
        return applyEx(substitution, false, formulaWithImplicitExistentialVariablesCounter);
    }

    virtual void accept(LogicObjectVisitor& visitor) const;

    virtual std::string toString(const Prefixes& prefixes) const;

};

#endif /* EXISTENCEEXPRESSION_H_ */
