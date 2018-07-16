// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef FILTER_H_
#define FILTER_H_

#include "AtomicFormula.h"
#include "BuiltinExpression.h"

class _Filter : public _AtomicFormula {

    friend class _LogicFactory::FactoryInterningManager<Filter>;

protected:

    BuiltinExpression m_builtinExpression;

    _Filter(_LogicFactory* const factory, const size_t hash, const BuiltinExpression& builtinExpression);

    static size_t hashCodeFor(const BuiltinExpression& builtinExpression);

    bool isEqual(const BuiltinExpression& builtinExpression) const;

    virtual LogicObject doClone(const LogicFactory& logicFactory) const;

    virtual Formula doApply(const Substitution& substitution, const bool renameImplicitExistentialVariables, size_t& formulaWithImplicitExistentialVariablesCounter) const;

public:

    virtual ~_Filter();

    always_inline Filter clone(const LogicFactory& logicFactory) const {
        return static_pointer_cast<Filter>(doClone(logicFactory));
    }

    virtual const BuiltinExpression& getBuiltinExpression() const;

    virtual FormulaType getType() const;

    virtual bool isGround() const;

    always_inline Filter applyEx(const Substitution& substitution, const bool renameImplicitExistentialVariables, size_t& formulaWithImplicitExistentialVariablesCounter) const {
        return static_pointer_cast<Filter>(doApply(substitution, renameImplicitExistentialVariables, formulaWithImplicitExistentialVariablesCounter));
    }

    always_inline Filter apply(const Substitution& substitution) const {
        size_t formulaWithImplicitExistentialVariablesCounter = 0;
        return applyEx(substitution, false, formulaWithImplicitExistentialVariablesCounter);
    }

    virtual void accept(LogicObjectVisitor& visitor) const;

    virtual std::string toString(const Prefixes& prefixes) const;

};

#endif /* FILTER_H_ */
