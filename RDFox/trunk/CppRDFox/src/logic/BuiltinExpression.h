// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef BUILTINEXPRESSION_H_
#define BUILTINEXPRESSION_H_

#include "LogicObject.h"

enum BuiltinExpressionType {
    VARIABLE,
    RESOURCE_BY_ID,
    RESOURCE_BY_NAME,
    BUILTIN_FUNCTION_CALL,
    EXISTENCE_EXPRESSION
};

class _BuiltinExpression : public _LogicObject {

protected:

    _BuiltinExpression(_LogicFactory* const factory, const size_t hash);

    virtual BuiltinExpression doApply(const Substitution& substitution, const bool renameImplicitExistentialVariables, size_t& formulaWithImplicitExistentialVariablesCounter) const = 0;

public:

    virtual ~_BuiltinExpression();

    always_inline BuiltinExpression clone(const LogicFactory& logicFactory) const {
        return static_pointer_cast<BuiltinExpression>(doClone(logicFactory));
    }

    virtual BuiltinExpressionType getType() const = 0;

    virtual bool isGround() const = 0;

    std::unordered_set<Variable> getFreeVariables() const;

    virtual void getFreeVariablesEx(std::unordered_set<Variable>& freeVariables) const = 0;

    always_inline BuiltinExpression applyEx(const Substitution& substitution, const bool renameImplicitExistentialVariables, size_t& formulaWithImplicitExistentialVariablesCounter) const {
        return doApply(substitution, renameImplicitExistentialVariables, formulaWithImplicitExistentialVariablesCounter);
    }

    always_inline BuiltinExpression apply(const Substitution& substitution) const {
        size_t formulaWithImplicitExistentialVariablesCounter = 0;
        return applyEx(substitution, false, formulaWithImplicitExistentialVariablesCounter);
    }

};

#endif /* BUILTINEXPRESSION_H_ */
