// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef BUILTINFUNCTIONCALL_H_
#define BUILTINFUNCTIONCALL_H_

#include "BuiltinExpression.h"

class _BuiltinFunctionCall : public _BuiltinExpression {

    friend class _LogicFactory::FactoryInterningManager<BuiltinFunctionCall>;

protected:

    std::string m_functionName;
    std::vector<BuiltinExpression> m_arguments;

    _BuiltinFunctionCall(_LogicFactory* const factory, const size_t hash, const char* const functionName, const std::vector<BuiltinExpression>& arguments);

    static size_t hashCodeFor(const char* const functionName, const std::vector<BuiltinExpression>& arguments);

    bool isEqual(const char* const functionName, const std::vector<BuiltinExpression>& arguments) const;

    virtual LogicObject doClone(const LogicFactory& logicFactory) const;

    virtual BuiltinExpression doApply(const Substitution& substitution, const bool renameImplicitExistentialVariables, size_t& formulaWithImplicitExistentialVariablesCounter) const;

public:

    virtual ~_BuiltinFunctionCall();

    always_inline BuiltinFunctionCall clone(const LogicFactory& logicFactory) const {
        return static_pointer_cast<BuiltinFunctionCall>(doClone(logicFactory));
    }

    virtual const std::string& getFunctionName() const;

    virtual const std::vector<BuiltinExpression>& getArguments() const;

    virtual size_t getNumberOfArguments() const;

    virtual const BuiltinExpression& getArgument(const size_t index) const;

    virtual BuiltinExpressionType getType() const;

    virtual bool isGround() const;

    virtual void getFreeVariablesEx(std::unordered_set<Variable>& freeVariables) const;

    always_inline BuiltinFunctionCall applyEx(const Substitution& substitution, const bool renameImplicitExistentialVariables, size_t& formulaWithImplicitExistentialVariablesCounter) const {
        return static_pointer_cast<BuiltinFunctionCall>(doApply(substitution, renameImplicitExistentialVariables, formulaWithImplicitExistentialVariablesCounter));
    }

    always_inline BuiltinFunctionCall apply(const Substitution& substitution) const {
        size_t formulaWithImplicitExistentialVariablesCounter = 0;
        return applyEx(substitution, false, formulaWithImplicitExistentialVariablesCounter);
    }

    virtual void accept(LogicObjectVisitor& visitor) const;

    virtual std::string toString(const Prefixes& prefixes) const;

};

#endif /* BUILTINFUNCTIONCALL_H_ */
