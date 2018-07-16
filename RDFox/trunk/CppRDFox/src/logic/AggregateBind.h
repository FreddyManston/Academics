// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef AGGREGATEBIND_H_
#define AGGREGATEBIND_H_

#include "LogicObject.h"

class _AggregateBind : public _LogicObject {

    friend class _LogicFactory::FactoryInterningManager<AggregateBind>;

protected:

    std::string m_functionName;
    const bool m_distinct;
    std::vector<BuiltinExpression> m_arguments;
    Term m_boundTerm;

    _AggregateBind(_LogicFactory* const factory, const size_t hash, const char* const functionName, const bool distinct, const std::vector<BuiltinExpression>& arguments, const Term& boundTerm);

    _AggregateBind(_LogicFactory* const factory, const size_t hash, const char* const functionName, const bool distinct, const BuiltinExpression& argument, const Term& boundTerm);

    static size_t hashCodeFor(const char* const functionName, const bool distinct, const std::vector<BuiltinExpression>& arguments, const Term& boundTerm);

    bool isEqual(const char* const functionName, const bool distinct, const std::vector<BuiltinExpression>& arguments, const Term& boundTerm) const;

    static size_t hashCodeFor(const char* const functionName, const bool distinct, const BuiltinExpression& argument, const Term& boundTerm);

    bool isEqual(const char* const functionName, const bool distinct, const BuiltinExpression& argument, const Term& boundTerm) const;

    virtual LogicObject doClone(const LogicFactory& logicFactory) const;
    
    virtual AggregateBind doApply(const Substitution& substitution, const bool renameImplicitExistentialVariables, size_t& formulaWithImplicitExistentialVariablesCounter) const;

public:

    virtual ~_AggregateBind();

    always_inline AggregateBind clone(const LogicFactory& logicFactory) const {
        return static_pointer_cast<AggregateBind>(doClone(logicFactory));
    }

    virtual const std::string& getFunctionName() const;

    virtual bool isDistinct() const;

    virtual const std::vector<BuiltinExpression>& getArguments() const;

    virtual size_t getNumberOfArguments() const;

    virtual const BuiltinExpression& getArgument(const size_t index) const;

    virtual const Term& getBoundTerm() const;

    virtual bool isGround();

    std::unordered_set<Variable> getFreeVariables() const;

    virtual void getFreeVariablesEx(std::unordered_set<Variable>& freeVariables) const;

    always_inline AggregateBind applyEx(const Substitution& substitution, const bool renameImplicitExistentialVariables, size_t& formulaWithImplicitExistentialVariablesCounter) const {
        return doApply(substitution, renameImplicitExistentialVariables, formulaWithImplicitExistentialVariablesCounter);
    }

    always_inline AggregateBind apply(const Substitution& substitution) const {
        size_t formulaWithImplicitExistentialVariablesCounter = 0;
        return applyEx(substitution, false, formulaWithImplicitExistentialVariablesCounter);
    }

    virtual void accept(LogicObjectVisitor& visitor) const;

    virtual std::string toString(const Prefixes& prefixes) const;

};

#endif /* AGGREGATEBIND_H_ */
