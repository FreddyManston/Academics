// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef VARIABLE_H_
#define VARIABLE_H_

#include "Term.h"

class _Variable : public _Term {

    friend class _LogicFactory::FactoryInterningManager<Variable>;

protected:

    std::string m_name;

    _Variable(_LogicFactory* const factory, const size_t hash, const char* const name);

    static size_t hashCodeFor(const char* const name);

    bool isEqual(const char* const name) const;

    virtual LogicObject doClone(const LogicFactory& logicFactory) const;

    virtual BuiltinExpression doApply(const Substitution& substitution, const bool renameImplicitExistentialVariables, size_t& formulaWithImplicitExistentialVariablesCounter) const;

public:

    virtual ~_Variable();

    always_inline Variable clone(const LogicFactory& logicFactory) const {
        return static_pointer_cast<Variable>(doClone(logicFactory));
    }

    virtual const std::string& getName() const;

    virtual BuiltinExpressionType getType() const;

    virtual bool isGround() const;

    virtual void getFreeVariablesEx(std::unordered_set<Variable>& freeVariables) const;

    always_inline Term applyEx(const Substitution& substitution, const bool renameImplicitExistentialVariables, size_t& formulaWithImplicitExistentialVariablesCounter) const {
        return static_pointer_cast<Term>(doApply(substitution, renameImplicitExistentialVariables, formulaWithImplicitExistentialVariablesCounter));
    }

    always_inline Term apply(const Substitution& substitution) const {
        size_t formulaWithImplicitExistentialVariablesCounter = 0;
        return applyEx(substitution, false, formulaWithImplicitExistentialVariablesCounter);
    }

    virtual void accept(LogicObjectVisitor& visitor) const;

    virtual std::string toString(const Prefixes& prefixes) const;

};

#endif /* VARIABLE_H_ */
