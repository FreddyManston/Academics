// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef FORMULA_H_
#define FORMULA_H_

#include "LogicObject.h"

enum FormulaType {
    ATOM_FORMULA,
    BIND_FORMULA,
    FILTER_FORMULA,
    NEGATION_FORMULA,
    AGGREGATE_FORMULA,
    CONJUNCTION_FORMULA,
    DISJUNCTION_FORMULA,
    OPTIONAL_FORMULA,
    MINUS_FORMULA,
    VALUES_FORMULA,
    QUERY_FORMULA,
    RULE_FORMULA
};

always_inline bool isLiteralType(const FormulaType type) {
    return type == ATOM_FORMULA || type == FILTER_FORMULA || type == BIND_FORMULA;
}

class _Formula : public _LogicObject {

protected:

    _Formula(_LogicFactory* const factory, const size_t hash);

    virtual Formula doApply(const Substitution& substitution, const bool renameImplicitExistentialVariables, size_t& formulaWithImplicitExistentialVariablesCounter) const = 0;

public:

    virtual ~_Formula();

    always_inline Formula clone(const LogicFactory& logicFactory) const {
        return static_pointer_cast<Formula>(doClone(logicFactory));
    }

    virtual FormulaType getType() const = 0;

    virtual bool isGround() const = 0;

    std::unordered_set<Variable> getFreeVariables() const;

    virtual void getFreeVariablesEx(std::unordered_set<Variable>& freeVariables) const = 0;

    always_inline Formula applyEx(const Substitution& substitution, const bool renameImplicitExistentialVariables, size_t& formulaWithImplicitExistentialVariablesCounter) const {
        return doApply(substitution, renameImplicitExistentialVariables, formulaWithImplicitExistentialVariablesCounter);
    }

    always_inline Formula apply(const Substitution& substitution) const {
        size_t formulaWithImplicitExistentialVariablesCounter = 0;
        return applyEx(substitution, false, formulaWithImplicitExistentialVariablesCounter);
    }

};

#endif /* FORMULA_H_ */
