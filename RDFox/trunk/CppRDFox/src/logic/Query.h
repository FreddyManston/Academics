// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef QUERY_H_
#define QUERY_H_

#include "Formula.h"

class _Query : public _Formula {

    friend class _LogicFactory::FactoryInterningManager<Query>;

protected:

    bool m_distinct;
    std::vector<Term> m_answerTerms;
    Formula m_queryFormula;

    _Query(_LogicFactory* const factory, const size_t hash, const bool distinct, const std::vector<Term>& answerTerms, const Formula& queryFormula);

    static size_t hashCodeFor(const bool distinct, const std::vector<Term>& answerTerms, const Formula& queryFormula);

    bool isEqual(const bool distinct, const std::vector<Term>& answerTerms, const Formula& queryFormula) const;

    virtual LogicObject doClone(const LogicFactory& logicFactory) const;

    virtual Formula doApply(const Substitution& substitution, const bool renameImplicitExistentialVariables, size_t& formulaWithImplicitExistentialVariablesCounter) const;

public:

    virtual ~_Query();

    always_inline Query clone(const LogicFactory& logicFactory) const {
        return static_pointer_cast<Query>(doClone(logicFactory));
    }

    virtual bool isDistinct() const;

    virtual const std::vector<Term>& getAnswerTerms() const;

    virtual size_t getNumberOfAnswerTerms() const;

    virtual const Term& getAnswerTerm(const size_t index) const;

    virtual const Formula& getQueryFormula() const;

    virtual FormulaType getType() const;

    virtual bool isGround() const;

    virtual void getFreeVariablesEx(std::unordered_set<Variable>& freeVariables) const;

    always_inline Query applyEx(const Substitution& substitution, const bool renameImplicitExistentialVariables, size_t& formulaWithImplicitExistentialVariablesCounter) const {
        return static_pointer_cast<Query>(doApply(substitution, renameImplicitExistentialVariables, formulaWithImplicitExistentialVariablesCounter));
    }

    always_inline Query apply(const Substitution& substitution) const {
        size_t formulaWithImplicitExistentialVariablesCounter = 0;
        return applyEx(substitution, false, formulaWithImplicitExistentialVariablesCounter);
    }

    virtual void accept(LogicObjectVisitor& visitor) const;

    virtual std::string toString(const Prefixes& prefixes) const;

};

#endif /* QUERY_H_ */
