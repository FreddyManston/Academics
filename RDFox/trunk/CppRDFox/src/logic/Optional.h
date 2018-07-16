// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef OPTIONAL_H_
#define OPTIONAL_H_

#include "Formula.h"

class _Optional : public _Formula {

    friend class _LogicFactory::FactoryInterningManager<Optional>;

protected:

    Formula m_main;
    std::vector<Formula> m_optionals;

    _Optional(_LogicFactory* const factory, const size_t hash, const Formula& main, const std::vector<Formula>& optionals);

    _Optional(_LogicFactory* const factory, const size_t hash, const Formula& main, const Formula& optional);

    static size_t hashCodeFor(const Formula& main, const std::vector<Formula>& optionals);

    bool isEqual(const Formula& main, const std::vector<Formula>& optionals) const;

    static size_t hashCodeFor(const Formula& main, const Formula& optional);

    bool isEqual(const Formula& main, const Formula& optional) const;

    virtual LogicObject doClone(const LogicFactory& logicFactory) const;

    virtual Formula doApply(const Substitution& substitution, const bool renameImplicitExistentialVariables, size_t& formulaWithImplicitExistentialVariablesCounter) const;

public:

    virtual ~_Optional();

    always_inline Optional clone(const LogicFactory& logicFactory) const {
        return static_pointer_cast<Optional>(doClone(logicFactory));
    }

    virtual const Formula& getMain() const;

    virtual const std::vector<Formula>& getOptionals() const;

    virtual size_t getNumberOfOptionals() const;

    virtual const Formula& getOptional(const size_t index) const;

    virtual FormulaType getType() const;

    virtual bool isGround() const;

    virtual void getFreeVariablesEx(std::unordered_set<Variable>& freeVariables) const;

    always_inline Optional applyEx(const Substitution& substitution, const bool renameImplicitExistentialVariables, size_t& formulaWithImplicitExistentialVariablesCounter) const {
        return static_pointer_cast<Optional>(doApply(substitution, renameImplicitExistentialVariables, formulaWithImplicitExistentialVariablesCounter));
    }

    always_inline Optional apply(const Substitution& substitution) const {
        size_t formulaWithImplicitExistentialVariablesCounter = 0;
        return applyEx(substitution, false, formulaWithImplicitExistentialVariablesCounter);
    }

    virtual void accept(LogicObjectVisitor& visitor) const;

    virtual std::string toString(const Prefixes& prefixes) const;

};

#endif /* OPTIONAL_H_ */
