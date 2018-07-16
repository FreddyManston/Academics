// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef RULE_H_
#define RULE_H_

#include "Formula.h"

class _Rule : public _Formula {

    friend class _LogicFactory::FactoryInterningManager<Rule>;

protected:

    std::vector<Atom> m_head;
    std::vector<Literal> m_body;

    _Rule(_LogicFactory* const factory, const size_t hash, const Atom& head, const std::vector<Literal>& body);

    _Rule(_LogicFactory* const factory, const size_t hash, const std::vector<Atom>& head, const std::vector<Literal>& body);

    static size_t hashCodeFor(const Atom& head, const std::vector<Literal>& body);

    static size_t hashCodeFor(const std::vector<Atom>& head, const std::vector<Literal>& body);

    bool isEqual(const Atom& head, const std::vector<Literal>& body) const;

    bool isEqual(const std::vector<Atom>& head, const std::vector<Literal>& body) const;

    virtual LogicObject doClone(const LogicFactory& logicFactory) const;

    virtual Formula doApply(const Substitution& substitution, const bool renameImplicitExistentialVariables, size_t& formulaWithImplicitExistentialVariablesCounter) const;

public:

    virtual ~_Rule();

    always_inline Rule clone(const LogicFactory& logicFactory) const {
        return static_pointer_cast<Rule>(doClone(logicFactory));
    }

    virtual const std::vector<Atom>& getHead() const;

    virtual size_t getNumberOfHeadAtoms() const;

    virtual const Atom& getHead(const size_t index) const;
    
    virtual const std::vector<Literal>& getBody() const;

    virtual size_t getNumberOfBodyLiterals() const;

    virtual const Literal& getBody(const size_t index) const;

    virtual FormulaType getType() const;

    virtual bool isGround() const;

    virtual void getFreeVariablesEx(std::unordered_set<Variable>& freeVariables) const;

    always_inline Rule applyEx(const Substitution& substitution, const bool renameImplicitExistentialVariables, size_t& formulaWithImplicitExistentialVariablesCounter) const {
        return static_pointer_cast<Rule>(doApply(substitution, renameImplicitExistentialVariables, formulaWithImplicitExistentialVariablesCounter));
    }

    always_inline Rule apply(const Substitution& substitution) const {
        size_t formulaWithImplicitExistentialVariablesCounter = 0;
        return applyEx(substitution, false, formulaWithImplicitExistentialVariablesCounter);
    }

    virtual void accept(LogicObjectVisitor& visitor) const;

    virtual std::string toString(const Prefixes& prefixes) const;

};

#endif /* RULE_H_ */
