// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef ATOM_H_
#define ATOM_H_

#include "AtomicFormula.h"

class _Atom : public _AtomicFormula {

    friend class _LogicFactory::FactoryInterningManager<Atom>;

protected:

    Predicate m_predicate;

    _Atom(_LogicFactory* const factory, const size_t hash, const Predicate& predicate, const std::vector<Term>& arguments);

    _Atom(_LogicFactory* const factory, const size_t hash, const Predicate& rdfPredicate, const Term& subject, const Term& predicate, const Term& object);

    static size_t hashCodeFor(const Predicate& predicate, const std::vector<Term>& arguments);

    bool isEqual(const Predicate& predicate, const std::vector<Term>& arguments) const;

    static size_t hashCodeFor(const Predicate& rdfPredicate, const Term& subject, const Term& predicate, const Term& object);

    bool isEqual(const Predicate& rdfPredicate, const Term& subject, const Term& predicate, const Term& object) const;

    virtual LogicObject doClone(const LogicFactory& logicFactory) const;

    virtual Formula doApply(const Substitution& substitution, const bool renameImplicitExistentialVariables, size_t& formulaWithImplicitExistentialVariablesCounter) const;

public:

    virtual ~_Atom();

    always_inline Atom clone(const LogicFactory& logicFactory) const {
        return static_pointer_cast<Atom>(doClone(logicFactory));
    }

    virtual const Predicate& getPredicate() const;

    virtual FormulaType getType() const;

    always_inline Atom applyEx(const Substitution& substitution, const bool renameImplicitExistentialVariables, size_t& formulaWithImplicitExistentialVariablesCounter) const {
        return static_pointer_cast<Atom>(doApply(substitution, renameImplicitExistentialVariables, formulaWithImplicitExistentialVariablesCounter));
    }

    always_inline Atom apply(const Substitution& substitution) const {
        size_t formulaWithImplicitExistentialVariablesCounter = 0;
        return applyEx(substitution, false, formulaWithImplicitExistentialVariablesCounter);
    }

    virtual void accept(LogicObjectVisitor& visitor) const;

    virtual std::string toString(const Prefixes& prefixes) const;

};

#endif /* ATOM_H_ */
