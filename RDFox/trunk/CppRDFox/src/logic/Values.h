// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef VALUES_H_
#define VALUES_H_

#include "Formula.h"

class _Values : public _Formula {

    friend class _LogicFactory::FactoryInterningManager<Values>;

protected:

    std::vector<Variable> m_variables;
    std::vector<std::vector<GroundTerm> > m_data;

    _Values(_LogicFactory* const factory, const size_t hash, const std::vector<Variable>& variables, const std::vector<std::vector<GroundTerm> >& data);

    static size_t hashCodeFor(const std::vector<Variable>& variables, const std::vector<std::vector<GroundTerm> >& data);

    bool isEqual(const std::vector<Variable>& variables, const std::vector<std::vector<GroundTerm> >& data) const;

    virtual LogicObject doClone(const LogicFactory& logicFactory) const;

    virtual Formula doApply(const Substitution& substitution, const bool renameImplicitExistentialVariables, size_t& formulaWithImplicitExistentialVariablesCounter) const;

public:

    virtual ~_Values();

    always_inline Values clone(const LogicFactory& logicFactory) const {
        return static_pointer_cast<Values>(doClone(logicFactory));
    }

    virtual const std::vector<Variable>& getVariables() const;

    virtual size_t getNumberOfVariables() const;

    virtual const Variable& getVariable(const size_t index) const;

    virtual const std::vector<std::vector<GroundTerm> >& getData() const;

    virtual size_t getNumberOfDataTuples() const;

    virtual const std::vector<GroundTerm>& getDataTuple(const size_t tupleIndex) const;

    virtual const GroundTerm& getDataTupleElement(const size_t tupleIndex, const size_t elementIndex) const;

    virtual FormulaType getType() const;

    virtual bool isGround() const;

    virtual void getFreeVariablesEx(std::unordered_set<Variable>& freeVariables) const;

    always_inline Values applyEx(const Substitution& substitution, const bool renameImplicitExistentialVariables, size_t& formulaWithImplicitExistentialVariablesCounter) const {
        return static_pointer_cast<Values>(doApply(substitution, renameImplicitExistentialVariables, formulaWithImplicitExistentialVariablesCounter));
    }

    always_inline Values apply(const Substitution& substitution) const {
        size_t formulaWithImplicitExistentialVariablesCounter = 0;
        return applyEx(substitution, false, formulaWithImplicitExistentialVariablesCounter);
    }

    virtual void accept(LogicObjectVisitor& visitor) const;

    virtual std::string toString(const Prefixes& prefixes) const;

};

#endif /* VALUES_H_ */
