// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef RESOURCEBYID_H_
#define RESOURCEBYID_H_

#include "../Common.h"
#include "GroundTerm.h"

class _ResourceByID : public _GroundTerm {

    friend class _LogicFactory::FactoryInterningManager<ResourceByID>;

protected:

    ResourceID m_resourceID;

    _ResourceByID(_LogicFactory* const factory, const size_t hash, const ResourceID resourceID);

    static size_t hashCodeFor(const ResourceID resourceID);

    bool isEqual(const ResourceID resourceID) const;

    virtual LogicObject doClone(const LogicFactory& logicFactory) const;

    virtual BuiltinExpression doApply(const Substitution& substitution, const bool renameImplicitExistentialVariables, size_t& formulaWithImplicitExistentialVariablesCounter) const;

public:

    virtual ~_ResourceByID();

    always_inline ResourceByID clone(const LogicFactory& logicFactory) const {
        return static_pointer_cast<ResourceByID>(doClone(logicFactory));
    }

    virtual ResourceID getResourceID() const;

    virtual BuiltinExpressionType getType() const;

    virtual void getFreeVariablesEx(std::unordered_set<Variable>& freeVariables) const;

    always_inline ResourceByID applyEx(const Substitution& substitution, const bool renameImplicitExistentialVariables, size_t& formulaWithImplicitExistentialVariablesCounter) const {
        return static_pointer_cast<ResourceByID>(doApply(substitution, renameImplicitExistentialVariables, formulaWithImplicitExistentialVariablesCounter));
    }

    always_inline ResourceByID apply(const Substitution& substitution) const {
        size_t formulaWithImplicitExistentialVariablesCounter = 0;
        return applyEx(substitution, false, formulaWithImplicitExistentialVariablesCounter);
    }

    virtual void accept(LogicObjectVisitor& visitor) const;

    virtual std::string toString(const Prefixes& prefixes) const;

};

#endif /* RESOURCEBYID_H_ */
