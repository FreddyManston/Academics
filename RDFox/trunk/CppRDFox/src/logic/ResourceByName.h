// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef RESOURCEBYNAME_H_
#define RESOURCEBYNAME_H_

#include "../Common.h"
#include "GroundTerm.h"

class _ResourceByName : public _GroundTerm {

    friend class _LogicFactory::FactoryInterningManager<ResourceByName>;

protected:

    ResourceText m_resourceText;

    _ResourceByName(_LogicFactory* const factory, const size_t hash, const ResourceType resourceType, const char* const lexicalForm, const char* const datatypeIRI);

    static size_t hashCodeFor(const ResourceType resourceType, const char* lexicalForm, const char* datatypeIRI);

    bool isEqual(const ResourceType resourceType, const char* const lexicalForm, const char* const datatypeIRI) const;

    virtual LogicObject doClone(const LogicFactory& logicFactory) const;

    virtual BuiltinExpression doApply(const Substitution& substitution, const bool renameImplicitExistentialVariables, size_t& formulaWithImplicitExistentialVariablesCounter) const;

public:

    virtual ~_ResourceByName();

    always_inline ResourceByName clone(const LogicFactory& logicFactory) const {
        return static_pointer_cast<ResourceByName>(doClone(logicFactory));
    }

    virtual const ResourceText& getResourceText() const;

    virtual BuiltinExpressionType getType() const;

    virtual bool isRDFType() const;

    virtual void getFreeVariablesEx(std::unordered_set<Variable>& freeVariables) const;

    always_inline ResourceByName applyEx(const Substitution& substitution, const bool renameImplicitExistentialVariables, size_t& formulaWithImplicitExistentialVariablesCounter) const {
        return static_pointer_cast<ResourceByName>(doApply(substitution, renameImplicitExistentialVariables, formulaWithImplicitExistentialVariablesCounter));
    }

    always_inline ResourceByName apply(const Substitution& substitution) const {
        size_t formulaWithImplicitExistentialVariablesCounter = 0;
        return applyEx(substitution, false, formulaWithImplicitExistentialVariablesCounter);
    }

    virtual void accept(LogicObjectVisitor& visitor) const;

    virtual std::string toString(const Prefixes& prefixes) const;

};

#endif /* RESOURCEBYNAME_H_ */
