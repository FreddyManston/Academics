// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef EXPLANATION_H_
#define EXPLANATION_H_

#include "../Common.h"
#include "../logic/Logic.h"

class ExplanationRuleInstanceNode;

// ExplanationAtomNode

class ExplanationAtomNode : public Unmovable {

public:

    enum AtomNodeType { EDB_ATOM, EQUAL_TO_EDB_ATOM, IDB_ATOM, FALSE_ATOM };

    virtual ~ExplanationAtomNode() {
    }

    virtual const std::vector<ResourceID>& getTupleBuffer() const = 0;

    virtual AtomNodeType getType() const = 0;

    virtual Atom getAtom() const = 0;

    virtual const std::vector<std::unique_ptr<ExplanationRuleInstanceNode> >& getChildren() const = 0;

    virtual size_t getHeight() const = 0;

    virtual ExplanationRuleInstanceNode* getShortestChild() const = 0;

};

// ExplanationRuleInstanceNode

class ExplanationRuleInstanceNode : public Unmovable {

public:

    virtual ~ExplanationRuleInstanceNode() {
    }

    virtual const ExplanationAtomNode& getParent() const = 0;

    virtual const Rule& getRule() const = 0;

    virtual size_t getHeadAtomIndex() const = 0;

    virtual size_t getSubstitutionSize() const = 0;

    virtual const Variable& getSubstitutionVariable(const size_t index) const = 0;

    virtual ResourceID getSubstitutionValue(const size_t index) const = 0;

    virtual Rule getRuleInstance() const = 0;

    virtual const std::vector<const ExplanationAtomNode*>& getChildren() const = 0;
    
};

// ExplanationProvider

class ExplanationProvider : public Unmovable {

public:

    virtual ~ExplanationProvider() {
    }

    virtual const ExplanationAtomNode& getNode(const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes) = 0;

    virtual const ExplanationAtomNode& getNode(const Atom& atom) = 0;

};

#endif /* EXPLANATION_H_ */
