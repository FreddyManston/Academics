// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef LOGICOBJECTVISITOR_H_
#define LOGICOBJECTVISITOR_H_

#include "Logic.h"

class LogicObjectVisitor {

public:

    virtual ~LogicObjectVisitor() {
    }

    virtual void visit(const Predicate& object) = 0;

    virtual void visit(const BuiltinFunctionCall& object) = 0;

    virtual void visit(const ExistenceExpression& object) = 0;

    virtual void visit(const Variable& object) = 0;

    virtual void visit(const ResourceByID& object) = 0;

    virtual void visit(const ResourceByName& object) = 0;

    virtual void visit(const Atom& object) = 0;

    virtual void visit(const Bind& object) = 0;

    virtual void visit(const Filter& object) = 0;

    virtual void visit(const Negation& object) = 0;

    virtual void visit(const AggregateBind& object) = 0;

    virtual void visit(const Aggregate& object) = 0;

    virtual void visit(const Conjunction& object) = 0;

    virtual void visit(const Disjunction& object) = 0;

    virtual void visit(const Optional& object) = 0;

    virtual void visit(const Minus& object) = 0;

    virtual void visit(const Values& object) = 0;

    virtual void visit(const Query& object) = 0;

    virtual void visit(const Rule& object) = 0;

};

#endif /* LOGICOBJECTVISITOR_H_ */
