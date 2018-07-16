// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef LOGICOBJECTWALKER_H_
#define LOGICOBJECTWALKER_H_

#include "../logic/LogicObjectVisitor.h"

class LogicObjectWalker : public LogicObjectVisitor {

public:

    virtual void visit(const Predicate& object);

    virtual void visit(const BuiltinFunctionCall& object);

    virtual void visit(const ExistenceExpression& object);

    virtual void visit(const Variable& object);

    virtual void visit(const ResourceByID& object);

    virtual void visit(const ResourceByName& object);

    virtual void visit(const Atom& object);

    virtual void visit(const Bind& object);

    virtual void visit(const Filter& object);

    virtual void visit(const Negation& object);

    virtual void visit(const AggregateBind& object);

    virtual void visit(const Aggregate& object);

    virtual void visit(const Conjunction& object);

    virtual void visit(const Disjunction& object);

    virtual void visit(const Optional& object);

    virtual void visit(const Minus& object);

    virtual void visit(const Values& object);

    virtual void visit(const Query& object);

    virtual void visit(const Rule& object);

};

#endif /* LOGICOBJECTWALKER_H_ */
