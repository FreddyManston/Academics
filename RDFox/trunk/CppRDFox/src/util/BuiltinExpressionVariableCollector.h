// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef BUILTINEXPRESSIONVARIABLECOLLECTOR_H_
#define BUILTINEXPRESSIONVARIABLECOLLECTOR_H_

#include "LogicObjectWalker.h"

class BuiltinExpressionVariableCollector : public LogicObjectWalker {

protected:

    std::vector<Term>& m_termsArray;
    std::unordered_set<Term> m_termsSet;

public:

    BuiltinExpressionVariableCollector(std::vector<Term>& termsArray);

    using LogicObjectWalker::visit;

    virtual void visit(const Variable& object);

    virtual void visit(const Query& object);
    
    virtual void visit(const Rule& object);

};

#endif /* BUILTINEXPRESSIONVARIABLECOLLECTOR_H_ */
