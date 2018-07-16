// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "../util/LogicObjectWalker.h"
#include "TermArray.h"

class TermToPositionResolver : public LogicObjectWalker {

protected:

    std::unordered_map<Term, ArgumentIndex>& m_termToPosition;
    std::vector<Term>& m_termsByPosition;

public:

    TermToPositionResolver(std::unordered_map<Term, ArgumentIndex>& termToPosition, std::vector<Term>& termsByPosition) : m_termToPosition(termToPosition), m_termsByPosition(termsByPosition) {
    }

    using LogicObjectWalker::visit;

    virtual void visit(const Variable& object) {
        if (m_termToPosition.insert(std::pair<Term, ArgumentIndex>(object, static_cast<ArgumentIndex>(m_termsByPosition.size()))).second)
            m_termsByPosition.push_back(object);
    }

    virtual void visit(const ResourceByID& object) {
        if (m_termToPosition.insert(std::pair<Term, ArgumentIndex>(object, static_cast<ArgumentIndex>(m_termsByPosition.size()))).second)
            m_termsByPosition.push_back(object);
    }

    virtual void visit(const ResourceByName& object) {
        if (m_termToPosition.insert(std::pair<Term, ArgumentIndex>(object, static_cast<ArgumentIndex>(m_termsByPosition.size()))).second)
            m_termsByPosition.push_back(object);
    }

};

TermArray::TermArray() : m_termToPosition(), m_termsByPosition() {
}

TermArray::TermArray(const Formula& formula) : TermArray() {
    visitFormula(formula);
}

TermArray::TermArray(const std::vector<Formula>& formulas) : TermArray() {
    for (std::vector<Formula>::const_iterator iterator = formulas.begin(); iterator != formulas.end(); ++iterator)
        visitFormula(*iterator);
}

void TermArray::visitTerm(const Term& term) {
    TermToPositionResolver termToPositionResolver(m_termToPosition, m_termsByPosition);
    term->accept(termToPositionResolver);
}

void TermArray::visitFormula(const Formula& formula) {
    TermToPositionResolver termToPositionResolver(m_termToPosition, m_termsByPosition);
    formula->accept(termToPositionResolver);
}
