// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef TERMARRAY_H_
#define TERMARRAY_H_

#include "../Common.h"
#include "../logic/Logic.h"
#include "../storage/ArgumentIndexSet.h"

class Dictionary;

class TermArray : private Unmovable {

protected:

    std::unordered_map<Term, ArgumentIndex> m_termToPosition;
    std::vector<Term> m_termsByPosition;

public:

    TermArray();

    TermArray(const Formula& formula);

    TermArray(const std::vector<Formula>& formulas);

    always_inline void clear() {
        m_termToPosition.clear();
        m_termsByPosition.clear();
    }

    always_inline size_t getNumberOfTerms() const {
        return m_termsByPosition.size();
    }

    always_inline const Term& getTerm(const ArgumentIndex argumentIndex) const {
        return m_termsByPosition[argumentIndex];
    }

    always_inline ArgumentIndex getPosition(const Term& term) const {
        std::unordered_map<Term, ArgumentIndex>::const_iterator iterator = m_termToPosition.find(term);
        assert(iterator != m_termToPosition.end());
        return iterator->second;
    }

    void visitTerm(const Term& term);

    void visitFormula(const Formula& formula);

};

#endif /* TERMARRAY_H_ */
