// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef ITERATORPRINTING_H_
#define ITERATORPRINTING_H_

#include "../Common.h"

class Prefixes;
class TermArray;
class Dictionary;
class ArgumentIndexSet;
class TupleIterator;

class IteratorPrinting {

protected:

    const Prefixes& m_prefixes;
    const TermArray& m_termArray;
    const Dictionary& m_dictionary;

public:

    IteratorPrinting(const Prefixes& prefixes, const TermArray& termArray, const Dictionary& dictionary);

    always_inline const Prefixes& getPrefixes() const {
        return m_prefixes;
    }

    always_inline const TermArray& getTermArray() const {
        return m_termArray;
    }

    const Dictionary& getDictionary() const {
        return m_dictionary;
    }

    void printIterator(const TupleIterator& tupleIterator, std::ostream& output) const;

    void printIteratorSubtree(const TupleIterator& tupleIterator, std::ostream& output) const;

    void printIteratorSubtree(const TupleIterator& tupleIterator, std::ostream& output, const size_t level) const;

};

#endif /* ITERATORPRINTING_H_ */
