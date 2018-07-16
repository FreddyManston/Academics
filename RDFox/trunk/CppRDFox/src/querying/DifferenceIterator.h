// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef DIFFERENCEITERATOR_H_
#define DIFFERENCEITERATOR_H_

#include "../storage/TupleIterator.h"

class ThreadContext;
class ArgumentIndexSet;

template<bool callMonitor>
class DifferenceIterator : public TupleIterator {

protected:

    unique_ptr_vector<TupleIterator> m_childIterators;
    ThreadContext* m_threadContext;
    std::vector<std::pair<ArgumentIndex, ResourceID> > m_notSurelyBoundInputVariables;

    size_t ensureOnTuple(size_t multiplicity);

public:

    DifferenceIterator(TupleIteratorMonitor* const tupleIteratorMonitor, unique_ptr_vector<TupleIterator> childIterators);

    DifferenceIterator(const DifferenceIterator<callMonitor>& other, CloneReplacements& cloneReplacements);

    virtual const char* getName() const;

    virtual size_t getNumberOfChildIterators() const;

    virtual const TupleIterator& getChildIterator(const size_t childIteratorIndex) const;

    virtual std::unique_ptr<TupleIterator> clone(CloneReplacements& cloneReplacements) const;

    virtual size_t open(ThreadContext& threadContext);

    virtual size_t open();

    virtual size_t advance();

    virtual bool getCurrentTupleInfo(TupleIndex& tupleIndex, TupleStatus& tupleStatus) const;

    virtual TupleIndex getCurrentTupleIndex() const;

};


always_inline std::unique_ptr<TupleIterator> newDifferenceIterator(TupleIteratorMonitor* const tupleIteratorMonitor, unique_ptr_vector<TupleIterator> childIterators) {
    if (tupleIteratorMonitor)
        return std::unique_ptr<TupleIterator>(new DifferenceIterator<true>(tupleIteratorMonitor, std::move(childIterators)));
    else
        return std::unique_ptr<TupleIterator>(new DifferenceIterator<false>(tupleIteratorMonitor, std::move(childIterators)));
}

#endif /* DIFFERENCEITERATOR_H_ */
