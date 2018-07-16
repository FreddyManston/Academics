// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef EQUALITYEXPANSIONITERATOR_H_
#define EQUALITYEXPANSIONITERATOR_H_

#include "../storage/TupleIterator.h"

class DataStore;
class EqualityManager;

template<bool callMonitor>
class EqualityExpansionIterator : public TupleIterator {

protected:

    const EqualityManager& m_equalityManager;
    std::vector<ArgumentIndex> m_argumentIndexesToExpand;
    const std::unique_ptr<TupleIterator> m_tupleIterator;
    size_t m_currentMultiplicity;

public:

    EqualityExpansionIterator(TupleIteratorMonitor* const tupleIteratorMonitor, const DataStore& dataStore, std::unique_ptr<TupleIterator> tupleIterator);

    EqualityExpansionIterator(const EqualityExpansionIterator<callMonitor>& other, CloneReplacements& cloneReplacements);

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

always_inline std::unique_ptr<TupleIterator> newEqualityExpansionIterator(TupleIteratorMonitor* const tupleIteratorMonitor, const DataStore& dataStore, std::unique_ptr<TupleIterator> tupleIterator) {
    if (tupleIteratorMonitor)
        return std::unique_ptr<TupleIterator>(new EqualityExpansionIterator<true>(tupleIteratorMonitor, dataStore, std::move(tupleIterator)));
    else
        return std::unique_ptr<TupleIterator>(new EqualityExpansionIterator<false>(tupleIteratorMonitor, dataStore, std::move(tupleIterator)));
}

#endif /* EQUALITYEXPANSIONITERATOR_H_ */
