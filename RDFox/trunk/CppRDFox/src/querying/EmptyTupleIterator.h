// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef EMPTYTUPLETERATOR_H_
#define EMPTYTUPLETERATOR_H_

#include "../storage/TupleIterator.h"

class DataStore;
class Dictionary;
class TupleTable;

// UniverseIterator

template<bool callMonitor>
class EmptyTupleIterator : public TupleIterator {

protected:

public:

    EmptyTupleIterator(TupleIteratorMonitor* const tupleIteratorMonitor, std::vector<ResourceID>& argumentsBuffer, const ArgumentIndexSet& allInputArguments, const ArgumentIndexSet& surelyBoundInputArguments);

    EmptyTupleIterator(const EmptyTupleIterator<callMonitor>& other, CloneReplacements& cloneReplacements);

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

always_inline std::unique_ptr<TupleIterator> newEmptyTupleIterator(TupleIteratorMonitor* const tupleIteratorMonitor, std::vector<ResourceID>& argumentsBuffer, const ArgumentIndexSet& allInputArguments, const ArgumentIndexSet& surelyBoundInputArguments) {
    if (tupleIteratorMonitor)
        return std::unique_ptr<TupleIterator>(new EmptyTupleIterator<true>(tupleIteratorMonitor, argumentsBuffer, allInputArguments, surelyBoundInputArguments));
    else
        return std::unique_ptr<TupleIterator>(new EmptyTupleIterator<false>(tupleIteratorMonitor, argumentsBuffer, allInputArguments, surelyBoundInputArguments));
}

#endif /* UNIVERSEITERATOR_H_ */
