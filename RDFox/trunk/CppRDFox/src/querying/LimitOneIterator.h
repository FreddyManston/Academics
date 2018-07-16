// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef LIMITONEITERATOR_H_
#define LIMITONEITERATOR_H_

#include "../storage/TupleIterator.h"

template<bool callMonitor>
class LimitOneIterator : public TupleIterator {

protected:

    const std::unique_ptr<TupleIterator> m_tupleIterator;

public:

    LimitOneIterator(TupleIteratorMonitor* const tupleIteratorMonitor, std::unique_ptr<TupleIterator> tupleIterator);

    LimitOneIterator(const LimitOneIterator<callMonitor>& other, CloneReplacements& cloneReplacements);

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

always_inline std::unique_ptr<TupleIterator> newLimitOneIterator(TupleIteratorMonitor* const tupleIteratorMonitor, std::unique_ptr<TupleIterator> tupleIterator) {
    if (tupleIteratorMonitor)
        return std::unique_ptr<TupleIterator>(new LimitOneIterator<true>(tupleIteratorMonitor, std::move(tupleIterator)));
    else
        return std::unique_ptr<TupleIterator>(new LimitOneIterator<false>(tupleIteratorMonitor, std::move(tupleIterator)));
}

#endif /* LIMITONEITERATOR_H_ */
