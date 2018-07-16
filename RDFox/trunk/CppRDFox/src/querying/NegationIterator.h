// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef NEGATIONITERATOR_H_
#define NEGATIONITERATOR_H_

#include "../storage/TupleIterator.h"

class ThreadContext;

template<bool callMonitor>
class NegationIterator : public TupleIterator {
    
protected:
    
    std::unique_ptr<TupleIterator> m_tupleIterator;
    
public:
    
    NegationIterator(TupleIteratorMonitor* const tupleIteratorMonitor, std::unique_ptr<TupleIterator> tupleIterator);
    
    NegationIterator(const NegationIterator& other, CloneReplacements& cloneReplacements);
    
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

always_inline std::unique_ptr<TupleIterator> newNegationIterator(TupleIteratorMonitor* const tupleIteratorMonitor, std::unique_ptr<TupleIterator> tupleIterator) {
    if (tupleIteratorMonitor)
        return std::unique_ptr<TupleIterator>(new NegationIterator<true>(tupleIteratorMonitor, std::move(tupleIterator)));
    else
        return std::unique_ptr<TupleIterator>(new NegationIterator<false>(tupleIteratorMonitor, std::move(tupleIterator)));
}

#endif /* NEGATIONITERATOR_H_ */
