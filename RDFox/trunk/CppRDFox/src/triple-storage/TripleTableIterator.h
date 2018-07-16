// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef TRIPLETABLEITERATOR_H_
#define TRIPLETABLEITERATOR_H_

#include "../storage/TupleTable.h"
#include "../storage/TupleIterator.h"

class ArgumentIndexSet;
class TupleIteratorMonitor;

enum TripleTableIteratorType {
    ITERATE_LIST,
    ITERATE_NEXT,
    ITERATE_COMPARE,
    SINGLETON
};

// BaseTripleTableIterator

template<class TT, class FT, uint8_t equalityCheckType, bool callMonitor>
class BaseTripleTableIterator : public TupleIterator {

protected:

    const TT& m_tripleTable;
    const FT m_filter;
    TupleIndex m_currentTupleIndex;
    TupleStatus m_currentTupleStatus;
    TupleIndex m_compareGroupedMask;
    ResourceID m_compareResourceID;

    template<uint8_t queryType>
    bool isWithinGroup(const ResourceID s, const ResourceID p, const ResourceID o) const;

    template<uint8_t queryType>
    TupleIndex ensureOnTripleAndLoad(TupleIndex tupleIndex);

    template<uint8_t queryType>
    size_t doOpen(ThreadContext& threadContext);

    template<uint8_t queryType>
    size_t doAdvance();

public:

    BaseTripleTableIterator(TupleIteratorMonitor* const tupleIteratorMonitor, std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const ArgumentIndexSet& allInputArguments, const ArgumentIndexSet& surelyBoundInputArguments, const TT& tripleTable, const FT& filter);

    BaseTripleTableIterator(const BaseTripleTableIterator<TT, FT, equalityCheckType, callMonitor>& other, CloneReplacements& cloneReplacements);

    virtual const char* getName() const;

    virtual size_t getNumberOfChildIterators() const;

    virtual const TupleIterator& getChildIterator(const size_t childIteratorIndex) const;

    virtual bool getCurrentTupleInfo(TupleIndex& tupleIndex, TupleStatus& tupleStatus) const;

    virtual TupleIndex getCurrentTupleIndex() const;

};

// FixedQueryTypeTripleTableIterator

template<class TT, class FT, uint8_t queryType, uint8_t equalityCheckType, bool callMonitor>
class FixedQueryTypeTripleTableIterator : public BaseTripleTableIterator<TT, FT, equalityCheckType, callMonitor> {

public:

    FixedQueryTypeTripleTableIterator(TupleIteratorMonitor* const tupleIteratorMonitor, std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const ArgumentIndexSet& allInputArguments, const ArgumentIndexSet& surelyBoundInputArguments, const TT& tripleTable, const FT& filter);

    FixedQueryTypeTripleTableIterator(const FixedQueryTypeTripleTableIterator<TT, FT, queryType, equalityCheckType, callMonitor>& other, CloneReplacements& cloneReplacements);

    virtual std::unique_ptr<TupleIterator> clone(CloneReplacements& cloneReplacements) const;

    virtual size_t open(ThreadContext& threadContext);

    virtual size_t open();

    virtual size_t advance();

};

// VariableQueryTypeTripleTableIterator

template<class TT, class FT, uint8_t equalityCheckType, bool callMonitor>
class VariableQueryTypeTripleTableIterator : public BaseTripleTableIterator<TT, FT, equalityCheckType, callMonitor> {

protected:

    const uint8_t m_surelyBoundQueryMask;
    const bool m_checkS;
    const bool m_checkP;
    const bool m_checkO;
    uint8_t m_queryType;

public:

    VariableQueryTypeTripleTableIterator(TupleIteratorMonitor* const tupleIteratorMonitor, std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const ArgumentIndexSet& allInputArguments, const ArgumentIndexSet& surelyBoundInputArguments, const TT& tripleTable, const FT& filter);

    VariableQueryTypeTripleTableIterator(const VariableQueryTypeTripleTableIterator<TT, FT, equalityCheckType, callMonitor>& other, CloneReplacements& cloneReplacements);

    virtual std::unique_ptr<TupleIterator> clone(CloneReplacements& cloneReplacements) const;

    virtual size_t open(ThreadContext& threadContext);

    virtual size_t open();

    virtual size_t advance();

};

#endif // TRIPLETABLEITERATOR_H_
