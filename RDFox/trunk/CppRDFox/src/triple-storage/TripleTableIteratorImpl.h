// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef TRIPLETABLEITERATORIMPL_H_
#define TRIPLETABLEITERATORIMPL_H_

#include "../RDFStoreException.h"
#include "../util/ThreadContext.h"
#include "../storage/ArgumentIndexSet.h"
#include "../storage/TupleIteratorMonitor.h"
#include "TripleTableIterator.h"

// QPHandlerSelector

template<class TT, uint8_t queryType>
struct QPHandlerSelector;

template<class TT>
struct QPHandlerSelector<TT, 0> {
    typedef typename TT::QP0HandlerType Result;
};

template<class TT>
struct QPHandlerSelector<TT, 1> {
    typedef typename TT::QP1HandlerType Result;
};

template<class TT>
struct QPHandlerSelector<TT, 2> {
    typedef typename TT::QP2HandlerType Result;
};

template<class TT>
struct QPHandlerSelector<TT, 3> {
    typedef typename TT::QP3HandlerType Result;
};

template<class TT>
struct QPHandlerSelector<TT, 4> {
    typedef typename TT::QP4HandlerType Result;
};

template<class TT>
struct QPHandlerSelector<TT, 5> {
    typedef typename TT::QP5HandlerType Result;
};

template<class TT>
struct QPHandlerSelector<TT, 6> {
    typedef typename TT::QP6HandlerType Result;
};

template<class TT>
struct QPHandlerSelector<TT, 7> {
    typedef typename TT::QP7HandlerType Result;
};

template<uint8_t equalityCheckType>
always_inline static bool passesEqualityCheck(const ResourceID s, const ResourceID p, const ResourceID o) {
    switch (equalityCheckType) {
    case 0:
        return true;
    case 1:
        return s == p;
    case 2:
        return s == o;
    case 3:
        return p == o;
    case 4:
        return s == p && s == o;
    default:
        UNREACHABLE;
    }
}

// TripleTableIterator

template<class TT, class FT, uint8_t equalityCheckType, bool callMonitor>
template<uint8_t queryType>
always_inline bool BaseTripleTableIterator<TT, FT, equalityCheckType, callMonitor>::isWithinGroup(const ResourceID s, const ResourceID p, const ResourceID o) const {
    typedef typename QPHandlerSelector<TT, queryType>::Result QPHandler;
    switch (QPHandler::ITERATOR_TYPE) {
    case ITERATE_LIST:
    case ITERATE_NEXT:
    case SINGLETON:
        return true;
    case ITERATE_COMPARE:
        return ::getTripleComponent<QPHandler::ITERATE_COMPARE_WITH>(s, p, o) == m_compareResourceID;
    default:
        UNREACHABLE;
    }
}

template<class TT, class FT, uint8_t equalityCheckType, bool callMonitor>
template<uint8_t queryType>
always_inline TupleIndex BaseTripleTableIterator<TT, FT, equalityCheckType, callMonitor>::ensureOnTripleAndLoad(TupleIndex tupleIndex) {
    typedef typename QPHandlerSelector<TT, queryType>::Result QPHandler;
    const bool loadS = (queryType & 0x1) == 0;
    const bool loadP = (queryType & 0x2) == 0 && !(loadS && (equalityCheckType == 1 || equalityCheckType == 4));
    const bool loadO = (queryType & 0x4) == 0 && !(loadS && (equalityCheckType == 2 || equalityCheckType == 4)) && !(loadP && equalityCheckType == 3);
    while (tupleIndex != INVALID_TUPLE_INDEX) {
        ResourceID s;
        ResourceID p;
        ResourceID o;
        m_tripleTable.m_tripleList.getTripleStatusAndResourceIDs(tupleIndex, m_currentTupleStatus, s, p, o);
        if (isWithinGroup<queryType>(s, p, o)) {
            if (::passesEqualityCheck<equalityCheckType>(s, p, o) && m_filter.processTriple(tupleIndex, m_currentTupleStatus)) {
                if (loadS)
                    m_argumentsBuffer[m_argumentIndexes[0]] = s;
                if (loadP)
                    m_argumentsBuffer[m_argumentIndexes[1]] = p;
                if (loadO)
                    m_argumentsBuffer[m_argumentIndexes[2]] = o;
                return tupleIndex;
            }
            switch (QPHandler::ITERATOR_TYPE) {
            case ITERATE_LIST:
                tupleIndex = m_tripleTable.m_tripleList.getNextTripleIndex(tupleIndex);
                break;
            case ITERATE_NEXT:
            case ITERATE_COMPARE:
                tupleIndex = m_tripleTable.m_tripleList.getNext(tupleIndex, QPHandler::ITERATE_NEXT_ON);
                break;
            case SINGLETON:
                return INVALID_TUPLE_INDEX;
            default:
                UNREACHABLE;
            }
        }
        else {
            switch (QPHandler::ITERATOR_TYPE) {
            case ITERATE_LIST:
                tupleIndex = m_tripleTable.m_tripleList.getNextTripleIndex(tupleIndex);
                break;
            case ITERATE_NEXT:
                tupleIndex = m_tripleTable.m_tripleList.getNext(tupleIndex, QPHandler::ITERATE_NEXT_ON);
                break;
            case ITERATE_COMPARE:
                tupleIndex = m_compareGroupedMask & m_tripleTable.m_tripleList.getNext(tupleIndex, QPHandler::ITERATE_NEXT_ON);
                break;
            case SINGLETON:
                return INVALID_TUPLE_INDEX;
            default:
                UNREACHABLE;
            }
        }
    }
    return INVALID_TUPLE_INDEX;
}

template<class TT, class FT, uint8_t equalityCheckType, bool callMonitor>
template<uint8_t queryType>
always_inline size_t BaseTripleTableIterator<TT, FT, equalityCheckType, callMonitor>::doOpen(ThreadContext& threadContext) {
    TupleIndex tupleIndex = QPHandlerSelector<TT, queryType>::Result::getFirstTripleIndex(threadContext, m_tripleTable, m_argumentsBuffer[m_argumentIndexes[0]], m_argumentsBuffer[m_argumentIndexes[1]], m_argumentsBuffer[m_argumentIndexes[2]], m_compareResourceID, m_compareGroupedMask);
    tupleIndex = ensureOnTripleAndLoad<queryType>(tupleIndex);
    size_t multiplicity = (tupleIndex == INVALID_TUPLE_INDEX ? 0 : 1);
    m_currentTupleIndex = tupleIndex;
    if (callMonitor)
        m_tupleIteratorMonitor->iteratorOpened(*this, multiplicity);
    return multiplicity;
}

template<class TT, class FT, uint8_t equalityCheckType, bool callMonitor>
template<uint8_t queryType>
always_inline size_t BaseTripleTableIterator<TT, FT, equalityCheckType, callMonitor>::doAdvance() {
    TupleIndex tupleIndex = m_currentTupleIndex;
    typedef typename QPHandlerSelector<TT, queryType>::Result QPHandler;
    switch (QPHandler::ITERATOR_TYPE) {
    case ITERATE_LIST:
        tupleIndex = m_tripleTable.m_tripleList.getNextTripleIndex(tupleIndex);
        break;
    case ITERATE_NEXT:
    case ITERATE_COMPARE:
        tupleIndex = m_tripleTable.m_tripleList.getNext(tupleIndex, QPHandler::ITERATE_NEXT_ON);
        break;
    case SINGLETON:
        tupleIndex = 0;
        break;
    default:
        UNREACHABLE;
    }
    tupleIndex = ensureOnTripleAndLoad<queryType>(tupleIndex);
    size_t multiplicity = (tupleIndex == INVALID_TUPLE_INDEX ? 0 : 1);
    m_currentTupleIndex = tupleIndex;
    if (callMonitor)
        m_tupleIteratorMonitor->iteratorAdvanced(*this, multiplicity);
    return multiplicity;
}

template<class TT, class FT, uint8_t equalityCheckType, bool callMonitor>
BaseTripleTableIterator<TT, FT, equalityCheckType, callMonitor>::BaseTripleTableIterator(TupleIteratorMonitor* const tupleIteratorMonitor, std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const ArgumentIndexSet& allInputArguments, const ArgumentIndexSet& surelyBoundInputArguments, const TT& tripleTable, const FT& filter) :
    TupleIterator(tupleIteratorMonitor, argumentsBuffer, argumentIndexes, allInputArguments, surelyBoundInputArguments),
    m_tripleTable(tripleTable),
    m_filter(filter),
    m_currentTupleIndex(INVALID_TUPLE_INDEX),
    m_currentTupleStatus(TUPLE_STATUS_INVALID),
    m_compareGroupedMask(NOT_GROUPED_MASK),
    m_compareResourceID(INVALID_RESOURCE_ID)
{
    m_allArguments.add(m_argumentIndexes[0]);
    m_allArguments.add(m_argumentIndexes[1]);
    m_allArguments.add(m_argumentIndexes[2]);
    m_surelyBoundArguments.add(m_argumentIndexes[0]);
    m_surelyBoundArguments.add(m_argumentIndexes[1]);
    m_surelyBoundArguments.add(m_argumentIndexes[2]);
}

template<class TT, class FT, uint8_t equalityCheckType, bool callMonitor>
BaseTripleTableIterator<TT, FT, equalityCheckType, callMonitor>::BaseTripleTableIterator(const BaseTripleTableIterator<TT, FT, equalityCheckType, callMonitor>& other, CloneReplacements& cloneReplacements) :
    TupleIterator(other, cloneReplacements),
    m_tripleTable(other.m_tripleTable),
    m_filter(other.m_filter, cloneReplacements),
    m_currentTupleIndex(other.m_currentTupleIndex),
    m_compareGroupedMask(other.m_compareGroupedMask),
    m_compareResourceID(other.m_compareResourceID)
{
}

template<class TT, class FT, uint8_t equalityCheckType, bool callMonitor>
const char* BaseTripleTableIterator<TT, FT, equalityCheckType, callMonitor>::getName() const {
    return "TripleTableIterator";
}

template<class TT, class FT, uint8_t equalityCheckType, bool callMonitor>
size_t BaseTripleTableIterator<TT, FT, equalityCheckType, callMonitor>::getNumberOfChildIterators() const {
    return 0;
}

template<class TT, class FT, uint8_t equalityCheckType, bool callMonitor>
const TupleIterator& BaseTripleTableIterator<TT, FT, equalityCheckType, callMonitor>::getChildIterator(const size_t childIteratorIndex) const {
    throw RDF_STORE_EXCEPTION("Invalid child iterator index.");
}

template<class TT, class FT, uint8_t equalityCheckType, bool callMonitor>
bool BaseTripleTableIterator<TT, FT, equalityCheckType, callMonitor>::getCurrentTupleInfo(TupleIndex &tupleIndex, TupleStatus &tupleStatus) const {
    tupleIndex = m_currentTupleIndex;
    tupleStatus = m_currentTupleStatus;
    return true;
}

template<class TT, class FT, uint8_t equalityCheckType, bool callMonitor>
TupleIndex BaseTripleTableIterator<TT, FT, equalityCheckType, callMonitor>::getCurrentTupleIndex() const {
    return m_currentTupleIndex;
}

// FixedQueryTypeTripleTableIterator

template<class TT, class FT, uint8_t queryType, uint8_t equalityCheckType, bool callMonitor>
FixedQueryTypeTripleTableIterator<TT, FT, queryType, equalityCheckType, callMonitor>::FixedQueryTypeTripleTableIterator(TupleIteratorMonitor* const tupleIteratorMonitor, std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const ArgumentIndexSet& allInputArguments, const ArgumentIndexSet& surelyBoundInputArguments, const TT& tripleTable, const FT& filter) :
    BaseTripleTableIterator<TT, FT, equalityCheckType, callMonitor>(tupleIteratorMonitor, argumentsBuffer, argumentIndexes, allInputArguments, surelyBoundInputArguments, tripleTable, filter)
{
}

template<class TT, class FT, uint8_t queryType, uint8_t equalityCheckType, bool callMonitor>
FixedQueryTypeTripleTableIterator<TT, FT, queryType, equalityCheckType, callMonitor>::FixedQueryTypeTripleTableIterator(const FixedQueryTypeTripleTableIterator<TT, FT, queryType, equalityCheckType, callMonitor>& other, CloneReplacements& cloneReplacements) :
    BaseTripleTableIterator<TT, FT, equalityCheckType, callMonitor>(other, cloneReplacements)
{
}

template<class TT, class FT, uint8_t queryType, uint8_t equalityCheckType, bool callMonitor>
std::unique_ptr<TupleIterator> FixedQueryTypeTripleTableIterator<TT, FT, queryType, equalityCheckType, callMonitor>::clone(CloneReplacements& cloneReplacements) const {
    return std::unique_ptr<TupleIterator>(new FixedQueryTypeTripleTableIterator<TT, FT, queryType, equalityCheckType, callMonitor>(*this, cloneReplacements));
}

template<class TT, class FT, uint8_t queryType, uint8_t equalityCheckType, bool callMonitor>
size_t FixedQueryTypeTripleTableIterator<TT, FT, queryType, equalityCheckType, callMonitor>::open(ThreadContext& threadContext) {
    return this->template doOpen<queryType>(threadContext);
}

template<class TT, class FT, uint8_t queryType, uint8_t equalityCheckType, bool callMonitor>
size_t FixedQueryTypeTripleTableIterator<TT, FT, queryType, equalityCheckType, callMonitor>::open() {
    return open(ThreadContext::getCurrentThreadContext());
}

template<class TT, class FT, uint8_t queryType, uint8_t equalityCheckType, bool callMonitor>
size_t FixedQueryTypeTripleTableIterator<TT, FT, queryType, equalityCheckType, callMonitor>::advance() {
    return this->template doAdvance<queryType>();
}

// VariableQueryTypeTripleTableIterator

template<class TT, class FT, uint8_t equalityCheckType, bool callMonitor>
VariableQueryTypeTripleTableIterator<TT, FT, equalityCheckType, callMonitor>::VariableQueryTypeTripleTableIterator(TupleIteratorMonitor* const tupleIteratorMonitor, std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const ArgumentIndexSet& allInputArguments, const ArgumentIndexSet& surelyBoundInputArguments, const TT& tripleTable, const FT& filter) :
    BaseTripleTableIterator<TT, FT, equalityCheckType, callMonitor>(tupleIteratorMonitor, argumentsBuffer, argumentIndexes, allInputArguments, surelyBoundInputArguments, tripleTable, filter),
    m_surelyBoundQueryMask(::getConditionalMask(surelyBoundInputArguments.contains(this->m_argumentIndexes[0]), 1) | ::getConditionalMask(surelyBoundInputArguments.contains(this->m_argumentIndexes[1]), 2) | ::getConditionalMask(surelyBoundInputArguments.contains(this->m_argumentIndexes[2]), 4)),
    m_checkS(allInputArguments.contains(this->m_argumentIndexes[0]) && !surelyBoundInputArguments.contains(this->m_argumentIndexes[0])),
    m_checkP(allInputArguments.contains(this->m_argumentIndexes[1]) && !surelyBoundInputArguments.contains(this->m_argumentIndexes[1])),
    m_checkO(allInputArguments.contains(this->m_argumentIndexes[2]) && !surelyBoundInputArguments.contains(this->m_argumentIndexes[2])),
    m_queryType(0)
{
}

template<class TT, class FT, uint8_t equalityCheckType, bool callMonitor>
VariableQueryTypeTripleTableIterator<TT, FT, equalityCheckType, callMonitor>::VariableQueryTypeTripleTableIterator(const VariableQueryTypeTripleTableIterator<TT, FT, equalityCheckType, callMonitor>& other, CloneReplacements& cloneReplacements) :
    BaseTripleTableIterator<TT, FT, equalityCheckType, callMonitor>(other, cloneReplacements),
    m_surelyBoundQueryMask(other.m_surelyBoundQueryMask),
    m_checkS(other.m_checkS),
    m_checkP(other.m_checkP),
    m_checkO(other.m_checkO),
    m_queryType(other.m_queryType)
{
}

template<class TT, class FT, uint8_t equalityCheckType, bool callMonitor>
std::unique_ptr<TupleIterator> VariableQueryTypeTripleTableIterator<TT, FT, equalityCheckType, callMonitor>::clone(CloneReplacements& cloneReplacements) const {
    return std::unique_ptr<TupleIterator>(new VariableQueryTypeTripleTableIterator<TT, FT, equalityCheckType, callMonitor>(*this, cloneReplacements));
}

template<class TT, class FT, uint8_t equalityCheckType, bool callMonitor>
size_t VariableQueryTypeTripleTableIterator<TT, FT, equalityCheckType, callMonitor>::open(ThreadContext& threadContext) {
    m_queryType =
        m_surelyBoundQueryMask |
        (m_checkS && this->m_argumentsBuffer[this->m_argumentIndexes[0]] != INVALID_RESOURCE_ID ? 1 : 0) |
        (m_checkP && this->m_argumentsBuffer[this->m_argumentIndexes[1]] != INVALID_RESOURCE_ID ? 2 : 0) |
        (m_checkO && this->m_argumentsBuffer[this->m_argumentIndexes[2]] != INVALID_RESOURCE_ID ? 4 : 0);
    switch (m_queryType) {
    case 0:
        return this->template doOpen<0>(threadContext);
    case 1:
        return this->template doOpen<1>(threadContext);
    case 2:
        return this->template doOpen<2>(threadContext);
    case 3:
        return this->template doOpen<3>(threadContext);
    case 4:
        return this->template doOpen<4>(threadContext);
    case 5:
        return this->template doOpen<5>(threadContext);
    case 6:
        return this->template doOpen<6>(threadContext);
    case 7:
        return this->template doOpen<7>(threadContext);
    default:
        UNREACHABLE;
    }
}

template<class TT, class FT, uint8_t equalityCheckType, bool callMonitor>
size_t VariableQueryTypeTripleTableIterator<TT, FT, equalityCheckType, callMonitor>::open() {
    return open(ThreadContext::getCurrentThreadContext());
}

template<class TT, class FT, uint8_t equalityCheckType, bool callMonitor>
size_t VariableQueryTypeTripleTableIterator<TT, FT, equalityCheckType, callMonitor>::advance() {
    switch (m_queryType) {
    case 0:
        return this->template doAdvance<0>();
    case 1:
        return this->template doAdvance<1>();
    case 2:
        return this->template doAdvance<2>();
    case 3:
        return this->template doAdvance<3>();
    case 4:
        return this->template doAdvance<4>();
    case 5:
        return this->template doAdvance<5>();
    case 6:
        return this->template doAdvance<6>();
    case 7:
        return this->template doAdvance<7>();
    default:
        UNREACHABLE;
    }
}
#endif // TRIPLETABLEITERATORIMPL_H_
