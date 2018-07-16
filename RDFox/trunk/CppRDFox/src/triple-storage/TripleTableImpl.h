// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef TRIPLETABLEIMPL_H_
#define TRIPLETABLEIMPL_H_

#include "../Common.h"
#include "../RDFStoreException.h"
#include "../util/ComponentStatistics.h"
#include "../util/InputStream.h"
#include "../util/OutputStream.h"
#include "../util/Thread.h"
#include "../util/ThreadContext.h"
#include "../storage/TupleFilter.h"
#include "TripleTable.h"
#include "TripleTableIteratorImpl.h"
#include "TripleTableProxyImpl.h"

// FilterByTupleStatus

class FilterByTupleStatus {

protected:

    const TupleStatus m_tupleStatusMask;
    const TupleStatus m_tupleStatusExpectedValue;

public:

    FilterByTupleStatus(const TupleStatus tupleStatusMask, const TupleStatus tupleStatusExpectedValue) : m_tupleStatusMask(tupleStatusMask), m_tupleStatusExpectedValue(tupleStatusExpectedValue) {
    }

    FilterByTupleStatus(const FilterByTupleStatus& other, CloneReplacements& cloneReplacements) : m_tupleStatusMask(other.m_tupleStatusMask), m_tupleStatusExpectedValue(other.m_tupleStatusExpectedValue) {
    }

    bool processTriple(const TupleIndex tupleIndex, const TupleStatus tupleStatus) const {
        return (tupleStatus & m_tupleStatusMask) == m_tupleStatusExpectedValue;
    }

};

// FilterByTupleFilter

class FilterByTupleFilter {

protected:

    const TupleFilter* & m_tupleFilter;
    const void* const m_tupleFilterContext;

public:

    FilterByTupleFilter(const TupleFilter* & tupleFilter, const void* const tupleFilterContext) : m_tupleFilter(tupleFilter), m_tupleFilterContext(tupleFilterContext) {
    }

    FilterByTupleFilter(const FilterByTupleFilter& other, CloneReplacements& cloneReplacements) : m_tupleFilter(*cloneReplacements.getReplacement(&other.m_tupleFilter)), m_tupleFilterContext(cloneReplacements.getReplacement(other.m_tupleFilterContext)) {
    }

    bool processTriple(const TupleIndex tupleIndex, const TupleStatus tupleStatus) const {
        return (tupleStatus & TUPLE_STATUS_COMPLETE) == TUPLE_STATUS_COMPLETE && m_tupleFilter->processTuple(m_tupleFilterContext, tupleIndex, tupleStatus);
    }
    
};

// TripleTable

template<class TripleTableConfiguration>
TripleTable<TripleTableConfiguration>::TripleTable(MemoryManager& memoryManager, const Parameters& dataStoreParameters) :
    m_dataStoreParameters(dataStoreParameters),
    m_tripleList(memoryManager),
    m_twoKeysManager1(memoryManager, m_tripleList, dataStoreParameters),
    m_twoKeysManager2(memoryManager, m_tripleList, dataStoreParameters),
    m_twoKeysManager3(memoryManager, m_tripleList, dataStoreParameters),
    m_threeKeysManager(memoryManager, m_tripleList, dataStoreParameters),
    m_triplesScheduledForAddition(memoryManager),
    m_triplesScheduledForDeletion(memoryManager)
{
}

template<class TripleTableConfiguration>
const char* TripleTable<TripleTableConfiguration>::getTypeName() {
    return TripleTableTraits<TripleTableType>::TYPE_NAME;
}

template<class TripleTableConfiguration>
std::string TripleTable<TripleTableConfiguration>::getPredicateName() const {
    return std::string("internal$rdf");
}

template<class TripleTableConfiguration>
void TripleTable<TripleTableConfiguration>::setNumberOfThreads(const size_t numberOfThreads) {
    m_twoKeysManager1.setNumberOfThreads(numberOfThreads);
    m_twoKeysManager2.setNumberOfThreads(numberOfThreads);
    m_twoKeysManager3.setNumberOfThreads(numberOfThreads);
    m_threeKeysManager.setNumberOfThreads(numberOfThreads);
}

template<class TripleTableConfiguration>
size_t TripleTable<TripleTableConfiguration>::getArity() const {
    return 3;
}

template<class TripleTableConfiguration>
size_t TripleTable<TripleTableConfiguration>::getTupleCount(const TupleStatus tupleStatusMask, const TupleStatus tupleStatusExpectedValue) const {
    return m_tripleList.getExactTripleCount(tupleStatusMask, tupleStatusExpectedValue);
}

template<class TripleTableConfiguration>
bool TripleTable<TripleTableConfiguration>::supportsProxy() const {
    return m_tripleList.supportsWindowedAdds();
}

template<class TripleTableConfiguration>
std::unique_ptr<TupleTableProxy> TripleTable<TripleTableConfiguration>::createTupleTableProxy(const size_t windowSize) {
    return std::unique_ptr<TupleTableProxy>(new TripleTableProxyType(*this, m_dataStoreParameters, windowSize));
}

template<class TripleTableConfiguration>
always_inline std::pair<bool, TupleIndex> TripleTable<TripleTableConfiguration>::addTuple(ThreadContext& threadContext, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const TupleStatus deleteTupleStatus, const TupleStatus addTupleStatus) {
    const ResourceID s = argumentsBuffer[argumentIndexes[0]];
    const ResourceID p = argumentsBuffer[argumentIndexes[1]];
    const ResourceID o = argumentsBuffer[argumentIndexes[2]];
    if (s == INVALID_RESOURCE_ID || p == INVALID_RESOURCE_ID || o == INVALID_RESOURCE_ID)
        throw RDF_STORE_EXCEPTION("A triple contained undefined values cannot be added to the store.");
    typename ThreeKeysManagerType::InsertToken threeKeysInsertToken;
    bool alreadyExists;
    if (!m_threeKeysManager.getInsertToken(threadContext, s, p, o, threeKeysInsertToken, alreadyExists)) {
        m_threeKeysManager.releaseInsertToken(threadContext, threeKeysInsertToken);
        throw RDF_STORE_EXCEPTION("Memory exhausted.");
    }
    TupleIndex tupleIndex;
    if (alreadyExists)
        tupleIndex = threeKeysInsertToken.m_bucketContents.m_tripleIndex;
    else {
        tupleIndex = m_tripleList.add(s, p, o);
        if (tupleIndex == INVALID_TUPLE_INDEX) {
            m_threeKeysManager.abortInsertToken(threadContext, threeKeysInsertToken);
            m_threeKeysManager.releaseInsertToken(threadContext, threeKeysInsertToken);
            throw RDF_STORE_EXCEPTION("Memory exhausted.");
        }
        else
            m_threeKeysManager.updateOnInsert(threadContext, threeKeysInsertToken, tupleIndex, s, p, o);
    }
    m_threeKeysManager.releaseInsertToken(threadContext, threeKeysInsertToken);
    TupleStatus completeStatus;
    if (alreadyExists)
        completeStatus = 0;
    else {
        if (!m_twoKeysManager1.insertTriple(threadContext, tupleIndex, s, p, o) || !m_twoKeysManager2.insertTriple(threadContext, tupleIndex, s, p, o) || !m_twoKeysManager3.insertTriple(threadContext, tupleIndex, s, p, o))
            throw RDF_STORE_EXCEPTION("Memory exhausted.");
        completeStatus = TUPLE_STATUS_COMPLETE;
    }
    const TupleStatus tupleStatusMask = deleteTupleStatus | addTupleStatus;
    TupleStatus existingTripleStatus;
    TupleStatus newTripleStatus;
    do {
        existingTripleStatus = m_tripleList.getTripleStatus(tupleIndex);
        newTripleStatus = (existingTripleStatus & ~tupleStatusMask) | addTupleStatus | completeStatus;
    } while (existingTripleStatus != newTripleStatus && !m_tripleList.setTripleStatusConditional(tupleIndex, existingTripleStatus, newTripleStatus));
    return std::make_pair((existingTripleStatus & tupleStatusMask) != addTupleStatus, static_cast<TupleIndex>(tupleIndex));
}

template<class TripleTableConfiguration>
std::pair<bool, TupleIndex> TripleTable<TripleTableConfiguration>::addTuple(const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const TupleStatus deleteTupleStatus, const TupleStatus addTupleStatus) {
    return TripleTable<TripleTableConfiguration>::addTuple(ThreadContext::getCurrentThreadContext(), argumentsBuffer, argumentIndexes, deleteTupleStatus, addTupleStatus);
}

template<class TripleTableConfiguration>
bool TripleTable<TripleTableConfiguration>::addTupleStatus(const TupleIndex tupleIndex, const TupleStatus tupleStatus) {
    TupleStatus existingTupleStatus;
    TupleStatus newTupleStatus;
    do {
        existingTupleStatus = m_tripleList.getTripleStatus(tupleIndex);
        newTupleStatus = existingTupleStatus | tupleStatus;
        if (existingTupleStatus == newTupleStatus)
            return false;
    } while (!m_tripleList.setTripleStatusConditional(tupleIndex, existingTupleStatus, newTupleStatus));
    return true;
}

template<class TripleTableConfiguration>
bool TripleTable<TripleTableConfiguration>::deleteTupleStatus(const TupleIndex tupleIndex, const TupleStatus tupleStatus) {
    TupleStatus existingTupleStatus;
    TupleStatus newTupleStatus;
    do {
        existingTupleStatus = m_tripleList.getTripleStatus(tupleIndex);
        newTupleStatus = existingTupleStatus & ~tupleStatus;
        if (existingTupleStatus == newTupleStatus)
            return false;
    } while (!m_tripleList.setTripleStatusConditional(tupleIndex, existingTupleStatus, newTupleStatus));
    return true;
}

template<class TripleTableConfiguration>
bool TripleTable<TripleTableConfiguration>::deleteAddTupleStatus(const TupleIndex tupleIndex, const TupleStatus deleteMask, const TupleStatus deleteExpected, const TupleStatus deleteTupleStatus, const TupleStatus addMask, const TupleStatus addExpected, const TupleStatus addTupleStatus) {
    TupleStatus existingTupleStatus;
    TupleStatus newTupleStatus;
    do {
        existingTupleStatus = m_tripleList.getTripleStatus(tupleIndex);
        newTupleStatus = existingTupleStatus;
        if ((newTupleStatus & deleteMask) == deleteExpected)
            newTupleStatus &= ~deleteTupleStatus;
        if ((newTupleStatus & addMask) == addExpected)
            newTupleStatus |= addTupleStatus;
        if (existingTupleStatus == newTupleStatus)
            return false;
    } while (!m_tripleList.setTripleStatusConditional(tupleIndex, existingTupleStatus, newTupleStatus));
    return true;
}

template<class TripleTableConfiguration>
TupleIndex TripleTable<TripleTableConfiguration>::getTupleIndex(ThreadContext& threadContext, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes) const {
    return m_threeKeysManager.getTripleIndex(threadContext, argumentsBuffer[argumentIndexes[0]], argumentsBuffer[argumentIndexes[1]], argumentsBuffer[argumentIndexes[2]]);
}

template<class TripleTableConfiguration>
TupleIndex TripleTable<TripleTableConfiguration>::getTupleIndex(const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes) const {
    return TripleTable<TripleTableConfiguration>::getTupleIndex(ThreadContext::getCurrentThreadContext(), argumentsBuffer, argumentIndexes);
}

template<class TripleTableConfiguration>
always_inline bool TripleTable<TripleTableConfiguration>::containsTuple(ThreadContext& threadContext, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const TupleStatus tupleStatusMask, const TupleStatus tupleStatusExpectedValue) const {
    return m_threeKeysManager.contains(threadContext, argumentsBuffer[argumentIndexes[0]], argumentsBuffer[argumentIndexes[1]], argumentsBuffer[argumentIndexes[2]], tupleStatusMask, tupleStatusExpectedValue);
}

template<class TripleTableConfiguration>
bool TripleTable<TripleTableConfiguration>::containsTuple(const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const TupleStatus tupleStatusMask, const TupleStatus tupleStatusExpectedValue) const {
    return TripleTable<TripleTableConfiguration>::containsTuple(ThreadContext::getCurrentThreadContext(), argumentsBuffer, argumentIndexes, tupleStatusMask, tupleStatusExpectedValue);
}

template<class TripleTableConfiguration>
TupleIndex TripleTable<TripleTableConfiguration>::getFirstFreeTupleIndex() const {
    return m_tripleList.getFirstFreeTripleIndex();
}

template<class TripleTableConfiguration>
TupleStatus TripleTable<TripleTableConfiguration>::getTupleStatus(const TupleIndex tupleIndex) const {
    return m_tripleList.getTripleStatus(tupleIndex);
}

template<class TripleTableConfiguration>
TupleStatus TripleTable<TripleTableConfiguration>::getStatusAndTuple(const TupleIndex tupleIndex, std::vector<ResourceID>& tupleBuffer) const {
    TupleStatus tupleStatus;
    do {
        tupleStatus = m_tripleList.getTripleStatus(tupleIndex);
    } while ((tupleStatus & TUPLE_STATUS_COMPLETE) != TUPLE_STATUS_COMPLETE);
    m_tripleList.getResourceIDs(tupleIndex, tupleBuffer[0], tupleBuffer[1], tupleBuffer[2]);
    return tupleStatus;
}

template<class TripleTableConfiguration>
TupleStatus TripleTable<TripleTableConfiguration>::getStatusAndTupleIfComplete(const TupleIndex tupleIndex, std::vector<ResourceID>& tupleBuffer) const {
    const TupleStatus tupleStatus = m_tripleList.getTripleStatus(tupleIndex);
    if ((tupleStatus & TUPLE_STATUS_COMPLETE) == TUPLE_STATUS_COMPLETE)
        m_tripleList.getResourceIDs(tupleIndex, tupleBuffer[0], tupleBuffer[1], tupleBuffer[2]);
    return tupleStatus;
}

template<class TripleTableConfiguration>
template<class FT, bool callMonitor>
always_inline std::unique_ptr<TupleIterator> TripleTable<TripleTableConfiguration>::createTupleIteratorInternal(std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const ArgumentIndexSet& allInputArguments, const ArgumentIndexSet& surelyBoundInputArguments, const FT& filter, TupleIteratorMonitor* const tupleIteratorMonitor) const {
    const ArgumentIndex indexS = argumentIndexes[0];
    const ArgumentIndex indexP = argumentIndexes[1];
    const ArgumentIndex indexO = argumentIndexes[2];
    const uint8_t queryType = ::getConditionalMask(allInputArguments.contains(indexS), 1) | ::getConditionalMask(allInputArguments.contains(indexP), 2) | ::getConditionalMask(allInputArguments.contains(indexO), 4);
    const uint8_t surelyBoundQueryType = ::getConditionalMask(surelyBoundInputArguments.contains(indexS), 1) | ::getConditionalMask(surelyBoundInputArguments.contains(indexP), 2) | ::getConditionalMask(surelyBoundInputArguments.contains(indexO), 4);
    if (queryType == surelyBoundQueryType) {
        switch (queryType) {
        case 0:
            if (indexS == indexP) {
                if (indexP == indexO)
                    return std::unique_ptr<TupleIterator>(new FixedQueryTypeTripleTableIterator<TripleTableType, FT, 0, 4, callMonitor>(tupleIteratorMonitor, argumentsBuffer, argumentIndexes, allInputArguments, surelyBoundInputArguments, *this, filter));
                else
                    return std::unique_ptr<TupleIterator>(new FixedQueryTypeTripleTableIterator<TripleTableType, FT, 0, 1, callMonitor>(tupleIteratorMonitor, argumentsBuffer, argumentIndexes, allInputArguments, surelyBoundInputArguments, *this, filter));
            }
            else if (indexS == indexO)
                return std::unique_ptr<TupleIterator>(new FixedQueryTypeTripleTableIterator<TripleTableType, FT, 0, 2, callMonitor>(tupleIteratorMonitor, argumentsBuffer, argumentIndexes, allInputArguments, surelyBoundInputArguments, *this, filter));
            else if (indexP == indexO)
                return std::unique_ptr<TupleIterator>(new FixedQueryTypeTripleTableIterator<TripleTableType, FT, 0, 3, callMonitor>(tupleIteratorMonitor, argumentsBuffer, argumentIndexes, allInputArguments, surelyBoundInputArguments, *this, filter));
            else
                return std::unique_ptr<TupleIterator>(new FixedQueryTypeTripleTableIterator<TripleTableType, FT, 0, 0, callMonitor>(tupleIteratorMonitor, argumentsBuffer, argumentIndexes, allInputArguments, surelyBoundInputArguments, *this, filter));
        case 1:
            if (indexP == indexO)
                return std::unique_ptr<TupleIterator>(new FixedQueryTypeTripleTableIterator<TripleTableType, FT, 1, 3, callMonitor>(tupleIteratorMonitor, argumentsBuffer, argumentIndexes, allInputArguments, surelyBoundInputArguments, *this, filter));
            else
                return std::unique_ptr<TupleIterator>(new FixedQueryTypeTripleTableIterator<TripleTableType, FT, 1, 0, callMonitor>(tupleIteratorMonitor, argumentsBuffer, argumentIndexes, allInputArguments, surelyBoundInputArguments, *this, filter));
        case 2:
            if (indexS == indexO)
                return std::unique_ptr<TupleIterator>(new FixedQueryTypeTripleTableIterator<TripleTableType, FT, 2, 2, callMonitor>(tupleIteratorMonitor, argumentsBuffer, argumentIndexes, allInputArguments, surelyBoundInputArguments, *this, filter));
            else
                return std::unique_ptr<TupleIterator>(new FixedQueryTypeTripleTableIterator<TripleTableType, FT, 2, 0, callMonitor>(tupleIteratorMonitor, argumentsBuffer, argumentIndexes, allInputArguments, surelyBoundInputArguments, *this, filter));
        case 3:
            return std::unique_ptr<TupleIterator>(new FixedQueryTypeTripleTableIterator<TripleTableType, FT, 3, 0, callMonitor>(tupleIteratorMonitor, argumentsBuffer, argumentIndexes, allInputArguments, surelyBoundInputArguments, *this, filter));
        case 4:
            if (indexS == indexP)
                return std::unique_ptr<TupleIterator>(new FixedQueryTypeTripleTableIterator<TripleTableType, FT, 4, 1, callMonitor>(tupleIteratorMonitor, argumentsBuffer, argumentIndexes, allInputArguments, surelyBoundInputArguments, *this, filter));
            else
                return std::unique_ptr<TupleIterator>(new FixedQueryTypeTripleTableIterator<TripleTableType, FT, 4, 0, callMonitor>(tupleIteratorMonitor, argumentsBuffer, argumentIndexes, allInputArguments, surelyBoundInputArguments, *this, filter));
        case 5:
            return std::unique_ptr<TupleIterator>(new FixedQueryTypeTripleTableIterator<TripleTableType, FT, 5, 0, callMonitor>(tupleIteratorMonitor, argumentsBuffer, argumentIndexes, allInputArguments, surelyBoundInputArguments, *this, filter));
        case 6:
            return std::unique_ptr<TupleIterator>(new FixedQueryTypeTripleTableIterator<TripleTableType, FT, 6, 0, callMonitor>(tupleIteratorMonitor, argumentsBuffer, argumentIndexes, allInputArguments, surelyBoundInputArguments, *this, filter));
        case 7:
            return std::unique_ptr<TupleIterator>(new FixedQueryTypeTripleTableIterator<TripleTableType, FT, 7, 0, callMonitor>(tupleIteratorMonitor, argumentsBuffer, argumentIndexes, allInputArguments, surelyBoundInputArguments, *this, filter));
        default:
            UNREACHABLE;
        }
    }
    else {
        if (indexS == indexP && indexS != indexO && !surelyBoundInputArguments.contains(indexS))
            return std::unique_ptr<TupleIterator>(new VariableQueryTypeTripleTableIterator<TripleTableType, FT, 1, callMonitor>(tupleIteratorMonitor, argumentsBuffer, argumentIndexes, allInputArguments, surelyBoundInputArguments, *this, filter));
        else if (indexS == indexO && indexS != indexP && !surelyBoundInputArguments.contains(indexS))
            return std::unique_ptr<TupleIterator>(new VariableQueryTypeTripleTableIterator<TripleTableType, FT, 2, callMonitor>(tupleIteratorMonitor, argumentsBuffer, argumentIndexes, allInputArguments, surelyBoundInputArguments, *this, filter));
        else if (indexP == indexO && indexS != indexP && !surelyBoundInputArguments.contains(indexP))
            return std::unique_ptr<TupleIterator>(new VariableQueryTypeTripleTableIterator<TripleTableType, FT, 3, callMonitor>(tupleIteratorMonitor, argumentsBuffer, argumentIndexes, allInputArguments, surelyBoundInputArguments, *this, filter));
        else if (indexS == indexP && indexS == indexO && !surelyBoundInputArguments.contains(indexS))
            return std::unique_ptr<TupleIterator>(new VariableQueryTypeTripleTableIterator<TripleTableType, FT, 4, callMonitor>(tupleIteratorMonitor, argumentsBuffer, argumentIndexes, allInputArguments, surelyBoundInputArguments, *this, filter));
        else
            return std::unique_ptr<TupleIterator>(new VariableQueryTypeTripleTableIterator<TripleTableType, FT, 0, callMonitor>(tupleIteratorMonitor, argumentsBuffer, argumentIndexes, allInputArguments, surelyBoundInputArguments, *this, filter));
    }
}

template<class TripleTableConfiguration>
std::unique_ptr<TupleIterator> TripleTable<TripleTableConfiguration>::createTupleIterator(std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const ArgumentIndexSet& allInputArguments, const ArgumentIndexSet& surelyBoundInputArguments, const TupleStatus tupleStatusMask, const TupleStatus tupleStatusExpectedValue, TupleIteratorMonitor* const tupleIteratorMonitor) const {
    if (tupleIteratorMonitor == nullptr)
        return createTupleIteratorInternal<FilterByTupleStatus, false>(argumentsBuffer, argumentIndexes, allInputArguments, surelyBoundInputArguments, FilterByTupleStatus(tupleStatusMask, tupleStatusExpectedValue), tupleIteratorMonitor);
    else
        return createTupleIteratorInternal<FilterByTupleStatus, true>(argumentsBuffer, argumentIndexes, allInputArguments, surelyBoundInputArguments, FilterByTupleStatus(tupleStatusMask, tupleStatusExpectedValue), tupleIteratorMonitor);
}

template<class TripleTableConfiguration>
std::unique_ptr<TupleIterator> TripleTable<TripleTableConfiguration>::createTupleIterator(std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const ArgumentIndexSet& allInputArguments, const ArgumentIndexSet& surelyBoundInputArguments, const TupleFilter* & tupleFilter, const void* const tupleFilterContext, TupleIteratorMonitor* const tupleIteratorMonitor) const {
    if (tupleIteratorMonitor == nullptr)
        return createTupleIteratorInternal<FilterByTupleFilter, false>(argumentsBuffer, argumentIndexes, allInputArguments, surelyBoundInputArguments, FilterByTupleFilter(tupleFilter, tupleFilterContext), tupleIteratorMonitor);
    else
        return createTupleIteratorInternal<FilterByTupleFilter, true>(argumentsBuffer, argumentIndexes, allInputArguments, surelyBoundInputArguments, FilterByTupleFilter(tupleFilter, tupleFilterContext), tupleIteratorMonitor);
}

template<class TripleTableConfiguration>
always_inline size_t TripleTable<TripleTableConfiguration>::getCountEstimate(ThreadContext& threadContext, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const ArgumentIndexSet& allInputArguments) const {
    ArgumentIndex indexS = argumentIndexes[0];
    ArgumentIndex indexP = argumentIndexes[1];
    ArgumentIndex indexO = argumentIndexes[2];
    ResourceID queryS = argumentsBuffer[indexS];
    ResourceID queryP = argumentsBuffer[indexP];
    ResourceID queryO = argumentsBuffer[indexO];
    uint8_t queryMask = ::getConditionalMask(allInputArguments.contains(indexS), 1) | ::getConditionalMask(allInputArguments.contains(indexP), 2) | ::getConditionalMask(allInputArguments.contains(indexO), 4);
    switch (queryMask) {
    case 0:
        return QP0HandlerType::getCountEstimate(threadContext, *this, queryS, queryP, queryO);
    case 1:
        return QP1HandlerType::getCountEstimate(threadContext, *this, queryS, queryP, queryO);
    case 2:
        return QP2HandlerType::getCountEstimate(threadContext, *this, queryS, queryP, queryO);
    case 3:
        return QP3HandlerType::getCountEstimate(threadContext, *this, queryS, queryP, queryO);
    case 4:
        return QP4HandlerType::getCountEstimate(threadContext, *this, queryS, queryP, queryO);
    case 5:
        return QP5HandlerType::getCountEstimate(threadContext, *this, queryS, queryP, queryO);
    case 6:
        return QP6HandlerType::getCountEstimate(threadContext, *this, queryS, queryP, queryO);
    case 7:
        return QP7HandlerType::getCountEstimate(threadContext, *this, queryS, queryP, queryO);
    default:
        UNREACHABLE;
    }
}

template<class TripleTableConfiguration>
size_t TripleTable<TripleTableConfiguration>::getCountEstimate(const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const ArgumentIndexSet& allInputArguments) const {
    return TripleTable<TripleTableConfiguration>::getCountEstimate(ThreadContext::getCurrentThreadContext(), argumentsBuffer, argumentIndexes, allInputArguments);
}

template<class TripleTableConfiguration>
bool TripleTable<TripleTableConfiguration>::scheduleForAddition(ThreadContext& threadContext, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes) {
    const std::pair<bool, TupleIndex> result = addTuple(threadContext, argumentsBuffer, argumentIndexes, 0, TUPLE_STATUS_EDB_INS);
    if (result.first) {
        m_triplesScheduledForAddition.enqueue<TripleListType::IS_CONCURRENT>(result.second);
        return true;
    }
    else
        return false;
}

template<class TripleTableConfiguration>
bool TripleTable<TripleTableConfiguration>::scheduleForAddition(const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes) {
    return TripleTable<TripleTableConfiguration>::scheduleForAddition(ThreadContext::getCurrentThreadContext(), argumentsBuffer, argumentIndexes);
}

template<class TripleTableConfiguration>
const LockFreeQueue<TupleIndex>& TripleTable<TripleTableConfiguration>::getTupleIndexesScheduledForAddition() const {
    return m_triplesScheduledForAddition;
}

template<class TripleTableConfiguration>
bool TripleTable<TripleTableConfiguration>::scheduleForDeletion(ThreadContext& threadContext, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes) {
    const TupleIndex tupleIndex = getTupleIndex(threadContext, argumentsBuffer, argumentIndexes);
    if (tupleIndex != INVALID_TUPLE_INDEX && addTupleStatus(tupleIndex, TUPLE_STATUS_EDB_DEL)) {
        m_triplesScheduledForDeletion.enqueue<TripleListType::IS_CONCURRENT>(tupleIndex);
        return true;
    }
    else
        return false;
}

template<class TripleTableConfiguration>
bool TripleTable<TripleTableConfiguration>::scheduleForDeletion(const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes) {
    return TripleTable<TripleTableConfiguration>::scheduleForDeletion(ThreadContext::getCurrentThreadContext(), argumentsBuffer, argumentIndexes);
}

template<class TripleTableConfiguration>
const LockFreeQueue<TupleIndex>& TripleTable<TripleTableConfiguration>::getTupleIndexesScheduledForDeletion() const {
    return m_triplesScheduledForDeletion;
}

template<class TripleTableConfiguration>
void TripleTable<TripleTableConfiguration>::printContents(std::ostream& output, const bool printTriples, const bool printManager1, const bool printManager2, const bool printManager3) const {
    const TupleIndex firstFreeTripleIndex = m_tripleList.getFirstFreeTripleIndex();
    output << "TripleTable" << std::endl;
    output << "First free triple index: " << firstFreeTripleIndex << std::endl << std::endl;
    if (printTriples) {
        output << "-- START OF TRIPLES ------------------------------------------------------------" << std::endl;
        const size_t tupleIndexNumberOfDigits = ::getNumberOfDigits(firstFreeTripleIndex);
        for (TupleIndex tupleIndex = INVALID_TUPLE_INDEX; tupleIndex < firstFreeTripleIndex; ++tupleIndex) {
            const std::streamsize oldWidth = output.width();
            const std::ostream::fmtflags oldFlags = output.flags();
            output.width(tupleIndexNumberOfDigits);
            output.setf(std::ios::right);
            output << tupleIndex;
            output.width(oldWidth);
            output.flags(oldFlags);
            TupleStatus tupleStatus;
            ResourceID s;
            ResourceID p;
            ResourceID o;
            m_tripleList.getTripleStatusAndResourceIDs(tupleIndex, tupleStatus, s, p, o);
            output << ": " << s << "  " << p << "  " << o << "  + " << std::hex << tupleStatus << std::dec << std::endl;
        }
        output << "-- END OF TRIPLES --------------------------------------------------------------" << std::endl;
    }
    if (printManager1) {
        output << "TWO KEYS MANAGER 1" << std::endl;
        m_twoKeysManager1.printContents(output);
    }
    if (printManager2) {
        output << "TWO KEYS MANAGER 2" << std::endl;
        m_twoKeysManager2.printContents(output);
    }
    if (printManager3) {
        output << "TWO KEYS MANAGER 3" << std::endl;
        m_twoKeysManager3.printContents(output);
    }
}

template<class TripleTableConfiguration>
std::unique_ptr<ComponentStatistics> TripleTable<TripleTableConfiguration>::getComponentStatistics() const {
    std::unique_ptr<ComponentStatistics> result(new ComponentStatistics("TripleTable"));
    std::unique_ptr<ComponentStatistics> tripleListStatistics = m_tripleList.getComponentStatistics();
    std::unique_ptr<ComponentStatistics> twoKeysManager1Statistics = m_twoKeysManager1.getComponentStatistics();
    std::unique_ptr<ComponentStatistics> twoKeysManager2Statistics = m_twoKeysManager2.getComponentStatistics();
    std::unique_ptr<ComponentStatistics> twoKeysManager3Statistics = m_twoKeysManager3.getComponentStatistics();
    std::unique_ptr<ComponentStatistics> threeKeysManagerStatistics = m_threeKeysManager.getComponentStatistics();
    const uint64_t aggregateSize =
    tripleListStatistics->getItemIntegerValue("Size") +
    twoKeysManager1Statistics->getItemIntegerValue("Aggregate size") +
    twoKeysManager2Statistics->getItemIntegerValue("Aggregate size") +
    twoKeysManager3Statistics->getItemIntegerValue("Aggregate size") +
    threeKeysManagerStatistics->getItemIntegerValue("Size");
    result->addIntegerItem("Aggregate size", aggregateSize);
    const size_t tripleCount = m_tripleList.getExactTripleCount(TUPLE_STATUS_COMPLETE, TUPLE_STATUS_COMPLETE);
    result->addIntegerItem("Triple count", tripleCount);
    if (tripleCount != 0)
        result->addFloatingPointItem("Bytes per triple", aggregateSize / static_cast<double>(tripleCount));
    result->addSubcomponent(std::move(tripleListStatistics));
    result->addSubcomponent(std::move(twoKeysManager1Statistics));
    result->addSubcomponent(std::move(twoKeysManager2Statistics));
    result->addSubcomponent(std::move(twoKeysManager3Statistics));
    result->addSubcomponent(std::move(threeKeysManagerStatistics));
    return result;
}

template<class TripleTableConfiguration>
void TripleTable<TripleTableConfiguration>::initialize(const size_t initialTripleCapacity, const size_t initialResourceCapacity) {
    if (!m_tripleList.initialize(initialTripleCapacity))
        throw RDF_STORE_EXCEPTION("Cannot initialize the triple list.");
    if (!m_twoKeysManager1.initialize(initialTripleCapacity, initialResourceCapacity))
        throw RDF_STORE_EXCEPTION("Cannot initialize two keys manager 1.");
    if (!m_twoKeysManager2.initialize(initialTripleCapacity, initialResourceCapacity))
        throw RDF_STORE_EXCEPTION("Cannot initialize two keys manager 2.");
    if (!m_twoKeysManager3.initialize(initialTripleCapacity, initialResourceCapacity))
        throw RDF_STORE_EXCEPTION("Cannot initialize two keys manager 3.");
    if (!m_threeKeysManager.initialize(initialTripleCapacity))
        throw RDF_STORE_EXCEPTION("Cannot initialize three keys manager.");
    if (!m_triplesScheduledForAddition.initializeLarge())
        throw RDF_STORE_EXCEPTION("Cannot initialize the insertion list.");
    if (!m_triplesScheduledForDeletion.initializeLarge())
        throw RDF_STORE_EXCEPTION("Cannot initialize the deletion list.");
}

template<class TripleTableConfiguration>
void TripleTable<TripleTableConfiguration>::reindex(const bool dropIDB, const size_t initialTripleCapacity, const size_t initialResourceCapacity) {
    if (!m_twoKeysManager1.initialize(initialTripleCapacity, initialResourceCapacity))
        throw RDF_STORE_EXCEPTION("Cannot initialize two keys manager 1.");
    if (!m_twoKeysManager2.initialize(initialTripleCapacity, initialResourceCapacity))
        throw RDF_STORE_EXCEPTION("Cannot initialize two keys manager 2.");
    if (!m_twoKeysManager3.initialize(initialTripleCapacity, initialResourceCapacity))
        throw RDF_STORE_EXCEPTION("Cannot initialize two keys manager 3.");
    if (!m_threeKeysManager.initialize(initialTripleCapacity))
        throw RDF_STORE_EXCEPTION("Cannot initialize three keys manager.");
    if (!m_triplesScheduledForAddition.initializeLarge())
        throw RDF_STORE_EXCEPTION("Cannot initialize the insertion list.");
    if (!m_triplesScheduledForDeletion.initializeLarge())
        throw RDF_STORE_EXCEPTION("Cannot initialize the deletion list.");
    ThreadContext& threadContext = ThreadContext::getCurrentThreadContext();
    TupleIndex writeTupleIndex = m_tripleList.getFirstWriteTripleIndex();
    TupleIndex readTupleIndex = m_tripleList.getFirstTripleIndex();
    while (readTupleIndex != INVALID_TUPLE_INDEX) {
        const TupleStatus tupleStatus = m_tripleList.getTripleStatus(readTupleIndex);
        if ((tupleStatus & TUPLE_STATUS_EDB) == TUPLE_STATUS_EDB || (!dropIDB && (tupleStatus & (TUPLE_STATUS_IDB | TUPLE_STATUS_IDB_MERGED)) == TUPLE_STATUS_IDB)) {
            ResourceID s;
            ResourceID p;
            ResourceID o;
            m_tripleList.getResourceIDs(readTupleIndex, s, p ,o);
            if (readTupleIndex > writeTupleIndex)
                m_tripleList.addAt(writeTupleIndex, s, p, o);
            m_tripleList.setTripleStatus(writeTupleIndex, TUPLE_STATUS_COMPLETE | (tupleStatus & (TUPLE_STATUS_EDB | TUPLE_STATUS_IDB)));
            typename ThreeKeysManagerType::InsertToken threeKeysInsertToken;
            bool alreadyExists;
            if (!m_threeKeysManager.getInsertToken(threadContext, s, p, o, threeKeysInsertToken, alreadyExists))
                throw RDF_STORE_EXCEPTION("Memory exhausted.");
            m_threeKeysManager.updateOnInsert(threadContext, threeKeysInsertToken, writeTupleIndex, s, p, o);
            m_threeKeysManager.releaseInsertToken(threadContext, threeKeysInsertToken);
            if (!m_twoKeysManager1.insertTriple(threadContext, writeTupleIndex, s, p, o) || !m_twoKeysManager2.insertTriple(threadContext, writeTupleIndex, s, p, o) || !m_twoKeysManager3.insertTriple(threadContext, writeTupleIndex, s, p, o))
                throw RDF_STORE_EXCEPTION("Memory exhausted.");
            ++writeTupleIndex;
        }
        readTupleIndex = m_tripleList.getNextTripleIndex(readTupleIndex);
    }
    m_tripleList.truncate(writeTupleIndex);
}

template<class TripleTableConfiguration>
void TripleTable<TripleTableConfiguration>::makeFactsExplicit() {
    m_triplesScheduledForAddition.initializeLarge();
    m_triplesScheduledForDeletion.initializeLarge();
    TupleIndex tupleIndex = m_tripleList.getFirstTripleIndex();
    while (tupleIndex != INVALID_TUPLE_INDEX) {
        const TupleStatus tupleStatus = m_tripleList.getTripleStatus(tupleIndex);
        m_tripleList.setTripleStatus(tupleIndex, TUPLE_STATUS_COMPLETE | ((tupleStatus & (TUPLE_STATUS_IDB_MERGED | TUPLE_STATUS_IDB)) == TUPLE_STATUS_IDB ? TUPLE_STATUS_IDB | TUPLE_STATUS_EDB : 0));
        tupleIndex = m_tripleList.getNextTripleIndex(tupleIndex);
    }
}

template<class TripleTableConfiguration>
void TripleTable<TripleTableConfiguration>::saveFormatted(OutputStream& outputStream) const {
    outputStream.writeString("TripleTable-Formatted");
    m_tripleList.save(outputStream);
    m_twoKeysManager1.save(outputStream);
    m_twoKeysManager2.save(outputStream);
    m_twoKeysManager3.save(outputStream);
    m_threeKeysManager.save(outputStream);
    m_triplesScheduledForAddition.save(outputStream);
    m_triplesScheduledForDeletion.save(outputStream);
}

template<class TripleTableConfiguration>
void TripleTable<TripleTableConfiguration>::loadFormatted(InputStream& inputStream) {
    if (!inputStream.checkNextString("TripleTable-Formatted"))
        throw RDF_STORE_EXCEPTION("Invalid input: cannot load TripleTable.");
    m_tripleList.load(inputStream);
    m_twoKeysManager1.load(inputStream);
    m_twoKeysManager2.load(inputStream);
    m_twoKeysManager3.load(inputStream);
    m_threeKeysManager.load(inputStream);
    m_triplesScheduledForAddition.load(inputStream);
    m_triplesScheduledForDeletion.load(inputStream);
}

template<class TripleTableConfiguration>
void TripleTable<TripleTableConfiguration>::saveUnformatted(OutputStream& outputStream) const {
    outputStream.writeString("TripleTable-Unformatted");
    size_t tripleCount = 0;
    TupleIndex tupleIndex = m_tripleList.getFirstTripleIndex();
    while (tupleIndex != INVALID_TUPLE_INDEX) {
        ++tripleCount;
        tupleIndex = m_tripleList.getNextTripleIndex(tupleIndex);
    }
    outputStream.write(tripleCount);
    const size_t TRIPLE_SIZE = sizeof(TupleStatus) + 3 * sizeof(ResourceID);
    std::unique_ptr<uint8_t[]> contents(new uint8_t[TRIPLE_SIZE * tripleCount]);
    uint8_t* current = contents.get();
    tupleIndex = m_tripleList.getFirstTripleIndex();
    while (tupleIndex != INVALID_TUPLE_INDEX) {
        m_tripleList.getTripleStatusAndResourceIDs(tupleIndex, *reinterpret_cast<TupleStatus*>(current), *reinterpret_cast<ResourceID*>(current + sizeof(TupleStatus)), *reinterpret_cast<ResourceID*>(current + sizeof(TupleStatus) + sizeof(ResourceID)),  *reinterpret_cast<ResourceID*>(current + sizeof(TupleStatus) + 2 * sizeof(ResourceID)));
        current += TRIPLE_SIZE;
        tupleIndex = m_tripleList.getNextTripleIndex(tupleIndex);
    }
    outputStream.writeExactly(contents.get(), TRIPLE_SIZE * tripleCount);
}

class UnformattedLoadingThread : public Thread {

protected:

    TupleTable& m_table;
    const uint8_t* const m_firstResource;
    const uint8_t* const m_afterLastResource;

public:

    UnformattedLoadingThread(TupleTable& table, const uint8_t* const firstResource, const uint8_t* afterLastResource) : m_table(table), m_firstResource(firstResource), m_afterLastResource(afterLastResource) {
    }

    virtual void run();

};

template<class TripleTableConfiguration>
void TripleTable<TripleTableConfiguration>::loadUnformatted(InputStream& inputStream, const size_t numberOfThreads) {
    if (!inputStream.checkNextString("TripleTable-Unformatted"))
        throw RDF_STORE_EXCEPTION("Invalid input: cannot load TripleTable.");
    const size_t tripleCount = inputStream.read<size_t>();
    const size_t TRIPLE_SIZE = sizeof(TupleStatus) + 3 * sizeof(ResourceID);
    std::unique_ptr<uint8_t> contents(new uint8_t[TRIPLE_SIZE * tripleCount]);
    inputStream.readExactly(contents.get(), TRIPLE_SIZE * tripleCount);
    if (numberOfThreads < 2 || !m_tripleList.IS_CONCURRENT || tripleCount < numberOfThreads) {
        std::vector<ResourceID> argumentsBuffer;
        argumentsBuffer.insert(argumentsBuffer.begin(), 3, INVALID_RESOURCE_ID);
        std::vector<ArgumentIndex> argumentIndexes;
        argumentIndexes.push_back(0);
        argumentIndexes.push_back(1);
        argumentIndexes.push_back(2);
        const uint8_t* const afterLastResource = contents.get() + TRIPLE_SIZE * tripleCount;
        for (const uint8_t* current = contents.get(); current != afterLastResource;) {
            const TupleStatus tupleStatus = *reinterpret_cast<const TupleStatus*>(current);
            current += sizeof(TupleStatus);
            argumentsBuffer[0] = *reinterpret_cast<const ResourceID*>(current);
            current += sizeof(ResourceID);
            argumentsBuffer[1] = *reinterpret_cast<const ResourceID*>(current);
            current += sizeof(ResourceID);
            argumentsBuffer[2] = *reinterpret_cast<const ResourceID*>(current);
            current += sizeof(ResourceID);
            addTuple(argumentsBuffer, argumentIndexes, 0, tupleStatus);
        }
    }
    else {
        unique_ptr_vector<UnformattedLoadingThread> threads;
        const size_t tripleCountPerWorker = tripleCount / numberOfThreads;
        const uint8_t* currentFirst = contents.get();
        for (size_t threadIndex = 0; threadIndex < numberOfThreads; threadIndex++) {
            const uint8_t* currentAfterLast = currentFirst + TRIPLE_SIZE * tripleCountPerWorker;
            if (threadIndex + 1 == numberOfThreads)
                currentAfterLast = contents.get() + TRIPLE_SIZE * tripleCount;
            std::unique_ptr<UnformattedLoadingThread> thread(new UnformattedLoadingThread(*this, currentFirst, currentAfterLast));
            threads.push_back(std::move(thread));
            threads.back()->start();
            currentFirst = currentAfterLast;
        }
        for (size_t threadIndex = 0; threadIndex < numberOfThreads; threadIndex++)
            threads[threadIndex]->join();
    }
}

template<class TripleTableConfiguration>
void TripleTable<TripleTableConfiguration>::updateStatistics() {
    m_twoKeysManager1.startUpdatingStatistics();
    m_twoKeysManager2.startUpdatingStatistics();
    m_twoKeysManager3.startUpdatingStatistics();
    const TupleIndex firstFreeTripleIndex = m_tripleList.getFirstFreeTripleIndex();
    for (TupleIndex tupleIndex = 1; tupleIndex < firstFreeTripleIndex; ++tupleIndex) {
        TupleStatus tupleStatus;
        ResourceID s;
        ResourceID p;
        ResourceID o;
        m_tripleList.getTripleStatusAndResourceIDs(tupleIndex, tupleStatus, s, p, o);
        if ((tupleStatus & TUPLE_STATUS_COMPLETE) == TUPLE_STATUS_COMPLETE && s != INVALID_RESOURCE_ID) {
            m_twoKeysManager1.updateStatisticsFor(s, p, o);
            m_twoKeysManager2.updateStatisticsFor(s, p, o);
            m_twoKeysManager3.updateStatisticsFor(s, p, o);
        }
    }
}

#endif /* TRIPLETABLEIMPL_H_ */
