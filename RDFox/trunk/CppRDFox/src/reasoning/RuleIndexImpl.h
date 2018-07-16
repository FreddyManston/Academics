// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef RULEINDEXIMPL_H_
#define RULEINDEXIMPL_H_

#include "../dictionary/ResourceValueCache.h"
#include "../storage/DataStore.h"
#include "../util/SequentialHashTableImpl.h"
#include "MaterializationMonitor.h"
#include "RuleIndex.h"

// BodyTupleIteratorFilters

template<TupleIteratorFilterType FilterType, typename TargetType, typename PositiveCheckType, typename NegativeCheckType>
always_inline void TupleIteratorFilters<FilterType, TargetType, PositiveCheckType, NegativeCheckType>::setFilters(const size_t workerIndex, TupleFilter* positiveTupleFilter, TupleFilter* negativeTupleFilter) {
    RuleIndex::ByThreadInfo& byTreadInfo = *m_ruleIndex->m_byThreadInfos[workerIndex];
    switch (FilterType) {
    case MAIN_TUPLE_ITERATOR_FILTER:
        byTreadInfo.m_mainRuleEvaluatorPositiveFilter = positiveTupleFilter;
        byTreadInfo.m_mainRuleEvaluatorNegativeFilter = negativeTupleFilter;
        break;
    case INCREMENTAL_TUPLE_ITERATOR_FILTER:
        byTreadInfo.m_incrementalRuleEvaluatorPositiveFilter = positiveTupleFilter;
        byTreadInfo.m_incrementalRuleEvaluatorNegativeFilter = negativeTupleFilter;
        break;
    case SUPPORTING_FACTS_TUPLE_ITERATOR_FILTER:
        byTreadInfo.m_supportingFactsPositiveFilter = positiveTupleFilter;
        byTreadInfo.m_supportingFactsNegativeFilter = negativeTupleFilter;
        break;
    }
}

template<TupleIteratorFilterType FilterType, typename TargetType, typename PositiveCheckType, typename NegativeCheckType>
always_inline TupleIteratorFilters<FilterType, TargetType, PositiveCheckType, NegativeCheckType>::TupleIteratorFilters(RuleIndex* ruleIndex, TargetType& target, PositiveCheckType positiveCheck, NegativeCheckType negativeCheck) :
    m_ruleIndex(ruleIndex),
    m_positiveTupleFilter(target, positiveCheck),
    m_negativeTupleFilter(target, negativeCheck)
{
    setFilters(target.getWorkerIndex(), &m_positiveTupleFilter, &m_negativeTupleFilter);
}

template<TupleIteratorFilterType FilterType, typename TargetType, typename PositiveCheckType, typename NegativeCheckType>
always_inline TupleIteratorFilters<FilterType, TargetType, PositiveCheckType, NegativeCheckType>::TupleIteratorFilters(TupleIteratorFilters<FilterType, TargetType, PositiveCheckType, NegativeCheckType>&& other) :
    m_ruleIndex(other.m_ruleIndex),
    m_positiveTupleFilter(std::move(other.m_positiveTupleFilter)),
    m_negativeTupleFilter(std::move(other.m_negativeTupleFilter))
{
    other.m_ruleIndex = nullptr;
    setFilters(m_positiveTupleFilter.m_target.getWorkerIndex(), &m_positiveTupleFilter, &m_negativeTupleFilter);
}

template<TupleIteratorFilterType FilterType, typename TargetType, typename PositiveCheckType, typename NegativeCheckType>
always_inline TupleIteratorFilters<FilterType, TargetType, PositiveCheckType, NegativeCheckType>::~TupleIteratorFilters() {
    if (m_ruleIndex != nullptr)
        setFilters(m_positiveTupleFilter.m_target.getWorkerIndex(), nullptr, nullptr);
}

// BodyLiteralInfoFilter

template<BodyLiteralInfoFilterType FilterType, typename TargetType, typename CheckType>
always_inline void BodyLiteralInfoFilter<FilterType, TargetType, CheckType>::setFilter(TupleFilter* tupleFilter) {
    RuleIndex::ByThreadInfo& byThreadInfo = *m_ruleIndex->m_byThreadInfos[m_target.getWorkerIndex()];
    switch (FilterType) {
    case MAIN_POSITIVE_BEFORE_PIVOT:
        byThreadInfo.m_mainPositiveBodyLiteralBeforePivotFilter = tupleFilter;
        break;
    case MAIN_POSITIVE_AFTER_PIVOT:
        byThreadInfo.m_mainPositiveBodyLiteralAfterPivotFilter = tupleFilter;
        break;
    case MAIN_NEGATIVE_SINGLE_ATOM_BEFORE_PIVOT:
        byThreadInfo.m_mainNegativeBodyLiteralSingleAtomBeforePivotFilter = tupleFilter;
        break;
    case MAIN_NEGATIVE_MULTIPLE_ATOMS_BEFORE_PIVOT:
        byThreadInfo.m_mainNegativeBodyLiteralMultipleAtomsBeforePivotFilter = tupleFilter;
        break;
    case MAIN_NEGATIVE_AT_PIVOT_OR_AFTER_PIVOT:
        byThreadInfo.m_mainNegativeBodyLiteralPivotOrAfterPivotFilter = tupleFilter;
        break;
    case INCREMENTAL_POSITIVE_BEFORE_PIVOT:
        byThreadInfo.m_incrementalPositiveBodyLiteralBeforePivotFilter = tupleFilter;
        break;
    case INCREMENTAL_POSITIVE_AFTER_PIVOT:
        byThreadInfo.m_incrementalPositiveBodyLiteralAfterPivotFilter = tupleFilter;
        break;
    case INCREMENTAL_NEGATIVE:
        byThreadInfo.m_incrementalNegativeBodyLiteralFilterIncremental = tupleFilter;
        break;
    }
}

template<BodyLiteralInfoFilterType FilterType, typename TargetType, typename CheckType>
always_inline BodyLiteralInfoFilter<FilterType, TargetType, CheckType>::BodyLiteralInfoFilter(RuleIndex* ruleIndex, TargetType& target, CheckType check) :
    m_ruleIndex(ruleIndex),
    m_target(target),
    m_check(check)
{
    setFilter(this);
}

template<BodyLiteralInfoFilterType FilterType, typename TargetType, typename CheckType>
always_inline BodyLiteralInfoFilter<FilterType, TargetType, CheckType>::BodyLiteralInfoFilter(BodyLiteralInfoFilter<FilterType, TargetType, CheckType>&& other) :
    m_ruleIndex(other.m_ruleIndex),
    m_target(other.m_target),
    m_check(std::move(other.m_check))
{
    other.m_ruleIndex = nullptr;
    setFilter(this);
}

template<BodyLiteralInfoFilterType FilterType, typename TargetType, typename CheckType>
always_inline BodyLiteralInfoFilter<FilterType, TargetType, CheckType>::~BodyLiteralInfoFilter() {
    if (m_ruleIndex != nullptr)
        setFilter(nullptr);
}

// UnderlyingLiteralInfoFilter

template<UnderlyingLiteralInfoFilterType FilterType, typename TargetType, typename CheckType>
always_inline void UnderlyingLiteralInfoFilter<FilterType, TargetType, CheckType>::setFilter(TupleFilter* tupleFilter) {
    RuleIndex::ByThreadInfo& byThreadInfo = *m_ruleIndex->m_byThreadInfos[m_target.getWorkerIndex()];
    switch (FilterType) {
    case UNDERLYING_BEFORE_PIVOT:
        byThreadInfo.m_underlyingLiteralBeforePivotFilter = tupleFilter;
        break;
    case UNDERLYING_AFTER_PIVOT:
        byThreadInfo.m_underlyingLiteralAfterPivotFilter = tupleFilter;
        break;
    }
}

template<UnderlyingLiteralInfoFilterType FilterType, typename TargetType, typename CheckType>
always_inline UnderlyingLiteralInfoFilter<FilterType, TargetType, CheckType>::UnderlyingLiteralInfoFilter(RuleIndex* ruleIndex, TargetType& target, CheckType check) :
    m_ruleIndex(ruleIndex),
    m_target(target),
    m_check(check)
{
    setFilter(this);
}

template<UnderlyingLiteralInfoFilterType FilterType, typename TargetType, typename CheckType>
always_inline UnderlyingLiteralInfoFilter<FilterType, TargetType, CheckType>::UnderlyingLiteralInfoFilter(UnderlyingLiteralInfoFilter<FilterType, TargetType, CheckType>&& other) :
    m_ruleIndex(other.m_ruleIndex),
    m_target(other.m_target),
    m_check(std::move(other.m_check))
{
    other.m_ruleIndex = nullptr;
    setFilter(this);
}

template<UnderlyingLiteralInfoFilterType FilterType, typename TargetType, typename CheckType>
always_inline UnderlyingLiteralInfoFilter<FilterType, TargetType, CheckType>::~UnderlyingLiteralInfoFilter() {
    if (m_ruleIndex != nullptr)
        setFilter(nullptr);
}

template<UnderlyingLiteralInfoFilterType FilterType, typename TargetType, typename CheckType>
bool UnderlyingLiteralInfoFilter<FilterType, TargetType, CheckType>::processTuple(const void* const tupleFilterContext, const TupleIndex tupleIndex, const TupleStatus tupleStatus) const {
    return m_check(m_target, tupleIndex, tupleStatus);
}

// ApplicationManager

always_inline bool ApplicationManager::satisfiesEqualities(const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes) {
    if (!m_compareVariables.empty()) {
        for (auto iterator = m_compareVariables.begin(); iterator != m_compareVariables.end(); ++iterator)
            if (argumentsBuffer[argumentIndexes[iterator->m_index1]] != argumentsBuffer[argumentIndexes[iterator->m_index2]])
                return false;
    }
    return true;
}

always_inline bool ApplicationManager::prepareApply(const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, std::vector<ResourceID>& targetArgumentsBuffer) {
    if (satisfiesEqualities(argumentsBuffer, argumentIndexes)) {
        for (auto iterator = m_copyToBuffer.begin(); iterator != m_copyToBuffer.end(); ++iterator)
            targetArgumentsBuffer[iterator->m_targetIndex] = argumentsBuffer[argumentIndexes[iterator->m_sourceIndex]];
        return true;
    }
    else
        return false;
}

// BodyLiteralInfo

template<bool isMain, ComponentLevelFilter componentLevelFilter, bool callMonitor, class ConsumerType>
void BodyLiteralInfo::literalMatched(ThreadContext& threadContext, const size_t workerIndex, const size_t componentLevel, MaterializationMonitor* const materializationMonitor, ConsumerType consumer) {
    if (callMonitor)
        materializationMonitor->bodyLiteralMatchedStarted(workerIndex, *this);
    for (auto iterator = m_lastLiteralForHeadAtoms.begin(); iterator != m_lastLiteralForHeadAtoms.end(); ++iterator)
        if (componentLevelFilter == ALL_COMPONENTS || (*iterator)->getComponentLevel() == componentLevel)
            (*iterator)->headAtomMatched<isMain, callMonitor, ConsumerType>(threadContext, workerIndex, this, materializationMonitor, consumer);
    NonPivotBodyLiteralInfo* currentChild = m_firstChild;
    while (currentChild != nullptr) {
        currentChild->matchLiteral<isMain, componentLevelFilter, callMonitor, ConsumerType>(threadContext, workerIndex, componentLevel, materializationMonitor, consumer);
        currentChild = currentChild->m_nextSibling;
    }
    if (callMonitor)
        materializationMonitor->bodyLiteralMatchedFinish(workerIndex);
}

// PivotPositiveBodyLiteralInfo

template<bool isMain, ComponentLevelFilter componentLevelFilter, bool callMonitor, class ConsumerType>
always_inline void PivotPositiveBodyLiteralInfo::applyTo(ThreadContext& threadContext, const size_t workerIndex, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const size_t componentLevel, MaterializationMonitor* const materializationMonitor, ConsumerType consumer) {
    if (isInComponentLevelFilter<componentLevelFilter>(componentLevel)) {
        std::vector<ResourceID>& targetArgumentsBuffer(isMain ? m_ruleIndex.m_byThreadInfos[workerIndex]->m_argumentBufferMain : m_ruleIndex.m_byThreadInfos[workerIndex]->m_argumentBufferIncremental);
        if (prepareApply(argumentsBuffer, argumentIndexes, targetArgumentsBuffer))
            literalMatched<isMain, componentLevelFilter, callMonitor, ConsumerType>(threadContext, workerIndex, componentLevel, materializationMonitor, consumer);
    }
}

// PivotNegationBodyLiteralInfo

template<ComponentLevelFilter componentLevelFilter, bool callMonitor, class ConsumerType>
always_inline void PivotNegationBodyLiteralInfo::underlyingLiteralsMatched(ThreadContext& threadContext, const size_t workerIndex, const size_t componentLevel, MaterializationMonitor* const materializationMonitor, ConsumerType consumer) {
    if (!m_hasProjection || m_tupleIteratorsByThreadMain[workerIndex]->open(threadContext) != 0)
        literalMatched<true, componentLevelFilter, callMonitor, ConsumerType>(threadContext, workerIndex, componentLevel, materializationMonitor, consumer);
}

// NonPivotBodyLiteralInfo

template<bool isMain, ComponentLevelFilter componentLevelFilter, bool callMonitor, class ConsumerType>
always_inline void NonPivotBodyLiteralInfo::matchLiteral(ThreadContext& threadContext, const size_t workerIndex, const size_t componentLevel, MaterializationMonitor* const materializationMonitor, ConsumerType consumer) {
    if (isInComponentLevelFilter<componentLevelFilter>(componentLevel)) {
        if (callMonitor)
            materializationMonitor->bodyLiteralMatchingStarted(workerIndex, *this);
        TupleIterator& tupleIterator = (isMain ? *m_tupleIteratorsByThreadMain[workerIndex] : *m_tupleIteratorsByThreadIncremental[workerIndex]);
        size_t multiplicity = tupleIterator.open(threadContext);
        while (multiplicity != 0) {
            literalMatched<isMain, componentLevelFilter, callMonitor, ConsumerType>(threadContext, workerIndex, componentLevel, materializationMonitor, consumer);
            multiplicity = tupleIterator.advance();
        }
        if (callMonitor)
            materializationMonitor->bodyLiteralMatchingFinished(workerIndex);
    }
}

// UnderlyingLiteralInfo

template<class PBT>
template<ComponentLevelFilter componentLevelFilter, bool callMonitor, class ConsumerType>
void UnderlyingLiteralInfo<PBT>::literalMatched(ThreadContext& threadContext, const size_t workerIndex, const size_t componentLevel, MaterializationMonitor* const materializationMonitor, ConsumerType consumer) {
    for (auto iterator = m_lastLiteralForParentBodyLiterals.begin(); iterator != m_lastLiteralForParentBodyLiterals.end(); ++iterator)
        if ((*iterator)->template isInComponentLevelFilter<componentLevelFilter>(componentLevel))
            (*iterator)->template underlyingLiteralsMatched<componentLevelFilter, callMonitor, ConsumerType>(threadContext, workerIndex, componentLevel, materializationMonitor, consumer);
    NonPivotUnderlyingLiteralInfoType* currentChild = this->m_firstChild;
    while (currentChild != nullptr) {
        currentChild->template matchLiteral<componentLevelFilter, callMonitor, ConsumerType>(threadContext, workerIndex, componentLevel, materializationMonitor, consumer);
        currentChild = currentChild->m_nextSibling;
    }
}

// PivotUnderlyingLiteralInfo

template<class PBT>
template<ComponentLevelFilter componentLevelFilter, bool callMonitor, class ConsumerType>
always_inline void PivotUnderlyingLiteralInfo<PBT>::applyTo(ThreadContext& threadContext, const size_t workerIndex, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const size_t componentLevel, MaterializationMonitor* const materializationMonitor, ConsumerType consumer) {
    if (this->template isInComponentLevelFilter<componentLevelFilter>(componentLevel)) {
        std::vector<ResourceID>& targetArgumentsBuffer(this->m_ruleIndex.m_byThreadInfos[workerIndex]->m_argumentBufferMain);
        if (prepareApply(argumentsBuffer, argumentIndexes, targetArgumentsBuffer))
            this->template literalMatched<componentLevelFilter, callMonitor, ConsumerType>(threadContext, workerIndex, componentLevel, materializationMonitor, consumer);
    }
}

// NonPivotUnderlyingLiteralInfo

template<class PBT>
template<ComponentLevelFilter componentLevelFilter, bool callMonitor, class ConsumerType>
always_inline void NonPivotUnderlyingLiteralInfo<PBT>::matchLiteral(ThreadContext& threadContext, const size_t workerIndex, const size_t componentLevel, MaterializationMonitor* const materializationMonitor, ConsumerType consumer) {
    if (this->template isInComponentLevelFilter<componentLevelFilter>(componentLevel)) {
        TupleIterator& tupleIterator = (*m_tupleIteratorsByThread[workerIndex]);
        size_t multiplicity = tupleIterator.open(threadContext);
        while (multiplicity != 0) {
            this->template literalMatched<componentLevelFilter, callMonitor, ConsumerType>(threadContext, workerIndex, componentLevel, materializationMonitor, consumer);
            multiplicity = tupleIterator.advance();
        }
    }
}

// SupportingFactsEvaluator

always_inline size_t SupportingFactsEvaluator::open(ThreadContext& threadContext) {
    return m_tupleIterator->open(threadContext);
}

always_inline size_t SupportingFactsEvaluator::advance() {
    return m_tupleIterator->advance();
}

// HeadAtomInfo

template<bool isMain, bool callMonitor, class ConsumerType>
always_inline void HeadAtomInfo::headAtomMatched(ThreadContext& threadContext, const size_t workerIndex, const BodyLiteralInfo* const lastBodyLiteralInfo, MaterializationMonitor* const materializationMonitor, ConsumerType consumer) {
    if (m_ruleInfo.m_active) {
        std::vector<ResourceID>& argumentsBuffer(isMain ? m_ruleInfo.m_ruleIndex.m_byThreadInfos[workerIndex]->m_argumentBufferMain : m_ruleInfo.m_ruleIndex.m_byThreadInfos[workerIndex]->m_argumentBufferIncremental);
        if (callMonitor)
            materializationMonitor->ruleMatchedStarted(workerIndex, m_ruleInfo, lastBodyLiteralInfo, argumentsBuffer, m_headArgumentIndexes);
        consumer(threadContext, *this, argumentsBuffer, m_headArgumentIndexes);
        if (callMonitor)
            materializationMonitor->ruleMatchedFinished(workerIndex);
    }
}

always_inline HeadAtomInfo* HeadAtomInfo::getNextMatchingHeadAtomInfo(const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes) {
    HeadAtomInfo* result = HeadAtomInfoPatternMain::getNextIndexed();
    while (result != nullptr && !result->satisfiesEqualities(argumentsBuffer, argumentIndexes))
        result = result->HeadAtomInfoPatternMain::getNextIndexed();
    return result;
}

template<bool checkComponentLevel>
always_inline bool HeadAtomInfo::isSupportingBodyAtom(const size_t bodyLiteralIndex) const {
    const BodyLiteralInfo* const bodyLiteralInfo = m_supportingBodyLiteralInfosOrdered[bodyLiteralIndex];
    return bodyLiteralInfo != nullptr && (!checkComponentLevel || bodyLiteralInfo->getComponentLevel() == m_componentLevel);
}

always_inline SupportingFactsEvaluator& HeadAtomInfo::getSupportingFactsEvaluatorPrototype(const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes) {
    std::vector<ResourceID>& targetArgumentsBuffer = m_supportingFactsEvaluatorPrototype->m_argumentsBuffer;
    for (auto iterator = m_supportingFactsCopyToBuffer.begin(); iterator != m_supportingFactsCopyToBuffer.end(); ++iterator)
        targetArgumentsBuffer[iterator->m_targetIndex] = argumentsBuffer[argumentIndexes[iterator->m_sourceIndex]];
    return *m_supportingFactsEvaluatorPrototype;
}

always_inline std::unique_ptr<SupportingFactsEvaluator> HeadAtomInfo::getSupportingFactsEvaluator(const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes) {
    std::unique_ptr<SupportingFactsEvaluator> result;
    if (m_supportingFactsEvaluators.empty())
        result.reset(new SupportingFactsEvaluator(*m_supportingFactsEvaluatorPrototype));
    else {
        result = std::move(m_supportingFactsEvaluators.back());
        m_supportingFactsEvaluators.pop_back();
    }
    std::vector<ResourceID>& targetArgumentsBuffer = result->m_argumentsBuffer;
    for (auto iterator = m_supportingFactsCopyToBuffer.begin(); iterator != m_supportingFactsCopyToBuffer.end(); ++iterator)
        targetArgumentsBuffer[iterator->m_targetIndex] = argumentsBuffer[argumentIndexes[iterator->m_sourceIndex]];
    return result;
}

always_inline void HeadAtomInfo::leaveSupportFactsEvaluator(std::unique_ptr<SupportingFactsEvaluator> supportingFactsEvaluator) {
    m_supportingFactsEvaluators.push_back(std::move(supportingFactsEvaluator));
}

// RuleInfo

template<bool isMain, bool checkComponentLevel, bool callMonitor, class ConsumerType>
always_inline void RuleInfo::evaluateRule(ThreadContext& threadContext, const size_t workerIndex, const size_t componentLevel, MaterializationMonitor* const materializationMonitor, ConsumerType consumer) const {
    TupleIterator& tupleIterator = (isMain ? *m_ruleEvaluatorsByThreadMain[workerIndex] : *m_ruleEvaluatorsByThreadIncremental[workerIndex]);
    std::vector<ResourceID>& argumentsBuffer = (isMain ? m_ruleIndex.m_byThreadInfos[workerIndex]->m_argumentBufferMain : m_ruleIndex.m_byThreadInfos[workerIndex]->m_argumentBufferIncremental);
    size_t multiplicity = tupleIterator.open(threadContext);
    while (multiplicity != 0) {
        for (auto iterator = m_headAtomInfos.begin(); iterator != m_headAtomInfos.end(); ++iterator) {
            HeadAtomInfo& headAtomInfo = **iterator;
            if (!checkComponentLevel || headAtomInfo.getComponentLevel() == componentLevel) {
                if (callMonitor)
                    materializationMonitor->ruleMatchedStarted(workerIndex, *this, nullptr, argumentsBuffer, headAtomInfo.m_headArgumentIndexes);
                consumer(threadContext, headAtomInfo, argumentsBuffer, headAtomInfo.m_headArgumentIndexes);
                if (callMonitor)
                    materializationMonitor->ruleMatchedFinished(workerIndex);
            }
        }
        multiplicity = tupleIterator.advance();
    }
}

template<bool checkComponentLevel, bool callMonitor, class ConsumerType>
always_inline void RuleInfo::evaluateRuleMain(ThreadContext& threadContext, const size_t workerIndex, const size_t componentLevel, MaterializationMonitor* const materializationMonitor, ConsumerType consumer) const {
    evaluateRule<true, checkComponentLevel, callMonitor, ConsumerType>(threadContext, workerIndex, componentLevel, materializationMonitor, consumer);
}

template<bool checkComponentLevel, bool callMonitor, class ConsumerType>
always_inline void RuleInfo::evaluateRuleIncremental(ThreadContext& threadContext, const size_t workerIndex, const size_t componentLevel, MaterializationMonitor* const materializationMonitor, ConsumerType consumer) const {
    evaluateRule<false, checkComponentLevel, callMonitor, ConsumerType>(threadContext, workerIndex, componentLevel, materializationMonitor, consumer);
}

// LiteralPatternIndex::LiteralPatternIndexPolicy

template<class ObjectType, class PatternType>
always_inline void LiteralPatternIndex<ObjectType, PatternType>::LiteralPatternIndexPolicy::getBucketContents(const uint8_t* const bucket, typename LiteralPatternIndex<ObjectType, PatternType>::LiteralPatternIndexPolicy::BucketContents& bucketContents) {
    bucketContents.m_object = const_cast<ObjectType*>(*reinterpret_cast<const ObjectType* const *>(bucket));
}

template<class ObjectType, class PatternType>
always_inline BucketStatus LiteralPatternIndex<ObjectType, PatternType>::LiteralPatternIndexPolicy::getBucketContentsStatus(const typename LiteralPatternIndex<ObjectType, PatternType>::LiteralPatternIndexPolicy::BucketContents& bucketContents, const size_t valuesHashCode, const ResourceID subjectID, const ResourceID predicateID, const ResourceID objectID) {
    if (bucketContents.m_object == nullptr)
        return BUCKET_EMPTY;
    else
        return bucketContents.m_object->PatternType::m_currentIndexingPattern[0] == subjectID && bucketContents.m_object->PatternType::m_currentIndexingPattern[1] == predicateID && bucketContents.m_object->PatternType::m_currentIndexingPattern[2] == objectID ? BUCKET_CONTAINS : BUCKET_NOT_CONTAINS;
}

template<class ObjectType, class PatternType>
always_inline BucketStatus LiteralPatternIndex<ObjectType, PatternType>::LiteralPatternIndexPolicy::getBucketContentsStatus(const typename LiteralPatternIndex<ObjectType, PatternType>::LiteralPatternIndexPolicy::BucketContents& bucketContents, const size_t valuesHashCode, const ObjectType* const object) {
    if (bucketContents.m_object == nullptr)
        return BUCKET_EMPTY;
    else
        return bucketContents.m_object->PatternType::m_currentIndexingPattern[0] == object->PatternType::m_currentIndexingPattern[0] && bucketContents.m_object->PatternType::m_currentIndexingPattern[1] == object->PatternType::m_currentIndexingPattern[1] && bucketContents.m_object->PatternType::m_currentIndexingPattern[2] == object->PatternType::m_currentIndexingPattern[2] ? BUCKET_CONTAINS : BUCKET_NOT_CONTAINS;
}

template<class ObjectType, class PatternType>
always_inline bool LiteralPatternIndex<ObjectType, PatternType>::LiteralPatternIndexPolicy::setBucketContentsIfEmpty(uint8_t* const bucket, const typename LiteralPatternIndex<ObjectType, PatternType>::LiteralPatternIndexPolicy::BucketContents& bucketContents) {
    if (*reinterpret_cast<ObjectType**>(bucket) == nullptr) {
        setObject(bucket, bucketContents.m_object);
        return true;
    }
    else
        return false;
}

template<class ObjectType, class PatternType>
always_inline bool LiteralPatternIndex<ObjectType, PatternType>::LiteralPatternIndexPolicy::isBucketContentsEmpty(const typename LiteralPatternIndex<ObjectType, PatternType>::LiteralPatternIndexPolicy::BucketContents& bucketContents) {
    return bucketContents.m_object == nullptr;
}

template<class ObjectType, class PatternType>
always_inline size_t LiteralPatternIndex<ObjectType, PatternType>::LiteralPatternIndexPolicy::getBucketContentsHashCode(const typename LiteralPatternIndex<ObjectType, PatternType>::LiteralPatternIndexPolicy::BucketContents& bucketContents) {
    return hashCodeFor(bucketContents.m_object->PatternType::m_currentIndexingPattern[0], bucketContents.m_object->PatternType::m_currentIndexingPattern[1], bucketContents.m_object->PatternType::m_currentIndexingPattern[2]);
}

template<class ObjectType, class PatternType>
always_inline size_t LiteralPatternIndex<ObjectType, PatternType>::LiteralPatternIndexPolicy::hashCodeFor(const ResourceID subjectID, const ResourceID predicateID, const ResourceID objectID) {
    size_t hash = 0;

    hash += subjectID;
    hash += (hash << 10);
    hash ^= (hash >> 6);

    hash += predicateID;
    hash += (hash << 10);
    hash ^= (hash >> 6);

    hash += objectID;
    hash += (hash << 10);
    hash ^= (hash >> 6);

    hash += (hash << 3);
    hash ^= (hash >> 11);
    hash += (hash << 15);
    return hash;
}

template<class ObjectType, class PatternType>
always_inline size_t LiteralPatternIndex<ObjectType, PatternType>::LiteralPatternIndexPolicy::hashCodeFor(const ObjectType* const object) {
    return hashCodeFor(object->PatternType::m_currentIndexingPattern[0], object->PatternType::m_currentIndexingPattern[1], object->PatternType::m_currentIndexingPattern[2]);
}

template<class ObjectType, class PatternType>
always_inline void LiteralPatternIndex<ObjectType, PatternType>::LiteralPatternIndexPolicy::makeBucketEmpty(uint8_t* const bucket) {
    *reinterpret_cast<ObjectType**>(bucket) = nullptr;
}

template<class ObjectType, class PatternType>
always_inline const ObjectType* LiteralPatternIndex<ObjectType, PatternType>::LiteralPatternIndexPolicy::getObject(const uint8_t* const bucket) {
    return *reinterpret_cast<const ObjectType* const *>(bucket);
}

template<class ObjectType, class PatternType>
always_inline void LiteralPatternIndex<ObjectType, PatternType>::LiteralPatternIndexPolicy::setObject(uint8_t* const bucket, ObjectType* const object) {
    *reinterpret_cast<ObjectType**>(bucket) = object;
}

// LiteralPatternIndex

template<class ObjectType, class PatternType>
always_inline ObjectType* LiteralPatternIndex<ObjectType, PatternType>::getFirstObject(ThreadContext& threadContext, const size_t indexingPatternNumber, const ResourceID subjectID, const ResourceID predicateID, const ResourceID objectID) {
    const ResourceID querySubjectID = ((indexingPatternNumber & 0x4) == 0 ? INVALID_RESOURCE_ID : subjectID);
    const ResourceID queryPredicateID = ((indexingPatternNumber & 0x2) == 0 ? INVALID_RESOURCE_ID : predicateID);
    const ResourceID queryObjectID = ((indexingPatternNumber & 0x1) == 0 ? INVALID_RESOURCE_ID : objectID);
    typename SequentialHashTable<LiteralPatternIndexPolicy>::BucketDescriptor bucketDescriptor;
    m_index.acquireBucket(threadContext, bucketDescriptor, querySubjectID, queryPredicateID, queryObjectID);
    m_index.continueBucketSearch(threadContext, bucketDescriptor, querySubjectID, queryPredicateID, queryObjectID);
    m_index.releaseBucket(threadContext, bucketDescriptor);
    return bucketDescriptor.m_bucketContents.m_object;
}

// HeadAtomInfoByPatternIndex

template<class PatternType>
always_inline HeadAtomInfo* HeadAtomInfoByPatternIndex<PatternType>::getMatchingHeadAtomInfos(ThreadContext& threadContext, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const size_t indexingPatternNumber) {
    if (this->m_patternCounts[indexingPatternNumber] != 0) {
        HeadAtomInfo* headAtomInfo = this->getFirstObject(threadContext, indexingPatternNumber, argumentsBuffer[argumentIndexes[0]], argumentsBuffer[argumentIndexes[1]], argumentsBuffer[argumentIndexes[2]]);
        while (headAtomInfo != nullptr && !headAtomInfo->satisfiesEqualities(argumentsBuffer, argumentIndexes))
            headAtomInfo = headAtomInfo->PatternType::getNextIndexed();
        return headAtomInfo;
    }
    else
        return nullptr;
}

// BodyLiteralInfoByPatternIndex

template<class PatternType>
template<bool isMain, ComponentLevelFilter componentLevelFilter, bool callMonitor, class ConsumerType>
always_inline void PivotPositiveBodyLiteralInfoByPatternIndex<PatternType>::applyRulesTo(ThreadContext& threadContext, const size_t workerIndex, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const size_t componentLevel, MaterializationMonitor* const materializationMonitor, ConsumerType consumer) {
    for (size_t indexingPatternNumber = 0; indexingPatternNumber < 8; ++indexingPatternNumber) {
        if (this->m_patternCounts[indexingPatternNumber] != 0) {
            PivotPositiveBodyLiteralInfo* pivotPositiveBodyLiteralInfo = this->getFirstObject(threadContext, indexingPatternNumber, argumentsBuffer[argumentIndexes[0]], argumentsBuffer[argumentIndexes[1]], argumentsBuffer[argumentIndexes[2]]);
            while (pivotPositiveBodyLiteralInfo != nullptr) {
                pivotPositiveBodyLiteralInfo->template applyTo<isMain, componentLevelFilter, callMonitor, ConsumerType>(threadContext, workerIndex, argumentsBuffer, argumentIndexes, componentLevel, materializationMonitor, consumer);
                pivotPositiveBodyLiteralInfo = pivotPositiveBodyLiteralInfo->PatternType::getNextIndexed();
            }
        }
    }
}

// UnderlyingLiteralInfoByPatternIndex

template<class PatternType>
template<ComponentLevelFilter componentLevelFilter, bool callMonitor, class ConsumerType>
always_inline void PivotUnderlyingLiteralInfoByPatternIndex<PatternType>::applyRulesTo(ThreadContext& threadContext, const size_t workerIndex, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const size_t componentLevel, MaterializationMonitor* const materializationMonitor, ConsumerType consumer) {
    for (size_t indexingPatternNumber = 0; indexingPatternNumber < 8; ++indexingPatternNumber) {
        if (this->m_patternCounts[indexingPatternNumber] != 0) {
            typename PatternType::PivotUnderlyingLiteralInfoType* pivotUnderlyingLiteralInfo = this->getFirstObject(threadContext, indexingPatternNumber, argumentsBuffer[argumentIndexes[0]], argumentsBuffer[argumentIndexes[1]], argumentsBuffer[argumentIndexes[2]]);
            while (pivotUnderlyingLiteralInfo != nullptr) {
                pivotUnderlyingLiteralInfo->template applyTo<componentLevelFilter, callMonitor, ConsumerType>(threadContext, workerIndex, argumentsBuffer, argumentIndexes, componentLevel, materializationMonitor, consumer);
                pivotUnderlyingLiteralInfo = pivotUnderlyingLiteralInfo->PatternType::getNextIndexed();
            }
        }
    }
}

// RuleIndex

template<ComponentLevelFilter componentLevelFilter, bool callMonitor, class ConsumerType>
always_inline void RuleIndex::applyRulesToPositiveLiteralMain(ThreadContext& threadContext, const size_t workerIndex, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const size_t componentLevel, MaterializationMonitor* const materializationMonitor, ConsumerType consumer) {
    m_pivotPositiveBodyLiteralInfoByPatternIndexMain.applyRulesTo<true, componentLevelFilter, callMonitor, ConsumerType>(threadContext, workerIndex, argumentsBuffer, argumentIndexes, componentLevel, materializationMonitor, consumer);
}

template<ComponentLevelFilter componentLevelFilter, bool callMonitor, class ConsumerType>
always_inline void RuleIndex::applyRulesToPositiveLiteralIncremental(ThreadContext& threadContext, const size_t workerIndex, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const size_t componentLevel, MaterializationMonitor* const materializationMonitor, ConsumerType consumer) {
    m_pivotPositiveBodyLiteralInfoByPatternIndexIncremental.applyRulesTo<false, componentLevelFilter, callMonitor, ConsumerType>(threadContext, workerIndex, argumentsBuffer, argumentIndexes, componentLevel, materializationMonitor, consumer);
}

template<ComponentLevelFilter componentLevelFilter, bool callMonitor, class ConsumerType>
always_inline void RuleIndex::applyRulesToUnderlyingNegationLiteralMain(ThreadContext& threadContext, const size_t workerIndex, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const size_t componentLevel, MaterializationMonitor* const materializationMonitor, ConsumerType consumer) {
    m_pivotUnderlyingNegationLiteralInfoByPatternIndexMain.applyRulesTo<componentLevelFilter, callMonitor, ConsumerType>(threadContext, workerIndex, argumentsBuffer, argumentIndexes, componentLevel, materializationMonitor, consumer);
}

always_inline HeadAtomInfo* RuleIndex::getMatchingHeadAtomInfos(ThreadContext& threadContext, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const size_t indexingPatternNumber) {
    return m_headAtomInfoByPatternIndexMain.getMatchingHeadAtomInfos(threadContext, argumentsBuffer, argumentIndexes, indexingPatternNumber);
}

template<TupleIteratorFilterType FilterType, typename TargetType, typename PositiveCheckType, typename NegativeCheckType>
always_inline TupleIteratorFilters<FilterType, TargetType, PositiveCheckType, NegativeCheckType> RuleIndex::setTupleIteratorFilters(TargetType& target, PositiveCheckType positiveCheck, NegativeCheckType negativeCheck) {
    return TupleIteratorFilters<FilterType, TargetType, PositiveCheckType, NegativeCheckType>(this, target, positiveCheck, negativeCheck);
}

template<TupleIteratorFilterType FilterType, typename TargetType, typename CheckType>
always_inline TupleIteratorFilters<FilterType, TargetType, CheckType, CheckType> RuleIndex::setTupleIteratorFilters(TargetType& target, CheckType check) {
    return TupleIteratorFilters<FilterType, TargetType, CheckType, CheckType>(this, target, check, check);
}

template<BodyLiteralInfoFilterType FilterType, typename TargetType, typename CheckType>
always_inline BodyLiteralInfoFilter<FilterType, TargetType, CheckType> RuleIndex::setBodyLiteralInfoFilter(TargetType& target, CheckType check) {
    return BodyLiteralInfoFilter<FilterType, TargetType, CheckType>(this, target, check);
}

template<UnderlyingLiteralInfoFilterType FilterType, typename TargetType, typename CheckType>
always_inline UnderlyingLiteralInfoFilter<FilterType, TargetType, CheckType> RuleIndex::setUnderlyingLiteralInfoFilter(TargetType& target, CheckType check) {
    return UnderlyingLiteralInfoFilter<FilterType, TargetType, CheckType>(this, target, check);
}

#endif /* RULEINDEXIMPL_H_ */
