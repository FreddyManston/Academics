// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef ABSTRACTSTATISTICSIMPL_H_
#define ABSTRACTSTATISTICSIMPL_H_

#include "../storage/DataStore.h"
#include "../reasoning/RuleIndex.h"
#include "AbstractStatistics.h"

// Redirection tables

#define DECLARE_REDIRECTION_TABLE(name, offset) \
    static const size_t name##_REDIRECTION[AbstractStatistics::NUMBER_OF_DERIVATION_COUNTERS] = {\
        offset + 0,\
        offset + 1,\
        offset + 2,\
        offset + 3,\
        offset + 4,\
        offset + 5,\
        offset + 6,\
        offset + 7,\
        offset + 8,\
        offset + 9,\
        offset + 10,\
        offset + 11,\
        offset + 12,\
        offset + 13,\
        offset + 14,\
        offset + 15,\
        offset + 16,\
        offset + 17,\
        offset + 18,\
        offset + 19,\
        offset + 20,\
        offset + 21,\
        offset + 22,\
    }

// AbstractStatistics::ThreadState

always_inline void AbstractStatistics::ThreadState::setCurrentComponentLevel(const size_t componentLevel) {
    m_counters.ensureLevelExists(componentLevel);
    m_currentComponentLevel = componentLevel;
}

always_inline void AbstractStatistics::ThreadState::increment(const size_t counterIndex) {
    m_counters[m_currentComponentLevel][m_currentRedirectionTable[counterIndex]]++;
}

always_inline void AbstractStatistics::ThreadState::incrementNoRedirect(const size_t counterIndex) {
    m_counters[m_currentComponentLevel][counterIndex]++;
}

always_inline void AbstractStatistics::ThreadState::set(const size_t counterIndex, const size_t value) {
    m_counters[m_currentComponentLevel][counterIndex] = value;
}

// AbstractStatistics

always_inline void AbstractStatistics::setRedirectionTable(const size_t workerIndex, const size_t* redirectionTable) {
    m_statesByThread[workerIndex]->m_currentRedirectionTable = redirectionTable;
}

// AbstractStatisticsImpl

template<class Interface>
AbstractStatisticsImpl<Interface>::AbstractStatisticsImpl() : AbstractStatistics() {
}

template<class Interface>
AbstractStatisticsImpl<Interface>::~AbstractStatisticsImpl() {
}

template<class Interface>
void AbstractStatisticsImpl<Interface>::taskStarted(const DataStore& dataStore, const size_t maxComponentLevel) {
    m_statesByThread.clear();
    const size_t numberOfThreads = dataStore.getNumberOfThreads();
    size_t numberOfCountersPerLevel;
    const char* const* counterDescriptions = describeStatistics(numberOfCountersPerLevel);
    for (size_t threadIndex = 0; threadIndex < numberOfThreads; ++threadIndex)
        m_statesByThread.push_back(newThreadState(numberOfCountersPerLevel, counterDescriptions, maxComponentLevel + 1));
}

template<class Interface>
void AbstractStatisticsImpl<Interface>::taskFinished(const DataStore& dataStore) {
}

template<class Interface>
void AbstractStatisticsImpl<Interface>::componentLevelStarted(const size_t componentLevel) {
    if (componentLevel == static_cast<size_t>(-1)) {
        for (auto iterator = m_statesByThread.begin(); iterator != m_statesByThread.end(); ++iterator)
            (*iterator)->setCurrentComponentLevel(0);
    }
    else {
        for (auto iterator = m_statesByThread.begin(); iterator != m_statesByThread.end(); ++iterator)
            (*iterator)->setCurrentComponentLevel(componentLevel);
    }
}

template<class Interface>
void AbstractStatisticsImpl<Interface>::componentLevelFinished(const size_t componentLevel) {
}

template<class Interface>
void AbstractStatisticsImpl<Interface>::materializationStarted(const size_t workerIndex) {
}

template<class Interface>
void AbstractStatisticsImpl<Interface>::materializationFinished(const size_t workerIndex) {
}

template<class Interface>
void AbstractStatisticsImpl<Interface>::currentTupleExtracted(const size_t workerIndex, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes) {
    ThreadState& threadState = *m_statesByThread[workerIndex];
    threadState.increment(EXTRACTED_TUPLES);
    threadState.m_matchedFirstBodyLiterals = 0;
    threadState.m_matchedFirstBodyLiteralsWithNoDerivation = 0;
}

template<class Interface>
void AbstractStatisticsImpl<Interface>::currentTupleNormalized(const size_t workerIndex, std::vector<ResourceID>& normalizedArgumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const bool wasAdded) {
    ThreadState& threadState = *m_statesByThread[workerIndex];
    threadState.increment(DERIVATIONS);
    threadState.increment(DERIVATIONS_REPLACEMENT);
    if (wasAdded)
        threadState.increment(DERIVATIONS_REPLACEMENT_SUCCESFUL);
}

template<class Interface>
void AbstractStatisticsImpl<Interface>::currentTupleProcessed(const size_t workerIndex) {
    ThreadState& threadState = *m_statesByThread[workerIndex];
    if (threadState.m_matchedFirstBodyLiterals > 0) {
        threadState.increment(EXTRACTED_TUPLES_MATCHING_A_BODY_LITERAL);
        if (threadState.m_matchedFirstBodyLiteralsWithNoDerivation > 0)
            threadState.increment(EXTRACTED_TUPLES_MATCHING_NO_DERIVATION);
    }
}

template<class Interface>
void AbstractStatisticsImpl<Interface>::bodyLiteralMatchedStarted(const size_t workerIndex, const BodyLiteralInfo& bodyLiteralInfo) {
    ThreadState& threadState = *m_statesByThread[workerIndex];
    threadState.increment(MATCHED_BODY_LITERALS);
    if (threadState.m_currentBodyLiteralDepth == 0) {
        threadState.increment(MATCHED_FIRST_BODY_LITERALS);
        threadState.m_matchedFirstBodyLiterals++;
        threadState.m_matchedRulesInstancesSinceFirstBodyLiteral = 0;
    }
    threadState.m_currentBodyLiteralDepth++;
}

template<class Interface>
void AbstractStatisticsImpl<Interface>::bodyLiteralMatchedFinish(const size_t workerIndex) {
    ThreadState& threadState = *m_statesByThread[workerIndex];
    threadState.m_currentBodyLiteralDepth--;
    if (threadState.m_currentBodyLiteralDepth == 0 && threadState.m_matchedRulesInstancesSinceFirstBodyLiteral == 0) {
        threadState.increment(MATCHED_FIRST_BODY_LITERALS_NO_DERIVATION);
        threadState.m_matchedFirstBodyLiteralsWithNoDerivation++;
    }
}

template<class Interface>
void AbstractStatisticsImpl<Interface>::bodyLiteralMatchingStarted(const size_t workerIndex, const BodyLiteralInfo& bodyLiteralInfo) {
}

template<class Interface>
void AbstractStatisticsImpl<Interface>::bodyLiteralMatchingFinished(const size_t  workerIndex) {
}

template<class Interface>
void AbstractStatisticsImpl<Interface>::ruleReevaluationStarted(const size_t workerIndex, const RuleInfo& ruleInfo) {
    ThreadState& threadState = *m_statesByThread[workerIndex];
    threadState.m_inRuleReevaluation = true;
    threadState.m_matchedInstancesForRule = 0;
    threadState.increment(ATTEMPTS_TO_REEVALUATE_RULE);
}

template<class Interface>
void AbstractStatisticsImpl<Interface>::ruleReevaluationFinished(const size_t workerIndex) {
    ThreadState& threadState = *m_statesByThread[workerIndex];
    threadState.m_inRuleReevaluation = false;
    if (threadState.m_matchedInstancesForRule == 0)
        threadState.increment(ATTEMPTS_TO_REEVALUATE_RULE_NO_DERIVATION);
}

template<class Interface>
void AbstractStatisticsImpl<Interface>::pivotlessRuleEvaluationStarted(const size_t workerIndex, const RuleInfo& ruleInfo) {
    ThreadState& threadState = *m_statesByThread[workerIndex];
    threadState.m_inRuleReevaluation = false;
    threadState.m_matchedInstancesForRule = 0;
    threadState.increment(ATTEMPTS_TO_REEVALUATE_RULE);
}

template<class Interface>
void AbstractStatisticsImpl<Interface>::pivotlessRuleEvaluationFinished(const size_t workerIndex) {
    ThreadState& threadState = *m_statesByThread[workerIndex];
    threadState.m_inRuleReevaluation = false;
    if (threadState.m_matchedInstancesForRule == 0)
        threadState.increment(ATTEMPTS_TO_REEVALUATE_RULE_NO_DERIVATION);
}

template<class Interface>
void AbstractStatisticsImpl<Interface>::ruleMatchedStarted(const size_t workerIndex, const RuleInfo& ruleInfo, const BodyLiteralInfo* const lastBodyLiteralInfo, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes) {
    ThreadState& threadState = *m_statesByThread[workerIndex];
    threadState.increment(MATCHED_RULE_INSTANCES);
    if (lastBodyLiteralInfo == nullptr) {
        threadState.increment(threadState.m_inRuleReevaluation ? MATCHED_RULE_INSTANCES_RULE_REEVALUATION : MATCHED_RULE_INSTANCES_PIVOTLESS_EVALUATION);
        threadState.m_matchedInstancesForRule++;
    }
    else {
        threadState.increment(MATCHED_RULE_INSTANCES_TUPLE_EXTRACTION);
        threadState.m_matchedRulesInstancesSinceFirstBodyLiteral++;
    }
}

template<class Interface>
void AbstractStatisticsImpl<Interface>::ruleMatchedFinished(const size_t workerIndex) {
}

template<class Interface>
void AbstractStatisticsImpl<Interface>::tupleDerived(const size_t workerIndex, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const bool isNormal, const bool wasAdded) {
    ThreadState& threadState = *m_statesByThread[workerIndex];
    threadState.increment(DERIVATIONS);
    threadState.increment(DERIVATIONS_RULES);
    if (!isNormal)
        threadState.increment(DERIVATIONS_RULES_NOT_NORMAL);
    if (wasAdded)
        threadState.increment(DERIVATIONS_RULES_SUCCESSFUL);
}

template<class Interface>
void AbstractStatisticsImpl<Interface>::constantMerged(const size_t workerIndex, const ResourceID sourceID, const ResourceID targetID, const bool isSuccessful) {
    ThreadState& threadState = *m_statesByThread[workerIndex];
    threadState.increment(EQUALITIES);
    if (isSuccessful)
        threadState.increment(EQUALITIES_SUCCESSFUL);
}

template<class Interface>
void AbstractStatisticsImpl<Interface>::reflexiveSameAsTupleDerived(const size_t workerIndex, const ResourceID resourceID, const bool wasAdded) {
    ThreadState& threadState = *m_statesByThread[workerIndex];
    threadState.increment(DERIVATIONS);
    threadState.increment(DERIVATIONS_REFLEXIVITY);
    if (wasAdded)
        m_statesByThread[workerIndex]->increment(DERIVATIONS_REFLEXIVITY_SUCCESFUL);
}

template<class Interface>
void AbstractStatisticsImpl<Interface>::normalizeConstantStarted(const size_t workerIndex, const ResourceID mergedID) {
}

template<class Interface>
void AbstractStatisticsImpl<Interface>::tupleNormalized(const size_t workerIndex, std::vector<ResourceID>& originalArgumentsBuffer, const std::vector<ArgumentIndex>& originalArgumentIndexes, std::vector<ResourceID>& normalizedArgumentsBuffer, const std::vector<ArgumentIndex>& normalizedArgumentIndexes, const bool wasAdded) {
    ThreadState& threadState = *m_statesByThread[workerIndex];
    threadState.increment(DERIVATIONS);
    threadState.increment(DERIVATIONS_REPLACEMENT);
    if (wasAdded)
        threadState.increment(DERIVATIONS_REPLACEMENT_SUCCESFUL);
}

template<class Interface>
void AbstractStatisticsImpl<Interface>::normalizeConstantFinished(const size_t workerIndex) {
}

#endif /* ABSTRACTSTATISTICSIMPL_H_ */
