// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "../reasoning/RuleIndex.h"
#include "AbstractStatistics.h"

// AbstractStatistics::ThreadState

AbstractStatistics::ThreadState::ThreadState(const size_t numberOfCountersPerLevel, const char* const* counterDescriptions, const size_t initialNumberOfLevels) :
    m_counters(numberOfCountersPerLevel, counterDescriptions, initialNumberOfLevels),
    m_currentComponentLevel(0),
    m_matchedFirstBodyLiterals(0),
    m_matchedRulesInstancesSinceFirstBodyLiteral(0),
    m_matchedFirstBodyLiteralsWithNoDerivation(0),
    m_matchedInstancesForRule(0),
    m_currentBodyLiteralDepth(0),
    m_currentRedirectionTable(0),
    m_inRuleReevaluation(false)
{
}

// AbstractStatistics

std::unique_ptr<AbstractStatistics::ThreadState> AbstractStatistics::newThreadState(const size_t numberOfCountersPerLevel, const char* const* counterDescriptions, const size_t initialNumberOfLevels) {
    return std::unique_ptr<ThreadState>(new ThreadState(numberOfCountersPerLevel, counterDescriptions, initialNumberOfLevels));
}

AbstractStatistics::AbstractStatistics() : m_statesByThread() {
}

AbstractStatistics::~AbstractStatistics() {
}

StatisticsCounters AbstractStatistics::getStatisticsCounters() {
    unique_ptr_vector<ThreadState>::iterator iterator = m_statesByThread.begin();
    StatisticsCounters statisticsCounters((*iterator)->m_counters);
    for (++iterator; iterator != m_statesByThread.end(); ++iterator)
        statisticsCounters += (*iterator)->m_counters;
    return statisticsCounters;
}
