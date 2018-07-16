// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "../Common.h"
#include "../RDFStoreException.h"
#include "../node-server/QueryInfo.h"
#include "NodeServerStatistics.h"

static const size_t STAGES_TO_COUNT = 30;
static const size_t PROCESSED_STATS = 4;
static const size_t PROCESSED_QUERY_PLAN = PROCESSED_STATS + 1;
static const size_t PROCESSED_QUERY_PLAN_ACKNOWLEDGED = PROCESSED_QUERY_PLAN + 1;
static const size_t PROCESSED_PARTIAL_BINDINGS_0 = PROCESSED_QUERY_PLAN_ACKNOWLEDGED + 1;
static const size_t PROCESSED_PARTIAL_BINDINGS_REST = PROCESSED_PARTIAL_BINDINGS_0 + STAGES_TO_COUNT;
static const size_t PROCESSED_STAGE_FINISHED_0 = PROCESSED_PARTIAL_BINDINGS_REST + 1;
static const size_t PROCESSED_STAGE_FINISHED_REST = PROCESSED_STAGE_FINISHED_0 + STAGES_TO_COUNT;
static const size_t SENT_STATS = PROCESSED_STAGE_FINISHED_REST + 1;
static const size_t SENT_QUERY_PLAN = SENT_STATS + 1;
static const size_t SENT_QUERY_PLAN_ACKNOWLEDGED = SENT_QUERY_PLAN + 1;
static const size_t SENT_PARTIAL_BINDINGS_0 = SENT_QUERY_PLAN_ACKNOWLEDGED + 1;
static const size_t SENT_PARTIAL_BINDINGS_REST = SENT_PARTIAL_BINDINGS_0 + STAGES_TO_COUNT;
static const size_t SENT_STAGE_FINISHED_0 = SENT_PARTIAL_BINDINGS_REST + 1;
static const size_t SENT_STAGE_FINISHED_REST = SENT_STAGE_FINISHED_0 + STAGES_TO_COUNT;
static const size_t NUMBER_OF_COUNTERS = SENT_STAGE_FINISHED_REST + 1;

static const char* const s_counterDescriptions[NUMBER_OF_COUNTERS] = {
    "Number of started queries",
    "    Completed",
    "Number of matched atoms",
    "Number of produced answers",
    "-PROCESSED MESSAGE STATISTICS",
    "Query plan",
    "Query plan acknowledged",
    "Partial bindings - atom 0",
    "Partial bindings - atom 1",
    "Partial bindings - atom 2",
    "Partial bindings - atom 3",
    "Partial bindings - atom 4",
    "Partial bindings - atom 5",
    "Partial bindings - atom 6",
    "Partial bindings - atom 7",
    "Partial bindings - atom 8",
    "Partial bindings - atom 9",
    "Partial bindings - atom 10",
    "Partial bindings - atom 11",
    "Partial bindings - atom 12",
    "Partial bindings - atom 13",
    "Partial bindings - atom 14",
    "Partial bindings - atom 15",
    "Partial bindings - atom 16",
    "Partial bindings - atom 17",
    "Partial bindings - atom 18",
    "Partial bindings - atom 19",
    "Partial bindings - atom 20",
    "Partial bindings - atom 21",
    "Partial bindings - atom 22",
    "Partial bindings - atom 23",
    "Partial bindings - atom 24",
    "Partial bindings - atom 25",
    "Partial bindings - atom 26",
    "Partial bindings - atom 27",
    "Partial bindings - atom 28",
    "Partial bindings - atom 29",
    "Partial bindings - remiaining atoms",
    "Stage finished - atom 0",
    "Stage finished - atom 1",
    "Stage finished - atom 2",
    "Stage finished - atom 3",
    "Stage finished - atom 4",
    "Stage finished - atom 5",
    "Stage finished - atom 6",
    "Stage finished - atom 7",
    "Stage finished - atom 8",
    "Stage finished - atom 9",
    "Stage finished - atom 10",
    "Stage finished - atom 11",
    "Stage finished - atom 12",
    "Stage finished - atom 13",
    "Stage finished - atom 14",
    "Stage finished - atom 15",
    "Stage finished - atom 16",
    "Stage finished - atom 17",
    "Stage finished - atom 18",
    "Stage finished - atom 19",
    "Stage finished - atom 20",
    "Stage finished - atom 21",
    "Stage finished - atom 22",
    "Stage finished - atom 23",
    "Stage finished - atom 24",
    "Stage finished - atom 25",
    "Stage finished - atom 26",
    "Stage finished - atom 27",
    "Stage finished - atom 28",
    "Stage finished - atom 29",
    "Stage finished - remiaining atoms",
    "-SENT MESSAGE STATISTICS",
    "Query plan",
    "Query plan acknowledged",
    "Partial bindings - atom 0",
    "Partial bindings - atom 1",
    "Partial bindings - atom 2",
    "Partial bindings - atom 3",
    "Partial bindings - atom 4",
    "Partial bindings - atom 5",
    "Partial bindings - atom 6",
    "Partial bindings - atom 7",
    "Partial bindings - atom 8",
    "Partial bindings - atom 9",
    "Partial bindings - atom 10",
    "Partial bindings - atom 11",
    "Partial bindings - atom 12",
    "Partial bindings - atom 13",
    "Partial bindings - atom 14",
    "Partial bindings - atom 15",
    "Partial bindings - atom 16",
    "Partial bindings - atom 17",
    "Partial bindings - atom 18",
    "Partial bindings - atom 19",
    "Partial bindings - atom 20",
    "Partial bindings - atom 21",
    "Partial bindings - atom 22",
    "Partial bindings - atom 23",
    "Partial bindings - atom 24",
    "Partial bindings - atom 25",
    "Partial bindings - atom 26",
    "Partial bindings - atom 27",
    "Partial bindings - atom 28",
    "Partial bindings - atom 29",
    "Partial bindings - remiaining atoms",
    "Stage finished - atom 0",
    "Stage finished - atom 1",
    "Stage finished - atom 2",
    "Stage finished - atom 3",
    "Stage finished - atom 4",
    "Stage finished - atom 5",
    "Stage finished - atom 6",
    "Stage finished - atom 7",
    "Stage finished - atom 8",
    "Stage finished - atom 9",
    "Stage finished - atom 10",
    "Stage finished - atom 11",
    "Stage finished - atom 12",
    "Stage finished - atom 13",
    "Stage finished - atom 14",
    "Stage finished - atom 15",
    "Stage finished - atom 16",
    "Stage finished - atom 17",
    "Stage finished - atom 18",
    "Stage finished - atom 19",
    "Stage finished - atom 20",
    "Stage finished - atom 21",
    "Stage finished - atom 22",
    "Stage finished - atom 23",
    "Stage finished - atom 24",
    "Stage finished - atom 25",
    "Stage finished - atom 26",
    "Stage finished - atom 27",
    "Stage finished - atom 28",
    "Stage finished - atom 29",
    "Stage finished - remiaining atoms"
};

NodeServerStatistics::NodeServerStatistics() :
    m_statisticsCounters(NUMBER_OF_COUNTERS, s_counterDescriptions, 2)
{
}

void NodeServerStatistics::reset() {
    m_statisticsCounters.reset();
}

void NodeServerStatistics::queryProcessingStarted(const QueryID queryID, const char* const queryText, const size_t queryTextLength) {
    ::atomicIncrement(m_statisticsCounters[0][0]);
}

void NodeServerStatistics::queryAtomMatched(const QueryID queryID, const size_t threadIndex) {
    ::atomicIncrement(m_statisticsCounters[0][2]);
}

void NodeServerStatistics::queryAnswerProduced(const QueryID queryID, const size_t threadIndex, const ResourceValueCache& resourceValueCache, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& answerArgumentIndexes, const size_t multiplicity) {
    ::atomicIncrement(m_statisticsCounters[0][3]);
}

void NodeServerStatistics::messageProcessingStarted(const QueryID queryID, const size_t threadIndex, const MessageType messageType, const uint8_t* const message, const size_t messageSize) {
    size_t descriptionIndex;
    if (messageType == QUERY_PLAN_MESSAGE)
        descriptionIndex = PROCESSED_QUERY_PLAN;
    else if (messageType == QUERY_PLAN_ACKNOWLEDGED_MESSAGE)
        descriptionIndex = PROCESSED_QUERY_PLAN_ACKNOWLEDGED;

    else if (PARTIAL_BINDINGS_0_MESSAGE <= messageType && messageType < PARTIAL_BINDINGS_0_MESSAGE + STAGES_TO_COUNT)
        descriptionIndex = PROCESSED_PARTIAL_BINDINGS_0 + (messageType - PARTIAL_BINDINGS_0_MESSAGE);
    else if (PARTIAL_BINDINGS_0_MESSAGE + STAGES_TO_COUNT <= messageType && messageType <= PARTIAL_BINDINGS_LAST_MESSAGE)
        descriptionIndex = PROCESSED_PARTIAL_BINDINGS_REST;

    else if (STAGE_FINISHED_0_MESSAGE <= messageType && messageType < STAGE_FINISHED_0_MESSAGE + STAGES_TO_COUNT)
        descriptionIndex = PROCESSED_STAGE_FINISHED_0 + (messageType - STAGE_FINISHED_0_MESSAGE);
    else if (STAGE_FINISHED_0_MESSAGE + STAGES_TO_COUNT <= messageType && messageType <= STAGE_FINISHED_LAST_MESSAGE)
        descriptionIndex = PROCESSED_STAGE_FINISHED_REST;

    else
        throw RDF_STORE_EXCEPTION("Invalid message type.");

    ::atomicIncrement(m_statisticsCounters[0][descriptionIndex]);
    ::atomicAdd(m_statisticsCounters[1][descriptionIndex], messageSize);
}

void NodeServerStatistics::messageProcessingFinished(const QueryID queryID, const size_t threadIndex) {
}

void NodeServerStatistics::messageSent(const QueryID queryID, const size_t threadIndex, const NodeID destinationNodeID, const MessageType messageType, const uint8_t* const message, const size_t messageSize) {
    size_t descriptionIndex;
    if (messageType == QUERY_PLAN_MESSAGE)
        descriptionIndex = SENT_QUERY_PLAN;
    else if (messageType == QUERY_PLAN_ACKNOWLEDGED_MESSAGE)
        descriptionIndex = SENT_QUERY_PLAN_ACKNOWLEDGED;

    else if (PARTIAL_BINDINGS_0_MESSAGE <= messageType && messageType < PARTIAL_BINDINGS_0_MESSAGE + STAGES_TO_COUNT)
        descriptionIndex = SENT_PARTIAL_BINDINGS_0 + (messageType - PARTIAL_BINDINGS_0_MESSAGE);
    else if (PARTIAL_BINDINGS_0_MESSAGE + STAGES_TO_COUNT <= messageType && messageType <= PARTIAL_BINDINGS_LAST_MESSAGE)
        descriptionIndex = SENT_PARTIAL_BINDINGS_REST;

    else if (STAGE_FINISHED_0_MESSAGE <= messageType && messageType < STAGE_FINISHED_0_MESSAGE + STAGES_TO_COUNT)
        descriptionIndex = SENT_STAGE_FINISHED_0 + (messageType - STAGE_FINISHED_0_MESSAGE);
    else if (STAGE_FINISHED_0_MESSAGE + STAGES_TO_COUNT <= messageType && messageType <= STAGE_FINISHED_LAST_MESSAGE)
        descriptionIndex = SENT_STAGE_FINISHED_REST;

    else
        throw RDF_STORE_EXCEPTION("Invalid message type.");

    ::atomicIncrement(m_statisticsCounters[0][descriptionIndex]);
    ::atomicAdd(m_statisticsCounters[1][descriptionIndex], messageSize);
}

void NodeServerStatistics::queryProcessingFinished(const QueryID queryID, const size_t threadIndex) {
    ::atomicIncrement(m_statisticsCounters[0][1]);
}

void NodeServerStatistics::queryProcessingAborted(const QueryID queryID) {
}
