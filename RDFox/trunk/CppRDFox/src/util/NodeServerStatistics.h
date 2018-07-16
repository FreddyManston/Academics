// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef NODESERVERSTATISTICS_H_
#define NODESERVERSTATISTICS_H_

#include "../node-server/NodeServerMonitor.h"
#include "StatisticsCounters.h"

class ResourceValueCache;

class NodeServerStatistics : public NodeServerMonitor {

protected:

    StatisticsCounters m_statisticsCounters;

public:

    NodeServerStatistics();

    always_inline const StatisticsCounters& getStatisticsCounters() const {
        return m_statisticsCounters;
    }

    virtual void reset();

    virtual void queryProcessingStarted(const QueryID queryID, const char* const queryText, const size_t queryTextLength);

    virtual void queryAtomMatched(const QueryID queryID, const size_t threadIndex);

    virtual void queryAnswerProduced(const QueryID queryID, const size_t threadIndex, const ResourceValueCache& resourceValueCache, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& answerArgumentIndexes, const size_t multiplicity);

    virtual void messageProcessingStarted(const QueryID queryID, const size_t threadIndex, const MessageType messageType, const uint8_t* const message, const size_t messageSize);

    virtual void messageProcessingFinished(const QueryID queryID, const size_t threadIndex);

    virtual void messageSent(const QueryID queryID, const size_t threadIndex, const NodeID destinationNodeID, const MessageType messageType, const uint8_t* const message, const size_t messageSize);

    virtual void queryProcessingFinished(const QueryID queryID, const size_t threadIndex);

    virtual void queryProcessingAborted(const QueryID queryID);

};

#endif /* NODESERVERSTATISTICS_H_ */
