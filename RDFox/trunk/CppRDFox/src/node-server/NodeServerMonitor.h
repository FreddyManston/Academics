// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef NODESERVERMONITOR_H_
#define NODESERVERMONITOR_H_

#include "../Common.h"
#include "QueryInfo.h"

class ResourceValueCache;

class NodeServerMonitor {

public:

    virtual ~NodeServerMonitor() {
    }

    virtual void queryProcessingStarted(const QueryID queryID, const char* const queryText, const size_t queryTextLength) = 0;

    virtual void queryAtomMatched(const QueryID queryID, const size_t threadIndex) = 0;

    virtual void queryAnswerProduced(const QueryID queryID, const size_t threadIndex, const ResourceValueCache& resourceValueCache, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& answerArgumentIndexes, const size_t multiplicity) = 0;

    virtual void messageProcessingStarted(const QueryID queryID, const size_t threadIndex, const MessageType messageType, const uint8_t* const message, const size_t messageSize) = 0;

    virtual void messageProcessingFinished(const QueryID queryID, const size_t threadIndex) = 0;

    virtual void messageSent(const QueryID queryID, const size_t threadIndex, const NodeID destinationNodeID, const MessageType messageType, const uint8_t* const message, const size_t messageSize) = 0;

    virtual void queryProcessingFinished(const QueryID queryID, const size_t threadIndex) = 0;

    virtual void queryProcessingAborted(const QueryID queryID) = 0;

};

#endif // NODESERVERMONITOR_H_
