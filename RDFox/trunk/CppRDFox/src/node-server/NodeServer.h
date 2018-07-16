// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef NODESERVER_H_
#define NODESERVER_H_

#include "../Common.h"
#include "OccurrenceManager.h"
#include "ResourceIDMapper.h"
#include "MessagePassing.h"
#include "QueryInfo.h"

class DataStore;
class QueryInfo;
class QueryEvaluator;
class NodeServerQueryListener;
class NodeServerListener;
class NodeServerMonitor;

// IncomingMessage

class IncomingMessage : public Message {

    friend class NodeServer;

protected:

    MessagePassing<NodeServer>::IncomingDataInfo* m_incomingDataInfo;
    MessageType m_messageType;
    QueryInfo* m_queryInfo;

public:

    IncomingMessage() : Message(), m_incomingDataInfo(nullptr), m_messageType(0), m_queryInfo(nullptr) {
    }

    always_inline MessageType getMessageType() const {
        return m_messageType;
    }

    always_inline QueryInfo& getQueryInfo() const {
        return *m_queryInfo;
    }

};

// NodeServer

class NodeServer : protected MessagePassing<NodeServer> {

    friend class QueryInfo;
    friend class QueryEvaluator;
    friend class NodeServerThread;
    friend class MessageProcessor;
    friend class MessagePassing<NodeServer>;

protected:

    const size_t m_numberOfThreads;
    const size_t m_messageBatchSize;
    DataStore& m_dataStore;
    NodeServerListener* const m_nodeServerListener;
    NodeServerMonitor* const m_nodeServerMonitor;
    OccurrenceManager m_occurrenceManager;
    ResourceIDMapper m_resourceIDMapper;
    unique_ptr_vector<MessageBuffer> m_outgoingMessageBuffers;
    size_t m_numberOfRunningWorkers;
    std::unordered_map<QueryID, std::unique_ptr<QueryInfo> > m_queryInfosByID;
    uint16_t m_nextLocalQueryID;

    uint32_t getMessageBufferIndex(const uint8_t* messagePayloadStart);

    void messagePassingStarting();

    void messagePassingConnected();

    void messagePassingStopping();

    void messagePassingChannelsStopped();

    void messagePassingError(const std::exception& error);

    void markProcessed(IncomingMessage& incomingMessage);

    template<bool callMonitor, bool checkLevel>
    void messageLoopNoLock(const size_t threadIndex, const uint32_t minLevelToProcess);

    QueryInfo& getQueryInfoNoLock(const QueryID queryID);

    template<bool callMonitor>
    void ensureLevelNotPaused(const size_t threadIndex, const uint8_t conjunctIndex);

    MessageBuffer& getOutgoingMessageBuffer(const NodeID nodeID, const size_t threadIndex);

    void flushOutgoingMessages(const size_t threadIndex);
    
    template<bool callMonitor>
    void doAnswerQuery(const char* const queryText, const size_t queryTextLength, NodeServerQueryListener& nodeServerQueryListener);

public:

    using MessagePassingType::State;
    using MessagePassingType::NOT_RUNNING;
    using MessagePassingType::CONNECTING;
    using MessagePassingType::RUNNING;
    using MessagePassingType::STOPPING;

    NodeServer(const size_t numberOfThreads, const uint8_t numberOfNodes, const NodeID myNodeID, const size_t totalSizePerMessageBuffer, const size_t messageBatchSize, DataStore& dataStore, NodeServerListener* const nodeServerListener, NodeServerMonitor* const nodeServerMonitor);

    ~NodeServer();

    always_inline DataStore& getDataStore() {
        return m_dataStore;
    }

    always_inline OccurrenceManager& getOccurrenceManager() {
        return m_occurrenceManager;
    }

    always_inline ResourceIDMapper& getResourceIDMapper() {
        return m_resourceIDMapper;
    }

    uint8_t getNumberOfNodes() const;

    NodeID getMyNodeID() const;

    State getState();

    void setNodeInfo(const NodeID nodeID, const std::string& nodeName, const std::string& serviceName);

    bool loadOccurrencesFromInputSource(Prefixes& prefixes, InputSource& inputSource, const bool loadOnlyOwnedResources);

    void answerQuery(const char* const queryText, const size_t queryTextLength, NodeServerQueryListener& nodeServerQueryListener);

    bool startServer(const Duration startupTime);

    bool waitForReady();

    std::exception_ptr stopServer();

};

#endif // NODESERVER_H_
