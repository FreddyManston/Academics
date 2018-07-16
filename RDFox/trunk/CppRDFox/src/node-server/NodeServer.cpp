// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "../RDFStoreException.h"
#include "../logic/Logic.h"
#include "../formats/turtle/SPARQLParser.h"
#include "../storage/DataStore.h"
#include "../util/ThreadContext.h"
#include "QueryInfoImpl.h"
#include "NodeServerImpl.h"
#include "NodeServerQueryListener.h"
#include "NodeServerListener.h"
#include "MessagePassingImpl.h"
#include "ResourceIDMapperImpl.h"

// NodeServerThread

class NodeServerThread : public Thread {

protected:

    NodeServer& m_nodeServer;
    const size_t m_threadIndex;

public:

    NodeServerThread(NodeServer& nodeServer, const size_t threadIndex);

    template<bool callMonitor>
    void processMessages();

    virtual void run();
    
};

NodeServerThread::NodeServerThread(NodeServer& nodeServer, const size_t threadIndex) :
    Thread(true),
    m_nodeServer(nodeServer),
    m_threadIndex(threadIndex)
{
}

template<bool callMonitor>
always_inline void NodeServerThread::processMessages() {
    MutexHolder mutexHolder(m_nodeServer.m_mutex);
    try {
        m_nodeServer.messageLoopNoLock<callMonitor, false>(m_threadIndex, 0);
        --m_nodeServer.m_numberOfRunningWorkers;
    }
    catch (const std::exception& e) {
        --m_nodeServer.m_numberOfRunningWorkers;
        m_nodeServer.stopNoLock();
        if (m_nodeServer.m_nodeServerListener != nullptr)
            m_nodeServer.m_nodeServerListener->nodeServerError(e);
    }
    m_nodeServer.m_condition.signalAll();
}

void NodeServerThread::run() {
    if (m_nodeServer.m_nodeServerMonitor == nullptr)
        processMessages<false>();
    else
        processMessages<true>();
}

// NodeServer

always_inline uint32_t NodeServer::getMessageBufferIndex(const uint8_t* messagePayloadStart) {
    const MessageType messageType = getMessageType(*reinterpret_cast<const MessageHeader*>(messagePayloadStart)) & MESSAGE_TYPE_MASK;
    if (messageType == QUERY_PLAN_MESSAGE || messageType == QUERY_PLAN_ACKNOWLEDGED_MESSAGE)
        return 0;
    else if (PARTIAL_BINDINGS_0_MESSAGE <= messageType && messageType <= PARTIAL_BINDINGS_LAST_MESSAGE)
        return messageType - PARTIAL_BINDINGS_0_MESSAGE;
    else
        return messageType - STAGE_FINISHED_0_MESSAGE;
}

always_inline void NodeServer::messagePassingStarting() {
    if (m_nodeServerListener != nullptr)
        m_nodeServerListener->nodeServerStarting();
}

always_inline void NodeServer::messagePassingConnected() {
    if (m_nodeServerListener != nullptr)
        m_nodeServerListener->nodeServerReady();
}

always_inline void NodeServer::messagePassingStopping() {
    if (m_nodeServerListener != nullptr)
        m_nodeServerListener->nodeServerFinishing();
}

always_inline void NodeServer::messagePassingChannelsStopped() {
    while (m_numberOfRunningWorkers != 0)
        m_condition.wait(m_mutex);
    for (auto iterator = m_queryInfosByID.begin(); iterator != m_queryInfosByID.end(); ++iterator) {
        QueryInfo& queryInfo = *iterator->second;
        if (queryInfo.m_nodeServerQueryListener != nullptr)
            queryInfo.m_nodeServerQueryListener->queryInterrupted();
    }
    m_queryInfosByID.clear();
    for (auto iterator = m_outgoingMessageBuffers.begin(); iterator != m_outgoingMessageBuffers.end(); ++iterator)
        (*iterator)->clear();
}

always_inline void NodeServer::messagePassingError(const std::exception& error) {
    if (m_nodeServerListener != nullptr)
        m_nodeServerListener->nodeServerError(error);
}

void NodeServer::markProcessed(IncomingMessage& incomingMessage) {
    incomingMessage.m_incomingDataInfo->m_messageBuffer.messageProcessed(incomingMessage);
    // If a message was processed on a paused peer, we interrupt the socket selector to force
    // it to run gargabe collection, and the latter must be done on the network thread.
    if (incomingMessage.m_incomingDataInfo->m_peersPaused)
        m_socketSelector.interrupt();
}

template<bool callMonitor, bool checkLevel>
void NodeServer::messageLoopNoLock(const size_t threadIndex, const uint32_t minLevelToProcess) {
    ThreadContext& threadContext = ThreadContext::getCurrentThreadContext();
    const uint32_t levelToCheck = minLevelToProcess - 1;
    std::unique_ptr<IncomingMessage[]> incomingMessages(new IncomingMessage[m_messageBatchSize]);
    IncomingMessage* const afterLastIncomingMessage = incomingMessages.get() + m_messageBatchSize;
    while (m_state >= CONNECTING && (!checkLevel || levelPaused(levelToCheck))) {
        IncomingMessage* afterLastExtractedMessage = incomingMessages.get();
        QueryInfo* lastQueryInfo = nullptr;
        for (auto iterator = m_incomingDataInfos.rbegin(); afterLastExtractedMessage < afterLastIncomingMessage && iterator != m_incomingDataInfos.rend() && (!checkLevel || (*iterator)->m_level >= minLevelToProcess); ++iterator) {
            MessageBuffer* messageBuffer = &(*iterator)->m_messageBuffer;
            while (afterLastExtractedMessage < afterLastIncomingMessage && messageBuffer->extractMessage(*afterLastExtractedMessage)) {
                afterLastExtractedMessage->m_incomingDataInfo = iterator->get();
                const MessageHeader messageHeader = afterLastExtractedMessage->read<MessageHeader>();
                const QueryID queryID = ::getQueryID(messageHeader);
                if (lastQueryInfo == nullptr || lastQueryInfo->getQueryID() != queryID)
                    lastQueryInfo = &getQueryInfoNoLock(queryID);
                afterLastExtractedMessage->m_queryInfo = lastQueryInfo;
                afterLastExtractedMessage->m_queryInfo->addUser();
                afterLastExtractedMessage->m_messageType = ::getMessageType(messageHeader);
                ++afterLastExtractedMessage;
            }
        }
        if (afterLastExtractedMessage == incomingMessages.get())
            m_condition.wait(m_mutex);
        else {
            {
                MutexReleaser mutexReleaser(m_mutex);
                for (IncomingMessage* currentIncomingMessage = incomingMessages.get(); currentIncomingMessage < afterLastExtractedMessage; ++currentIncomingMessage)
                    currentIncomingMessage->getQueryInfo().dispatchMessage<callMonitor>(threadContext, *currentIncomingMessage, threadIndex);
            }
            for (IncomingMessage* currentIncomingMessage = incomingMessages.get(); currentIncomingMessage < afterLastExtractedMessage; ++currentIncomingMessage) {
                if (currentIncomingMessage->getQueryInfo().removeUser() && currentIncomingMessage->getQueryInfo().isQueryFinished()) {
                    const QueryID queryID = currentIncomingMessage->getQueryInfo().getQueryID();
                    auto iterator = m_queryInfosByID.find(queryID);
                    assert(iterator != m_queryInfosByID.end());
                    m_queryInfosByID.erase(iterator);
                }
            }
        }
    }
}

always_inline QueryInfo& NodeServer::getQueryInfoNoLock(const QueryID queryID) {
    std::unique_ptr<QueryInfo>& queryInfo = m_queryInfosByID[queryID];
    if (queryInfo.get() == nullptr)
        queryInfo.reset(new QueryInfo(*this, queryID));
    return *queryInfo;
}

template<bool callMonitor>
always_inline void NodeServer::doAnswerQuery(const char* const queryText, const size_t queryTextLength, NodeServerQueryListener& nodeServerQueryListener) {
    Prefixes prefixes;
    SPARQLParser parser(prefixes);
    LogicFactory factory(::newLogicFactory());
    const Query query = parser.parse(factory, queryText, queryTextLength);
    MutexHolder mutexHolder(m_mutex);
    if (m_state != RUNNING)
        throw RDF_STORE_EXCEPTION("Node server is not ready to process queries.");
    QueryInfo* queryInfo = nullptr;
    try {
        const QueryID queryID = ::getQueryID(getMyNodeID(), m_nextLocalQueryID++);
        queryInfo = &getQueryInfoNoLock(queryID);
        queryInfo->loadQueryPlan(query, &nodeServerQueryListener);
        // Notify the monitor
        if (callMonitor)
            m_nodeServerMonitor->queryProcessingStarted(queryID, queryText, queryTextLength);
        // Prepare the plan message for all other nodes.
        MessageBuffer outgoingMessageBuffer(m_dataStore.getMemoryManager(), queryTextLength + 100);
        outgoingMessageBuffer.initialize();
        uint8_t* const queryPlanMessageStart = outgoingMessageBuffer.startMessage();
        outgoingMessageBuffer.write(::getMessageHeader(QUERY_PLAN_MESSAGE, queryID));
        outgoingMessageBuffer.writeString(queryText);
        const size_t queryPlanMessageSize = outgoingMessageBuffer.finishMessage(queryPlanMessageStart) - queryPlanMessageStart;
        // Send the start message to everybody else.
        for (uint8_t nodeID = 0; nodeID < getNumberOfNodes(); ++nodeID) {
            if (nodeID != getMyNodeID()) {
                if (!sendData(nodeID, queryPlanMessageStart, queryPlanMessageSize)) {
                    std::ostringstream message;
                    message << "Cannot send the query plan to node '" << static_cast<uint32_t>(nodeID) << "'.";
                    throw RDF_STORE_EXCEPTION(message.str());
                }
                if (callMonitor)
                    m_nodeServerMonitor->messageSent(queryID, m_numberOfThreads, nodeID, QUERY_PLAN_MESSAGE, queryPlanMessageStart, queryPlanMessageSize);
            }
        }
        // Acknowledge the query plan to ourselves. We are holding the mutex so we can write into the buffer without problems.
        outgoingMessageBuffer.clear();
        uint8_t* startLocalQueryMessageStart = outgoingMessageBuffer.startMessage();
        outgoingMessageBuffer.write(::getMessageHeader(QUERY_PLAN_ACKNOWLEDGED_MESSAGE, queryID));
        const size_t startLocalQueryMessageSize = outgoingMessageBuffer.finishMessage(startLocalQueryMessageStart) - startLocalQueryMessageStart;
        if (!sendData(getMyNodeID(), startLocalQueryMessageStart, startLocalQueryMessageSize)) {
            std::ostringstream message;
            message << "Cannot send the query plan acknowledgement to myself..";
            throw RDF_STORE_EXCEPTION(message.str());
        }
        if (callMonitor)
            m_nodeServerMonitor->messageSent(queryID, m_numberOfThreads, getMyNodeID(), QUERY_PLAN_ACKNOWLEDGED_MESSAGE, startLocalQueryMessageStart, startLocalQueryMessageSize);
    }
    catch (...) {
        if (queryInfo != nullptr) {
            nodeServerQueryListener.queryInterrupted();
            const QueryID queryID = queryInfo->getQueryID();
            auto queryInfoIterator = m_queryInfosByID.find(queryID);
            if (queryInfoIterator != m_queryInfosByID.end())
                m_queryInfosByID.erase(queryInfoIterator);
            if (callMonitor)
                m_nodeServerMonitor->queryProcessingAborted(queryID);
        }
        throw;
    }
}

NodeServer::NodeServer(const size_t numberOfThreads, const uint8_t numberOfNodes, const NodeID myNodeID, const size_t totalSizePerMessageBuffer, const size_t messageBatchSize, DataStore& dataStore, NodeServerListener* const nodeServerListener, NodeServerMonitor* const nodeServerMonitor) :
    MessagePassing<NodeServer>(dataStore.getMemoryManager(), numberOfNodes, myNodeID, NODE_SERVER_MAX_NUMBER_OF_LITERALS, totalSizePerMessageBuffer),
    m_numberOfThreads(numberOfThreads),
    m_messageBatchSize(messageBatchSize),
    m_dataStore(dataStore),
    m_nodeServerListener(nodeServerListener),
    m_nodeServerMonitor(nodeServerMonitor),
    m_occurrenceManager(m_dataStore.getMemoryManager(), numberOfNodes, 3),
    m_resourceIDMapper(m_dataStore.getMemoryManager()),
    m_outgoingMessageBuffers(),
    m_numberOfRunningWorkers(0),
    m_queryInfosByID(),
    m_nextLocalQueryID(1)
{
    if (m_dataStore.getEqualityAxiomatizationType() != EQUALITY_AXIOMATIZATION_OFF)
        throw RDF_STORE_EXCEPTION("Distributed reasoning is currently not supported for RDF stores that use rewriting.");
    m_occurrenceManager.initialize();
    m_resourceIDMapper.initialize();
    const size_t numberOfBuffers = m_numberOfThreads * getNumberOfNodes();
    for (size_t bufferIndex = 0; bufferIndex < numberOfBuffers; ++bufferIndex) {
        m_outgoingMessageBuffers.push_back(std::unique_ptr<MessageBuffer>(new MessageBuffer(m_dataStore.getMemoryManager(), 2 * OUTGOING_BUFFER_WRITE_THRESHOLD)));
        m_outgoingMessageBuffers.back()->initialize();
    }
}

NodeServer::~NodeServer() {
    stopServer();
}

uint8_t NodeServer::getNumberOfNodes() const {
    return MessagePassingType::getNumberOfChannels();
}

NodeID NodeServer::getMyNodeID() const {
    return MessagePassingType::getMyChannelID();
}

NodeServer::State NodeServer::getState() {
    return MessagePassingType::getState();
}

void NodeServer::setNodeInfo(const NodeID nodeID, const std::string& nodeName, const std::string& serviceName) {
    MessagePassingType::setChannelNodeServiceName(nodeID, nodeName, serviceName);
}

bool NodeServer::loadOccurrencesFromInputSource(Prefixes& prefixes, InputSource& inputSource, const bool loadOnlyOwnedResources) {
    MutexHolder mutexHolder(m_mutex);
    if (m_state == NOT_RUNNING) {
        m_occurrenceManager.loadFromInputSource(prefixes, inputSource, m_dataStore.getDictionary(), m_resourceIDMapper, loadOnlyOwnedResources ? getMyNodeID() : OccurrenceManager::LOAD_RESOURCES_FOR_ALL_NODES);
        return true;
    }
    else
        return false;
}

void NodeServer::answerQuery(const char* const queryText, const size_t queryTextLength, NodeServerQueryListener& nodeServerQueryListener) {
    if (m_nodeServerMonitor == nullptr)
        doAnswerQuery<false>(queryText, queryTextLength, nodeServerQueryListener);
    else
        doAnswerQuery<true>(queryText, queryTextLength, nodeServerQueryListener);
}

bool NodeServer::startServer(const Duration startupTime) {
    MutexHolder mutexHolder(m_mutex);
    if (MessagePassingType::startNoLock(startupTime)) {
        try {
            for (size_t threadIndex = 0; threadIndex < m_numberOfThreads; ++threadIndex) {
                (new NodeServerThread(*this, threadIndex))->start();
                ++m_numberOfRunningWorkers;
            }
            return true;
        }
        catch (...) {
            MessagePassingType::stop();
            throw;
        }
    }
    return false;
}

bool NodeServer::waitForReady() {
    return MessagePassingType::waitForReady();
}

std::exception_ptr NodeServer::stopServer() {
    return MessagePassingType::stop();
}

template void NodeServer::messageLoopNoLock<false, false>(const size_t threadIndex, const uint32_t minLevelToProcess);
template void NodeServer::messageLoopNoLock<false, true>(const size_t threadIndex, const uint32_t minLevelToProcess);
template void NodeServer::messageLoopNoLock<true, false>(const size_t threadIndex, const uint32_t minLevelToProcess);
template void NodeServer::messageLoopNoLock<true, true>(const size_t threadIndex, const uint32_t minLevelToProcess);
