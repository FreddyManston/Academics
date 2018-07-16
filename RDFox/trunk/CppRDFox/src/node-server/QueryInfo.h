// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef QUERYINFO_H_
#define QUERYINFO_H_

#include "../Common.h"
#include "../logic/Logic.h"
#include "../util/MemoryRegion.h"
#include "../util/Mutex.h"
#include "MessageBuffer.h"

class Socket;
class QueryEvaluator;
class DataStore;
class NodeServerQueryListener;
class NodeServer;
class IncomingMessage;
class ThreadContext;
class ResourceValueCache;

typedef uint8_t MessageType;

const uint8_t NODE_SERVER_MAX_NUMBER_OF_LITERALS = 50;

const MessageType QUERY_PLAN_MESSAGE = 1;
const MessageType QUERY_PLAN_ACKNOWLEDGED_MESSAGE = 2;
const MessageType PARTIAL_BINDINGS_0_MESSAGE = 3;
const MessageType PARTIAL_BINDINGS_LAST_MESSAGE = PARTIAL_BINDINGS_0_MESSAGE + NODE_SERVER_MAX_NUMBER_OF_LITERALS - 1;
const MessageType STAGE_FINISHED_0_MESSAGE = PARTIAL_BINDINGS_LAST_MESSAGE + 1;
const MessageType STAGE_FINISHED_LAST_MESSAGE = STAGE_FINISHED_0_MESSAGE + NODE_SERVER_MAX_NUMBER_OF_LITERALS - 1;
const MessageType MULTIPLICITY_ONE_MESSAGE_TYPE = 0x80;
const MessageType MESSAGE_TYPE_MASK = 0x7f;

typedef uint32_t MessageHeader;

always_inline MessageType getMessageType(const MessageHeader messageHeader) {
    return static_cast<MessageType>(messageHeader >> (8 * (sizeof(MessageHeader) - sizeof(MessageType))));
}

always_inline MessageHeader getMessageHeader(const MessageType messageType, const QueryID queryID) {
    return (static_cast<MessageHeader>(messageType) << (8 * (sizeof(MessageHeader) - sizeof(MessageType)))) | static_cast<MessageHeader>(queryID);
}

always_inline QueryID getQueryID(const MessageHeader messageHeader) {
    return static_cast<QueryID>(messageHeader & 0x00ffffff);
}
class QueryInfo {

    friend class QueryEvaluator;
    friend class NodeServer;
    friend class NodeServerThread;

public:

    struct ConjunctInfo : private Unmovable {
        aligned_size_t m_partialAnswersIProcessed;
        aligned_size_t m_partialAnswersOthersSentMe;
        aligned_uint32_t m_nodesSentTermination;
        aligned_uint32_t m_finished;
        std::unique_ptr<uint32_t[]> m_partialAnswersISentToOthers;
        bool m_isBuiltin;
        std::vector<std::pair<ArgumentIndex, uint8_t> > m_conjunctArgumentsBoundBeforeConjunctIsMatched;
        std::vector<std::pair<ArgumentIndex, bool> > m_resourceValuesToSend;
        std::vector<ArgumentIndex> m_occurrenceSetsToSend;
        std::vector<bool> m_occrrenceSetsToSendByArgumentIndex;

        ConjunctInfo(const size_t numberOfNodes, const size_t numberOfTerms);

        ~ConjunctInfo();

        __ALIGNED(ConjunctInfo)

    };

protected:

    NodeServer& m_nodeServer;
    aligned_uint32_t m_numberOfQueryPlanAcknowledgements;
    const uint8_t m_numberOfNodes;
    const QueryID m_queryID;
    const NodeID m_originatorNodeID;
    const NodeID m_myNodeID;
    size_t m_numberOfConjunctsPlusOne;
    size_t m_numberOfUsers;
    NodeServerQueryListener* m_nodeServerQueryListener;
    std::vector<ArgumentIndex> m_answerArgumentIndexes;
    unique_ptr_vector<ConjunctInfo> m_conjunctInfos;
    unique_ptr_vector<ResourceValueCache> m_resourceValueCaches;
    unique_ptr_vector<QueryEvaluator> m_queryEvauatorsByThreadAndConjunct;
    bool m_queryFinished;

    always_inline void addUser() {
        ++m_numberOfUsers;
    }

    always_inline bool removeUser() {
        return (--m_numberOfUsers) == 0;
    }

    QueryEvaluator& getQueryEvaluator(const size_t threadIndex, const uint8_t conjunctIndex);

    template<bool callMonitor>
    void processQueryPlanMessage(ThreadContext& threadContext, IncomingMessage& incomingMessage, const size_t threadIndex);

    template<bool callMonitor>
    void processQueryPlanAcknowledgedMessage(ThreadContext& threadContext, IncomingMessage& incomingMessage, const size_t threadIndex);

    template<bool callMonitor>
    void processStageFinishedMessage(ThreadContext& threadContext, IncomingMessage& incomingMessage, const size_t threadIndex);

    template<bool callMonitor>
    void processPartialBindingsMessage(ThreadContext& threadContext, IncomingMessage& incomingMessage, const size_t threadIndex);

public:

    QueryInfo(NodeServer& nodeServer, const QueryID queryID);

    ~QueryInfo();

    always_inline QueryID getQueryID() const {
        return m_queryID;
    }

    always_inline NodeServer& getNodeServer() {
        return m_nodeServer;
    }

    always_inline size_t getNumberOfNodes() const {
        return m_numberOfNodes;
    }

    always_inline NodeID getOriginatorNodeID() const {
        return m_originatorNodeID;
    }

    always_inline NodeID getMyNodeID() const {
        return m_myNodeID;
    }

    always_inline bool hasQueryPlan() const {
        return !m_conjunctInfos.empty();
    }

    always_inline bool isQueryFinished() const {
        return ::atomicRead(m_queryFinished);
    }

    always_inline const std::vector<ArgumentIndex>& getAnswerArgumentIndexes() const {
        return m_answerArgumentIndexes;
    }

    template<bool callMonitor>
    void dispatchMessage(ThreadContext& threadContext, IncomingMessage& incomingMessage, const size_t threadIndex);

    void loadQueryPlan(const Query& query, NodeServerQueryListener* const nodeServerQueryListener);

    __ALIGNED(QueryInfo)

};

#endif // QUERYINFO_H_
