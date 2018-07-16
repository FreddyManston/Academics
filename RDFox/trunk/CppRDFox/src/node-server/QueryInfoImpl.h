// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef QUERYINFOIMPL_H_
#define QUERYINFOIMPL_H_

#include "../formats/turtle/SPARQLParser.h"
#include "QueryInfo.h"
#include "QueryEvaluatorImpl.h"
#include "NodeServer.h"
#include "NodeServerMonitor.h"

always_inline QueryEvaluator& QueryInfo::getQueryEvaluator(const size_t threadIndex, const uint8_t conjunctIndex) {
    assert(m_queryEvauatorsByThreadAndConjunct[threadIndex * m_numberOfConjunctsPlusOne + conjunctIndex]->m_threadIndex == threadIndex);
    return *m_queryEvauatorsByThreadAndConjunct[threadIndex * m_numberOfConjunctsPlusOne + conjunctIndex];
}

template<bool callMonitor>
always_inline void QueryInfo::processQueryPlanMessage(ThreadContext& threadContext, IncomingMessage& incomingMessage, const size_t threadIndex) {
    size_t queryTextLength;
    const char* const queryText = incomingMessage.readString(queryTextLength);
    Prefixes prefixes;
    SPARQLParser parser(prefixes);
    LogicFactory factory(::newLogicFactory());
    Query query = parser.parse(factory, queryText, queryTextLength);
    m_nodeServer.markProcessed(incomingMessage);
    loadQueryPlan(query, nullptr);
    // Send the acknowledgement.
    const NodeID destinationNodeID = ::getNodeID(m_queryID);
    assert(destinationNodeID != m_myNodeID);
    MessageBuffer& outgoingMessageBuffer = m_nodeServer.getOutgoingMessageBuffer(destinationNodeID, threadIndex);
    uint8_t* queryPlanAcknowledgedMessageStart = outgoingMessageBuffer.startMessage();
    outgoingMessageBuffer.write(::getMessageHeader(QUERY_PLAN_ACKNOWLEDGED_MESSAGE, m_queryID));
    const size_t queryPlanAcknowledgedMessageSize = outgoingMessageBuffer.finishMessage(queryPlanAcknowledgedMessageStart) - queryPlanAcknowledgedMessageStart;
    if (callMonitor)
        m_nodeServer.m_nodeServerMonitor->messageSent(m_queryID, threadIndex, destinationNodeID, QUERY_PLAN_ACKNOWLEDGED_MESSAGE, queryPlanAcknowledgedMessageStart, queryPlanAcknowledgedMessageSize);
}

template<bool callMonitor>
always_inline void QueryInfo::processQueryPlanAcknowledgedMessage(ThreadContext& threadContext, IncomingMessage& incomingMessage, const size_t threadIndex) {
    m_nodeServer.markProcessed(incomingMessage);
    if (::atomicIncrement(m_numberOfQueryPlanAcknowledgements) == m_numberOfNodes) {
        QueryEvaluator& queryEvaluator = getQueryEvaluator(threadIndex, 0);
        for (NodeID nodeID = 0; nodeID < m_numberOfNodes; ++nodeID)
            queryEvaluator.sendPartialBindingsMessage<callMonitor>(0, nodeID, 1);
    }
}

template<bool callMonitor>
always_inline void QueryInfo::processStageFinishedMessage(ThreadContext& threadContext, IncomingMessage& incomingMessage, const size_t threadIndex) {
    const uint8_t conjunctIndex = static_cast<uint8_t>((incomingMessage.getMessageType() & MESSAGE_TYPE_MASK) - STAGE_FINISHED_0_MESSAGE);
    const size_t sentMessageCount = static_cast<size_t>(incomingMessage.read<uint32_t>());
    m_nodeServer.markProcessed(incomingMessage);
    ConjunctInfo& conjunctInfo = *m_conjunctInfos[conjunctIndex];
    ::atomicAdd(conjunctInfo.m_partialAnswersOthersSentMe, sentMessageCount);
    ::atomicIncrement(conjunctInfo.m_nodesSentTermination);
    getQueryEvaluator(threadIndex, conjunctIndex).checkStageFinished<callMonitor>(conjunctIndex);
}

template<bool callMonitor>
always_inline void QueryInfo::processPartialBindingsMessage(ThreadContext& threadContext, IncomingMessage& incomingMessage, const size_t threadIndex) {
    const uint8_t conjunctIndex = static_cast<uint8_t>((incomingMessage.getMessageType() & MESSAGE_TYPE_MASK) - PARTIAL_BINDINGS_0_MESSAGE);
    QueryEvaluator& queryEvaluator = getQueryEvaluator(threadIndex, conjunctIndex);
    size_t multiplicity;
    queryEvaluator.readPartialBindingsMessage(threadContext, incomingMessage, conjunctIndex, multiplicity);
    m_nodeServer.markProcessed(incomingMessage);
    queryEvaluator.processPartialBindingsMessage<callMonitor>(conjunctIndex, multiplicity);
}

template<bool callMonitor>
always_inline void QueryInfo::dispatchMessage(ThreadContext& threadContext, IncomingMessage& incomingMessage, const size_t threadIndex) {
    const MessageType messageType = (incomingMessage.getMessageType() & MESSAGE_TYPE_MASK);
    assert(messageType == QUERY_PLAN_MESSAGE || hasQueryPlan());
    if (callMonitor)
        m_nodeServer.m_nodeServerMonitor->messageProcessingStarted(m_queryID, threadIndex, messageType, incomingMessage.getMessageStart(), incomingMessage.getMessageSize());
    if (messageType == QUERY_PLAN_MESSAGE)
        processQueryPlanMessage<callMonitor>(threadContext, incomingMessage, threadIndex);
    else if (messageType == QUERY_PLAN_ACKNOWLEDGED_MESSAGE)
        processQueryPlanAcknowledgedMessage<callMonitor>(threadContext, incomingMessage, threadIndex);
    else if (PARTIAL_BINDINGS_0_MESSAGE <= messageType && messageType <= PARTIAL_BINDINGS_LAST_MESSAGE)
        processPartialBindingsMessage<callMonitor>(threadContext, incomingMessage, threadIndex);
    else if (STAGE_FINISHED_0_MESSAGE <= messageType && messageType <= STAGE_FINISHED_LAST_MESSAGE)
        processStageFinishedMessage<callMonitor>(threadContext, incomingMessage, threadIndex);
    else
        throw RDF_STORE_EXCEPTION("Invalid message type.");
    m_nodeServer.flushOutgoingMessages(threadIndex);
    if (callMonitor)
        m_nodeServer.m_nodeServerMonitor->messageProcessingFinished(m_queryID, threadIndex);
}

#endif // QUERYINFOIMPL_H_
