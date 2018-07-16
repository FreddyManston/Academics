// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef QUERYEVALUATORIMPL_H_
#define QUERYEVALUATORIMPL_H_

#include "../RDFStoreException.h"
#include "../querying/TermArray.h"
#include "../dictionary/Dictionary.h"
#include "../dictionary/ResourceValueCache.h"
#include "../storage/DataStore.h"
#include "../storage/TupleTable.h"
#include "../storage/TupleIterator.h"
#include "MessageBufferImpl.h"
#include "QueryEvaluator.h"
#include "QueryInfo.h"
#include "OccurrenceManagerImpl.h"
#include "NodeServerImpl.h"
#include "NodeServerMonitor.h"
#include "NodeServerQueryListener.h"

always_inline ImmutableOccurrenceSet QueryEvaluator::getOccurrenceSetFor(const ArgumentIndex argumentIndex, const uint8_t position) {
    if (m_processingPartialBindingsFor->m_occrrenceSetsToSendByArgumentIndex[argumentIndex])
        return getTargetOccurrenceSet(argumentIndex, position);
    else
        return m_occurrenceManager.getOccurrenceSet(m_argumentsBuffer[argumentIndex], position);
}

always_inline OccurrenceSet QueryEvaluator::getTargetOccurrenceSet(const ArgumentIndex argumentIndex, const uint8_t position) {
    return m_occurrenceSets.data() + argumentIndex * m_occurrenceManager.getResourceWidth() + position * m_occurrenceManager.getSetWidth();
}

template<bool callMonitor>
always_inline void QueryEvaluator::sendPartialBindingsMessage(const uint8_t conjunctIndex, const NodeID destinationNodeID, const size_t multiplicity) {
    m_queryInfo.getNodeServer().ensureLevelNotPaused<callMonitor>(m_threadIndex, conjunctIndex);
    MessageBuffer& outgoingMessageBuffer = m_queryInfo.getNodeServer().getOutgoingMessageBuffer(destinationNodeID, m_threadIndex);
    uint8_t* const partialBindingsMessage = outgoingMessageBuffer.startMessage();
    if (multiplicity == 1)
        outgoingMessageBuffer.write(::getMessageHeader(static_cast<MessageType>(PARTIAL_BINDINGS_0_MESSAGE + conjunctIndex) | MULTIPLICITY_ONE_MESSAGE_TYPE, m_queryInfo.getQueryID()));
    else {
        outgoingMessageBuffer.write(::getMessageHeader(static_cast<MessageType>(PARTIAL_BINDINGS_0_MESSAGE + conjunctIndex), m_queryInfo.getQueryID()));
        outgoingMessageBuffer.write(static_cast<uint32_t>(multiplicity));
    }
    QueryInfo::ConjunctInfo& conjunctInfo = *m_queryInfo.m_conjunctInfos[conjunctIndex];
    for (auto iterator = conjunctInfo.m_occurrenceSetsToSend.begin(); iterator != conjunctInfo.m_occurrenceSetsToSend.end(); ++iterator)
        m_occurrenceManager.copyAll(getOccurrenceSetFor(*iterator, 0), outgoingMessageBuffer.moveRaw(m_occurrenceManager.getResourceWidth()));
    for (auto iterator = conjunctInfo.m_resourceValuesToSend.begin(); iterator != conjunctInfo.m_resourceValuesToSend.end(); ++iterator) {
        const ArgumentIndex argumentIndex = iterator->first;
        const ResourceID localResourceID = m_argumentsBuffer[argumentIndex];
        // The following code is hacked to use 4 bytes for resource IDs in the messages. This should be fixed later.
        if (iterator->second || !ResourceValueCache::isPermanentValue(localResourceID)) {
            outgoingMessageBuffer.write(static_cast<uint32_t>(INVALID_RESOURCE_ID));
            ResourceValue resourceValue;
            if (!m_resourceValueCache.getResource(localResourceID, resourceValue)) {
                std::ostringstream message;
                message << "Cannot resolve resource ID '" << localResourceID << "'.";
                throw RDF_STORE_EXCEPTION(message.str());
            }
            outgoingMessageBuffer.writeResourceValue(resourceValue);
        }
        else if (NodeServerQueryListener::isGlobalResourceID(localResourceID))
            outgoingMessageBuffer.write(static_cast<uint32_t>(localResourceID & 0x3fffffffffffffffULL));
        else {
            const ResourceID globalResourceID = m_resourceIDMapper.getGlobalResourceID(localResourceID);
            if (globalResourceID == INVALID_RESOURCE_ID) {
                std::ostringstream message;
                message << "Cannot resolve local resource ID '" << localResourceID << "' to global resource ID.";
                throw RDF_STORE_EXCEPTION(message.str());
            }
            outgoingMessageBuffer.write(static_cast<uint32_t>(globalResourceID));
        }
    }
    const size_t partialBindingsMessageSize = outgoingMessageBuffer.finishMessage(partialBindingsMessage) - partialBindingsMessage;
    ::atomicIncrement(m_queryInfo.m_conjunctInfos[conjunctIndex]->m_partialAnswersISentToOthers[destinationNodeID]);
    if (callMonitor)
        m_queryInfo.getNodeServer().m_nodeServerMonitor->messageSent(m_queryInfo.getQueryID(), m_threadIndex, destinationNodeID, static_cast<MessageType>(PARTIAL_BINDINGS_0_MESSAGE + conjunctIndex), partialBindingsMessage, partialBindingsMessageSize);
}

template<bool callMonitor>
always_inline void QueryEvaluator::sendStageFinishedMessage(const uint8_t conjunctIndex, const NodeID destinationNodeID) {
    m_queryInfo.getNodeServer().ensureLevelNotPaused<callMonitor>(m_threadIndex, conjunctIndex);
    const uint32_t partialAnswersISentToNode = ::atomicRead(m_queryInfo.m_conjunctInfos[conjunctIndex]->m_partialAnswersISentToOthers[destinationNodeID]);
    MessageBuffer& outgoingMessageBuffer = m_queryInfo.getNodeServer().getOutgoingMessageBuffer(destinationNodeID, m_threadIndex);
    uint8_t* const stageFinishedMessageStart = outgoingMessageBuffer.startMessage();
    outgoingMessageBuffer.write(::getMessageHeader(static_cast<MessageType>(STAGE_FINISHED_0_MESSAGE + conjunctIndex), m_queryInfo.getQueryID()));
    outgoingMessageBuffer.write(partialAnswersISentToNode);
    const size_t stageFinishedMessageSize = outgoingMessageBuffer.finishMessage(stageFinishedMessageStart) - stageFinishedMessageStart;
    if (callMonitor)
        m_queryInfo.getNodeServer().m_nodeServerMonitor->messageSent(m_queryInfo.getQueryID(), m_threadIndex, destinationNodeID, static_cast<MessageType>(STAGE_FINISHED_0_MESSAGE + conjunctIndex), stageFinishedMessageStart, stageFinishedMessageSize);
}

always_inline void QueryEvaluator::readPartialBindingsMessage(ThreadContext& threadContext, IncomingMessage& incomingMessage, const uint8_t conjunctIndex, size_t& multiplicity) {
    if ((incomingMessage.getMessageType() & MULTIPLICITY_ONE_MESSAGE_TYPE) == 0)
        multiplicity = static_cast<size_t>(incomingMessage.read<uint32_t>());
    else
        multiplicity = 1;
    m_processingPartialBindingsFor = m_queryInfo.m_conjunctInfos[conjunctIndex].get();
    for (auto iterator = m_processingPartialBindingsFor->m_occurrenceSetsToSend.begin(); iterator != m_processingPartialBindingsFor->m_occurrenceSetsToSend.end(); ++iterator)
        m_occurrenceManager.copyAll(incomingMessage.moveRaw(m_occurrenceManager.getResourceWidth()), getTargetOccurrenceSet(*iterator, 0));
    for (auto iterator = m_processingPartialBindingsFor->m_resourceValuesToSend.begin(); iterator != m_processingPartialBindingsFor->m_resourceValuesToSend.end(); ++iterator) {
        const ArgumentIndex argumentIndex = iterator->first;
        // The following code is hacked to use 4 bytes for resource IDs. This should be fixed later.
        const ResourceID globalResourceID = static_cast<ResourceID>(incomingMessage.read<uint32_t>());
        if (globalResourceID == INVALID_RESOURCE_ID) {
            ResourceValue resourceValue;
            incomingMessage.readResourceValue(resourceValue);
            m_argumentsBuffer[argumentIndex] = m_resourceValueCache.resolveResource(threadContext, resourceValue);
            // If this was a value invented somewhere else but that we own, then load our occurrence sets so that this gets propagated further.
            if (ResourceValueCache::isPermanentValue(m_argumentsBuffer[argumentIndex]))
                m_occurrenceManager.copyAll(m_occurrenceManager.getOccurrenceSet(m_argumentsBuffer[argumentIndex], 0), getTargetOccurrenceSet(argumentIndex, 0));
        }
        else {
            // The following avoids resolving IDs in messages that contain final query answers.
            ResourceID localResourceID;
            if (conjunctIndex == m_afterLastQueryConjunctIndex || (localResourceID = m_resourceIDMapper.getLocalResourceID(threadContext, globalResourceID)) == INVALID_RESOURCE_ID)
                m_argumentsBuffer[argumentIndex] = globalResourceID | 0x4000000000000000ULL;
            else
                m_argumentsBuffer[argumentIndex] = localResourceID;
        }
    }
}

template<bool callMonitor>
always_inline void QueryEvaluator::processPartialBindingsMessage(const uint8_t conjunctIndex, const size_t multiplicity) {
    if (m_queryInfo.getMyNodeID() == m_queryInfo.getOriginatorNodeID() || !m_queryInfo.m_conjunctInfos[conjunctIndex]->m_isBuiltin)
        matchNext<callMonitor>(conjunctIndex, multiplicity);
    ::atomicIncrement(m_queryInfo.m_conjunctInfos[conjunctIndex]->m_partialAnswersIProcessed);
    checkStageFinished<callMonitor>(conjunctIndex);
}

template<bool callMonitor>
always_inline void QueryEvaluator::checkStageFinished(const uint8_t conjunctIndex) {
    QueryInfo::ConjunctInfo& conjunctInfo = *m_queryInfo.m_conjunctInfos[conjunctIndex];
    if (!::atomicRead(conjunctInfo.m_finished) && ::atomicRead(conjunctInfo.m_partialAnswersIProcessed) == ::atomicRead(conjunctInfo.m_partialAnswersOthersSentMe) && ::atomicRead(conjunctInfo.m_nodesSentTermination) == m_queryInfo.getNumberOfNodes() && !::atomicExchange(conjunctInfo.m_finished, 1)) {
        if (conjunctIndex == m_afterLastQueryConjunctIndex) {
            m_queryInfo.m_nodeServerQueryListener->queryAnswersComplete();
            if (callMonitor)
                m_queryInfo.getNodeServer().m_nodeServerMonitor->queryProcessingFinished(m_queryInfo.getQueryID(), m_threadIndex);
            ::atomicWrite(m_queryInfo.m_queryFinished, true);
        }
        else if (conjunctIndex == m_lastQueryConjunctIndex) {
            sendStageFinishedMessage<callMonitor>(conjunctIndex + 1, m_queryInfo.getOriginatorNodeID());
            if (m_queryInfo.getOriginatorNodeID() != m_queryInfo.getMyNodeID())
                ::atomicWrite(m_queryInfo.m_queryFinished, true);
        }
        else {
            for (NodeID nodeID = 0; nodeID < m_queryInfo.getNumberOfNodes(); ++nodeID)
                sendStageFinishedMessage<callMonitor>(conjunctIndex + 1, nodeID);
        }
    }
}

#endif // QUERYEVALUATORIMPL_H_
