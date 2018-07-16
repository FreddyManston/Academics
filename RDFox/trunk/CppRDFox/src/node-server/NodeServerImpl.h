// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef NODESERVERIMPL_H_
#define NODESERVERIMPL_H_

#include "NodeServer.h"
#include "MessagePassingImpl.h"

const size_t OUTGOING_BUFFER_WRITE_THRESHOLD = 100 * 1024;

template<bool callMonitor>
void NodeServer::ensureLevelNotPaused(const size_t threadIndex, const uint8_t conjunctIndex) {
    if (levelPaused(conjunctIndex)) {
        flushOutgoingMessages(threadIndex);
        MutexHolder mutexHolder(m_mutex);
        messageLoopNoLock<callMonitor, true>(threadIndex, conjunctIndex + 1);
    }
}

always_inline MessageBuffer& NodeServer::getOutgoingMessageBuffer(const NodeID nodeID, const size_t threadIndex) {
    assert(nodeID < getNumberOfNodes());
    assert(threadIndex < m_numberOfThreads);
    MessageBuffer& outgoingMessageBuffer = *m_outgoingMessageBuffers[threadIndex * getNumberOfNodes() + nodeID];
    const size_t filledSize = outgoingMessageBuffer.getFilledSize();
    if (filledSize > OUTGOING_BUFFER_WRITE_THRESHOLD) {
        sendData(nodeID, outgoingMessageBuffer.getBufferStart(), filledSize);
        outgoingMessageBuffer.clear();
    }
    return outgoingMessageBuffer;
}

always_inline void NodeServer::flushOutgoingMessages(const size_t threadIndex) {
    for (NodeID nodeID = 0; nodeID < getNumberOfNodes(); ++nodeID) {
        MessageBuffer& outgoingMessageBuffer = getOutgoingMessageBuffer(nodeID, threadIndex);
        if (outgoingMessageBuffer.hasUnprocessedData()) {
            sendData(nodeID, outgoingMessageBuffer.getBufferStart(), outgoingMessageBuffer.getFilledSize());
            outgoingMessageBuffer.clear();
        }
    }
}

#endif // NODESERVERIMPL_H_
