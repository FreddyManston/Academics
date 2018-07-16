// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "MessageBufferImpl.h"

MessageBuffer::MessageBuffer(MemoryManager& memoryManager, const size_t bufferSize) :
    m_bufferSize(bufferSize & ~(sizeof(uint64_t) - 1)),
    m_buffer(memoryManager, 3),
    m_fullLocation(nullptr),
    m_nextAppendLocation(nullptr),
    m_firstUnextractedLocation(nullptr),
    m_firstUnprocessedLocation(nullptr)
{
    assert((m_bufferSize % sizeof(uint64_t)) == 0);
}

void MessageBuffer::initialize() {
    if (!m_buffer.initializeLarge())
        throw RDF_STORE_EXCEPTION("Cannot initialize the message buffer.");
    m_fullLocation = m_buffer.getData() + m_bufferSize;
    m_firstUnprocessedLocation = m_firstUnextractedLocation = m_nextAppendLocation = m_buffer.getData();
}

void MessageBuffer::deinitialize() {
    m_buffer.deinitialize();
    m_fullLocation = m_nextAppendLocation = m_firstUnextractedLocation = m_firstUnprocessedLocation = nullptr;
}
