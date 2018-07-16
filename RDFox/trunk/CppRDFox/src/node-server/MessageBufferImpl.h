// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef MESSAGEBUFFERIMPL_H_
#define MESSAGEBUFFERIMPL_H_

#include "../RDFStoreException.h"
#include "MessageBuffer.h"

// Message

always_inline MessageSize Message::getMessageSize() const {
    return *reinterpret_cast<MessageSize*>(m_messageStart);
}

always_inline uint8_t* Message::getMessageStart() const {
    return m_messageStart;
}

always_inline const uint8_t* Message::moveRaw(const size_t size) {
    const uint8_t* const valueLocation = m_nextReadPosition;
    m_nextReadPosition += size;
    assert(m_nextReadPosition <= m_messageStart + getMessageSize());
    return valueLocation;
}

template<typename T>
always_inline T Message::read() {
    uint8_t* const valueLocation = alignPointer<T>(m_nextReadPosition);
    m_nextReadPosition = valueLocation + sizeof(T);
    assert(valueLocation + sizeof(T) <= m_messageStart + getMessageSize());
    return *reinterpret_cast<const T*>(valueLocation);
}

always_inline const char* Message::readString(size_t& length) {
    const uint8_t* const valueLocation = m_nextReadPosition;
    assert(m_nextReadPosition < m_messageStart + getMessageSize());
    while (*m_nextReadPosition != 0) {
        ++m_nextReadPosition;
        assert(m_nextReadPosition < m_messageStart + getMessageSize());
    }
    length = (m_nextReadPosition++) - valueLocation;
    return reinterpret_cast<const char*>(valueLocation);
}

always_inline void Message::readString(std::string& string) {
    size_t length;
    string.replace(string.begin(), string.end(), readString(length));
}

always_inline void Message::readResourceValue(ResourceValue& resourceValue) {
    const size_t dataSize = read<size_t>();
    const uint8_t* data = dataSize == 0 ? nullptr : moveRaw(dataSize);
    const DatatypeID datatypeID = read<DatatypeID>();
    resourceValue.setDataRaw(datatypeID, data, dataSize);
}

// MessageBuffer

always_inline bool MessageBuffer::invariants() {
    uint8_t* const firstUnprocessedLocation = ::atomicRead(m_firstUnprocessedLocation);
    uint8_t* const firstUnextractedLocation = ::atomicRead(m_firstUnextractedLocation);
    uint8_t* const nextAppendLocation = ::atomicRead(m_nextAppendLocation);
    if (reinterpret_cast<uintptr_t>(firstUnprocessedLocation) % sizeof(MessageStartAlignmentType) != 0)
        return false;
    if (reinterpret_cast<uintptr_t>(firstUnextractedLocation) % sizeof(MessageStartAlignmentType) != 0)
        return false;
    if (reinterpret_cast<uintptr_t>(nextAppendLocation) % sizeof(MessageStartAlignmentType) != 0)
        return false;
    if (m_buffer.getData() > nextAppendLocation)
        return false;
    if (m_buffer.getData() > firstUnextractedLocation)
        return false;
    if (m_buffer.getData() > firstUnprocessedLocation)
        return false;
    return true;
}

always_inline bool MessageBuffer::isInitialized() const {
    return m_buffer.isInitialized();
}

always_inline bool MessageBuffer::hasUnprocessedData() const {
    return ::atomicRead(m_firstUnprocessedLocation) != ::atomicRead(m_nextAppendLocation);
}

always_inline void MessageBuffer::clear() {
    m_firstUnprocessedLocation = m_firstUnextractedLocation = m_nextAppendLocation = m_buffer.getData();
}

always_inline bool MessageBuffer::extractMessage(Message& message) {
    assert(invariants());
    // First read m_firstUnextractedLocation.
    message.m_messageStart = ::atomicRead(m_firstUnextractedLocation);
    // Read m_nextAppendLocation after m_firstUnextractedLocation. If message.m_messageStart < m_nextAppendLocation holds at this point,
    // then we've got a message. Note that m_nextAppendLocation can be reset before it is read below.
    if (message.m_messageStart >= ::atomicRead(m_nextAppendLocation))
        return false;
    const MessageSize messageSize = message.getMessageSize();
    assert((messageSize & MESSAGE_PROCESSED_FLAG) == 0);
    message.m_nextReadPosition = message.m_messageStart + sizeof(MessageSize);
    ::atomicWrite(m_firstUnextractedLocation, message.m_messageStart + messageSize);
    assert(invariants());
    return true;
}

always_inline void MessageBuffer::messageProcessed(Message& message) {
    assert((reinterpret_cast<uintptr_t>(message.m_messageStart) % sizeof(MessageStartAlignmentType)) == 0);
    const MessageSize messageSize = message.getMessageSize();
    assert((messageSize & MessageBuffer::MESSAGE_PROCESSED_FLAG) == 0);
    ::atomicWrite(*reinterpret_cast<MessageSize*>(const_cast<uint8_t*>(message.m_messageStart)), messageSize | MessageBuffer::MESSAGE_PROCESSED_FLAG);
}

always_inline bool MessageBuffer::reclaimProcessedSpace() {
    assert(invariants());
    MessageSize messageSize;
    while (m_firstUnprocessedLocation < m_nextAppendLocation && ((messageSize = *reinterpret_cast<MessageSize*>(m_firstUnprocessedLocation)) & MESSAGE_PROCESSED_FLAG) != 0) {
        m_firstUnprocessedLocation = m_firstUnprocessedLocation + (messageSize & ~MESSAGE_PROCESSED_FLAG);
        assert(invariants());
    }
    if (m_firstUnprocessedLocation == m_nextAppendLocation) {
        ::atomicWrite(m_nextAppendLocation, m_buffer.getData());
        ::atomicWrite(m_firstUnextractedLocation, m_buffer.getData());
        ::atomicWrite(m_firstUnprocessedLocation, m_buffer.getData());
        assert(invariants());
        return true;
    }
    else
        return false;
}

always_inline void MessageBuffer::appendData(const uint8_t* const startLocation, const size_t dataSize) {
    assert(invariants());
    uint8_t* newNextAppendLocation = m_nextAppendLocation + dataSize;
    if (!m_buffer.ensureEndElementAtLeast(newNextAppendLocation, 0))
        throw RDF_STORE_EXCEPTION("Out of memory while appending a message.");
    std::memcpy(m_nextAppendLocation, startLocation, dataSize);
    m_nextAppendLocation = newNextAppendLocation;
    assert(invariants());
}

always_inline bool MessageBuffer::isOverLimit() const {
    return ::atomicRead(m_nextAppendLocation) >= m_fullLocation;
}

always_inline const uint8_t* MessageBuffer::getBufferStart() const {
    return m_buffer.getData();
}

always_inline size_t MessageBuffer::getFilledSize() const {
    return ::atomicRead(m_nextAppendLocation) - m_buffer.getData();
}

always_inline uint8_t* MessageBuffer::startMessage() {
    assert(invariants());
    assert((reinterpret_cast<uintptr_t>(m_nextAppendLocation) % sizeof(MessageStartAlignmentType)) == 0);
    uint8_t* const messageStart = m_nextAppendLocation;
    m_nextAppendLocation += sizeof(MessageSize);
    return messageStart;
}

always_inline uint8_t* MessageBuffer::finishMessage(uint8_t* const messageStart) {
    assert((reinterpret_cast<uintptr_t>(messageStart) % sizeof(MessageStartAlignmentType)) == 0);
    assert(messageStart + sizeof(MessageSize) <= m_nextAppendLocation);
    m_nextAppendLocation = alignPointer<MessageStartAlignmentType>(m_nextAppendLocation);
    *reinterpret_cast<MessageSize*>(messageStart) = static_cast<MessageSize>(m_nextAppendLocation - messageStart);
    assert(invariants());
    return m_nextAppendLocation;
}

always_inline uint8_t* MessageBuffer::moveRaw(const size_t size) {
    uint8_t* valueLocation = m_nextAppendLocation;
    uint8_t* newNextAppendLocation = valueLocation + size;
    if (!m_buffer.ensureEndElementAtLeast(newNextAppendLocation, 0))
        throw RDF_STORE_EXCEPTION("Out of memory while appending a message.");
    m_nextAppendLocation = newNextAppendLocation;
    return valueLocation;
}

template<typename T>
always_inline void MessageBuffer::write(const T value) {
    uint8_t* valueLocation = alignPointer<T>(m_nextAppendLocation);
    uint8_t* newNextAppendLocation = valueLocation + sizeof(T);
    if (!m_buffer.ensureEndElementAtLeast(newNextAppendLocation, 0))
        throw RDF_STORE_EXCEPTION("Out of memory while appending a message.");
    *reinterpret_cast<T*>(valueLocation) = value;
    m_nextAppendLocation = newNextAppendLocation;
}

always_inline void MessageBuffer::writeString(const std::string& string) {
    const size_t stringLength = string.length();
    ::memcpy(moveRaw(stringLength + 1), string.c_str(), stringLength + 1);
}

always_inline void MessageBuffer::writeString(const char* const string) {
     const size_t stringLength = ::strlen(string);
    ::memcpy(moveRaw(stringLength + 1), string, stringLength + 1);
}

always_inline void MessageBuffer::writeResourceValue(const ResourceValue& resourceValue) {
    write(resourceValue.getDataSize());
    if (resourceValue.getDataSize() != 0)
        ::memcpy(moveRaw(resourceValue.getDataSize()), resourceValue.getDataRaw(), resourceValue.getDataSize());
    write(resourceValue.getDatatypeID());
}

#endif // MESSAGEBUFFERIMPL_H_
