// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "BufferedOutputStream.h"

BufferedOutputStream::BufferedOutputStream(OutputStream& outputStream, const size_t bufferSize) :
    m_outputStream(outputStream),
    m_buffer(new uint8_t[bufferSize]),
    m_bufferSize(bufferSize),
    m_nextFreePosition(m_buffer.get()),
    m_freeBytesLeft(m_bufferSize)
{
}

BufferedOutputStream::~BufferedOutputStream() {
}

void BufferedOutputStream::flush() {
    if (m_nextFreePosition != m_buffer.get()) {
        m_outputStream.writeExactly(m_buffer.get(), m_nextFreePosition - m_buffer.get());
        m_nextFreePosition = m_buffer.get();
        m_freeBytesLeft = m_bufferSize;
    }
    m_outputStream.flush();
}

size_t BufferedOutputStream::write(const void* const data, const size_t numberOfBytesToWrite) {
    if (m_nextFreePosition == m_buffer.get() && numberOfBytesToWrite >= m_bufferSize)
        return m_outputStream.write(data, numberOfBytesToWrite);
    const size_t bytesCopiedIntoBuffer = std::min(numberOfBytesToWrite, m_freeBytesLeft);
    ::memcpy(m_nextFreePosition, data, bytesCopiedIntoBuffer);
    if (bytesCopiedIntoBuffer < m_freeBytesLeft) {
        m_nextFreePosition += bytesCopiedIntoBuffer;
        m_freeBytesLeft -= bytesCopiedIntoBuffer;
        return bytesCopiedIntoBuffer;
    }
    m_outputStream.writeExactly(m_buffer.get(), m_bufferSize);
    m_nextFreePosition = m_buffer.get();
    m_freeBytesLeft = m_bufferSize;
    if (bytesCopiedIntoBuffer == numberOfBytesToWrite)
        return bytesCopiedIntoBuffer;
    const size_t bytesLeftToWrite = numberOfBytesToWrite - bytesCopiedIntoBuffer;
    const uint8_t* const remainingData = reinterpret_cast<const uint8_t*>(data) + bytesCopiedIntoBuffer;
    if (bytesLeftToWrite >= m_bufferSize)
        return m_outputStream.write(remainingData, bytesLeftToWrite) + bytesCopiedIntoBuffer;
    ::memcpy(m_nextFreePosition, remainingData, bytesLeftToWrite);
    m_nextFreePosition += bytesLeftToWrite;
    m_freeBytesLeft -= bytesLeftToWrite;
    return numberOfBytesToWrite;
}
