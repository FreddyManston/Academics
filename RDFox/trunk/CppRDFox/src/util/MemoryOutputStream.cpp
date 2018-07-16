// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "MemoryOutputStream.h"

MemoryOutputStream::MemoryOutputStream(std::string& buffer) : m_buffer(buffer) {
}

void MemoryOutputStream::flush() {
}

size_t MemoryOutputStream::write(const void* const data, const size_t numberOfBytesToWrite) {
    m_buffer.insert(m_buffer.length(), reinterpret_cast<const char*>(data), numberOfBytesToWrite);
    return numberOfBytesToWrite;
}
