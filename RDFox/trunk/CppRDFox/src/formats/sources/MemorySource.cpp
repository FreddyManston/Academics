// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "MemorySource.h"

void MemorySource::loadNextBlock() {
}

MemorySource::MemorySource(const uint8_t* const data, const size_t dataSize) :
    InputSource(data, data + dataSize, false),
    m_data(data)
{
}

MemorySource::MemorySource(const char* const data, const size_t dataSize) : MemorySource(reinterpret_cast<const uint8_t*>(data), dataSize) {
}

void MemorySource::rewind() {
    m_currentByte = m_data;
}

std::unique_ptr<InputSource::Position> MemorySource::createPosition() {
    return std::unique_ptr<Position>(new MemorySourcePosition);
}

void MemorySource::savePosition(Position& position) const {
    static_cast<MemorySourcePosition&>(position).m_currentByte = m_currentByte;
}

void MemorySource::restorePosition(const Position& position) {
    m_currentByte = static_cast<const MemorySourcePosition&>(position).m_currentByte;
}
