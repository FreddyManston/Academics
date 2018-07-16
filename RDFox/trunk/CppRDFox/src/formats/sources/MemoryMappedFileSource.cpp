// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "MemoryMappedFileSource.h"

void MemoryMappedFileSource::loadNextBlock() {
    m_currentBlockOffset += m_currentBlockSize;
    if (m_currentBlockOffset  + m_mapBlockSize <= m_fileSize) {
        m_currentBlockSize = m_mapBlockSize;
        m_hasMoreBlocks = true;
    }
    else {
        m_currentBlockSize = m_fileSize - m_currentBlockOffset;
        m_hasMoreBlocks = false;
    }
    if (m_currentBlockSize == 0) {
        m_memoryMappedFileView.unmapView();
        m_currentByte = m_afterBlockEnd = 0;
    }
    else {
        m_memoryMappedFileView.mapView(m_currentBlockOffset, m_currentBlockSize);
        m_currentByte = reinterpret_cast<const uint8_t*>(m_memoryMappedFileView.getMappedData());
        m_afterBlockEnd = m_currentByte + m_currentBlockSize;
    }
}

MemoryMappedFileSource::MemoryMappedFileSource(File& file, const size_t blockSize) :
    InputSource(0, 0, false),
    m_memoryMappedFileView(),
    m_mapBlockSize((blockSize / ::getAllocationGranularity()) * ::getAllocationGranularity()),
    m_fileSize(file.getSize()),
    m_currentBlockOffset(0),
    m_currentBlockSize(0)
{
    m_memoryMappedFileView.open(file);
    if (m_fileSize != 0)
        loadNextBlock();
}

void MemoryMappedFileSource::rewind() {
    m_currentBlockOffset = 0;
    m_currentBlockSize = 0;
    m_currentByte = 0;
    m_afterBlockEnd = 0;
    if (m_fileSize != 0)
        loadNextBlock();
}

std::unique_ptr<InputSource::Position> MemoryMappedFileSource::createPosition() {
    return std::unique_ptr<Position>(new MemoryMappedFileSourcePosition);
}

void MemoryMappedFileSource::savePosition(Position& position) const {
    MemoryMappedFileSourcePosition& memoryMappedFileSourcePosition = static_cast<MemoryMappedFileSourcePosition&>(position);
    memoryMappedFileSourcePosition.m_currentBlockOffset = m_currentBlockOffset;
    memoryMappedFileSourcePosition.m_currentByteOffset = (m_currentByte - reinterpret_cast<uint8_t*>(m_memoryMappedFileView.getMappedData()));
}

void MemoryMappedFileSource::restorePosition(const Position& position) {
    const MemoryMappedFileSourcePosition& memoryMappedFileSourcePosition = static_cast<const MemoryMappedFileSourcePosition&>(position);
    if (m_currentBlockOffset != memoryMappedFileSourcePosition.m_currentBlockOffset) {
        m_currentBlockOffset = memoryMappedFileSourcePosition.m_currentBlockOffset;
        if (m_currentBlockOffset  + m_mapBlockSize > m_fileSize) {
            m_currentBlockSize = m_mapBlockSize;
            m_hasMoreBlocks = true;
        }
        else {
            m_currentBlockSize = m_fileSize - m_currentBlockOffset;
            m_hasMoreBlocks = false;
        }
        m_memoryMappedFileView.mapView(m_currentBlockOffset, m_currentBlockSize);
        m_afterBlockEnd = reinterpret_cast<const uint8_t*>(m_memoryMappedFileView.getMappedData()) + m_currentBlockSize;
    }
    m_currentByte = reinterpret_cast<const uint8_t*>(m_memoryMappedFileView.getMappedData()) + memoryMappedFileSourcePosition.m_currentByteOffset;
}
