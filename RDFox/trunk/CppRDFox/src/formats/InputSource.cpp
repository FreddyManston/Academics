// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "InputSource.h"

// InputSource::Position

InputSource::Position::~Position() {
}

// InputSource

InputSource::InputSource(const uint8_t* const currentByte, const uint8_t* afterBlockEnd, const bool hasMoreBlocks) :
    m_currentByte(currentByte),
    m_afterBlockEnd(afterBlockEnd),
    m_hasMoreBlocks(hasMoreBlocks)
{
}

InputSource::~InputSource() {
}
