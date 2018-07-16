// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef MEMORYSOURCE_H_
#define MEMORYSOURCE_H_

#include "../../all.h"
#include "../InputSource.h"

class MemorySource : public InputSource {

protected:

    const uint8_t* const m_data;

    struct MemorySourcePosition : public Position {
        const uint8_t* m_currentByte;
    };

    virtual void loadNextBlock();

public:

    MemorySource(const uint8_t* const data, const size_t dataSize);

    MemorySource(const char* const data, const size_t dataSize);

    virtual void rewind();

    virtual std::unique_ptr<Position> createPosition();

    virtual void savePosition(Position& position) const;

    virtual void restorePosition(const Position& position);

};

#endif // MEMORYSOURCE_H_
