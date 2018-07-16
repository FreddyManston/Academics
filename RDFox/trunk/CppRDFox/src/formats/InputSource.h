// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef INPUTSOURCE_H_
#define INPUTSOURCE_H_

#include "../all.h"

class InputSource : private Unmovable {

protected:

    const uint8_t* m_currentByte;
    const uint8_t* m_afterBlockEnd;
    bool m_hasMoreBlocks;

    virtual void loadNextBlock() = 0;

    InputSource(const uint8_t* const currentByte, const uint8_t* afterBlockEnd, const bool hasMoreBlocks);

public:

    struct Position {

        virtual ~Position();
    };

    virtual ~InputSource();

    virtual void rewind() = 0;

    virtual std::unique_ptr<Position> createPosition() = 0;

    virtual void savePosition(Position& position) const = 0;

    virtual void restorePosition(const Position& position) = 0;

    always_inline bool isAtEOF() const {
        return m_currentByte == m_afterBlockEnd && !m_hasMoreBlocks;
    }

    always_inline void advanceByOne() {
        ++m_currentByte;
        if (m_currentByte == m_afterBlockEnd && m_hasMoreBlocks)
            loadNextBlock();
    }

    always_inline uint8_t getCurrentByte() const {
        return *m_currentByte;
    }

};

#endif // INPUTSOURCE_H_
