// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef INPUTSTREAMSOURCE_H_
#define INPUTSTREAMSOURCE_H_

#include "../../all.h"
#include "../../RDFStoreException.h"
#include "../InputSource.h"

template<class IST>
class InputStreamSource : public InputSource {

public:

    typedef IST InputStreamType;

protected:

    struct Block {
        size_t m_blockNumber;
        std::unique_ptr<uint8_t[]> m_buffer;
        const uint8_t* m_afterBlockEnd;

        always_inline Block() : m_blockNumber(0), m_buffer(), m_afterBlockEnd(0) {
        }
    };

    InputStreamType m_inputStream;
    const size_t m_blockSize;
    Block m_blocks[2];
    size_t m_nextBlockIndexToLoad;
    size_t m_nextBlockNumberToLoad;
    size_t m_currentBlockIndex;
    size_t m_currentBlockNumber;

    struct InputStreamSourcePosition : public Position {
        size_t m_currentBlockNumber;
        size_t m_currentByteOffset;
    };

    virtual void loadNextBlock();

public:

    template<class ISIT>
    InputStreamSource(ISIT& inputStreamInitializer, const size_t blockSize);

    virtual void rewind();

    virtual std::unique_ptr<Position> createPosition();

    virtual void savePosition(Position& position) const;

    virtual void restorePosition(const Position& position);

};

template<class IST>
void InputStreamSource<IST>::loadNextBlock() {
    if (m_hasMoreBlocks) {
        ++m_currentBlockNumber;
        // Due to position restore operations, we need to find the block with the right number.
        if (m_blocks[0].m_blockNumber == m_currentBlockNumber)
            m_currentBlockIndex = 0;
        else if (m_blocks[1].m_blockNumber == m_currentBlockNumber)
            m_currentBlockIndex = 1;
        else {
            Block& block = m_blocks[m_nextBlockIndexToLoad];
            const size_t bytesRead = m_inputStream.read(block.m_buffer.get(), m_blockSize);
            block.m_afterBlockEnd = block.m_buffer.get() + bytesRead;
            block.m_blockNumber = m_nextBlockNumberToLoad++;
            m_currentBlockIndex = m_nextBlockIndexToLoad;
            m_nextBlockIndexToLoad = 1 - m_nextBlockIndexToLoad;
            if (bytesRead == 0)
                m_hasMoreBlocks = false;
        }
        m_currentByte = m_blocks[m_currentBlockIndex].m_buffer.get();
        m_afterBlockEnd = m_blocks[m_currentBlockIndex].m_afterBlockEnd;
    }
}

template<class IST>
template<class ISIT>
InputStreamSource<IST>::InputStreamSource(ISIT& inputStreamInitializer, const size_t blockSize) :
    InputSource(0, 0, true),
    m_inputStream(inputStreamInitializer),
    m_blockSize(blockSize),
    m_nextBlockIndexToLoad(0),
    m_nextBlockNumberToLoad(0),
    m_currentBlockIndex(0),
    m_currentBlockNumber(static_cast<size_t>(-1))
{
    for (size_t index = 0; index < 2; ++index) {
        m_blocks[index].m_blockNumber = static_cast<size_t>(-1);
        m_blocks[index].m_buffer.reset(new uint8_t[m_blockSize]);
    }
    loadNextBlock();
}

template<class IST>
void InputStreamSource<IST>::rewind() {
    for (size_t blockIndex = 0; blockIndex < 2; ++blockIndex) {
        if (m_blocks[blockIndex].m_blockNumber == 0) {
            m_currentBlockIndex = blockIndex;
            m_currentBlockNumber = 0;
            m_currentByte = m_blocks[m_currentBlockIndex].m_buffer.get();
            return;
        }
    }
    m_inputStream.rewind();
    for (size_t index = 0; index < 2; ++index)
        m_blocks[index].m_blockNumber = static_cast<size_t>(-1);
    m_currentByte = 0;
    m_hasMoreBlocks = true;
    m_nextBlockIndexToLoad = 0;
    m_nextBlockNumberToLoad = 0;
    m_currentBlockIndex = 0;
    m_currentBlockNumber = static_cast<size_t>(-1);
    loadNextBlock();
}

template<class IST>
std::unique_ptr<InputSource::Position> InputStreamSource<IST>::createPosition() {
    return std::unique_ptr<InputSource::Position>(new InputStreamSourcePosition);
}

template<class IST>
void InputStreamSource<IST>::savePosition(Position& position) const {
    InputStreamSourcePosition& inputStreamSourcePosition = static_cast<InputStreamSourcePosition&>(position);
    inputStreamSourcePosition.m_currentBlockNumber = m_blocks[m_currentBlockIndex].m_blockNumber;
    inputStreamSourcePosition.m_currentByteOffset = (m_currentByte - m_blocks[m_currentBlockIndex].m_buffer.get());
}

template<class IST>
void InputStreamSource<IST>::restorePosition(const Position& position) {
    const InputStreamSourcePosition& inputStreamSourcePosition = static_cast<const InputStreamSourcePosition&>(position);
    if (inputStreamSourcePosition.m_currentBlockNumber == m_blocks[0].m_blockNumber)
        m_currentBlockIndex = 0;
    else if (inputStreamSourcePosition.m_currentBlockNumber == m_blocks[1].m_blockNumber)
        m_currentBlockIndex = 1;
    else
        throw RDF_STORE_EXCEPTION("Cannot restore position: the stream was advanced too far.");
    m_currentByte = m_blocks[m_currentBlockIndex].m_buffer.get() + inputStreamSourcePosition.m_currentByteOffset;
    m_afterBlockEnd = m_blocks[m_currentBlockIndex].m_afterBlockEnd;
    m_hasMoreBlocks = (m_currentByte != m_afterBlockEnd);
}

#endif /* INPUTSTREAMSOURCE_H_ */
