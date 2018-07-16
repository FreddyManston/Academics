// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef MEMORYMAPPEDFILESOURCE_H_
#define MEMORYMAPPEDFILESOURCE_H_

#include "../../all.h"
#include "../../RDFStoreException.h"
#include "../../util/File.h"
#include "../../util/MemoryMappedFileView.h"
#include "../InputSource.h"

class MemoryMappedFileSource : public InputSource {

protected:

    MemoryMappedFileView m_memoryMappedFileView;
    const size_t m_mapBlockSize;
    const size_t m_fileSize;
    size_t m_currentBlockOffset;
    size_t m_currentBlockSize;

    struct MemoryMappedFileSourcePosition : public Position {
        size_t m_currentBlockOffset;
        size_t m_currentByteOffset;
    };

    virtual void loadNextBlock();

public:

    MemoryMappedFileSource(File& file, const size_t blockSize);

    virtual void rewind();

    virtual std::unique_ptr<Position> createPosition();

    virtual void savePosition(Position& position) const;

    virtual void restorePosition(const Position& position);

};

#endif /* MEMORYMAPPEDFILESOURCE_H_ */
