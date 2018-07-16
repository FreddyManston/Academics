// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef MEMORYMANAGER_H_
#define MEMORYMANAGER_H_

#include "../all.h"

class MemoryManager : private Unmovable {

protected:

    const size_t m_totalBytesAvailable;
    const size_t m_largeRegionSize;
    int64_t m_bytesAvailable;


public:

    MemoryManager(size_t totalBytesAvailable) :
        m_totalBytesAvailable(totalBytesAvailable),
        m_largeRegionSize(totalBytesAvailable),
        m_bytesAvailable(static_cast<uint64_t>(totalBytesAvailable))
    {
    }

    always_inline size_t getLargeRegionSize() const {
        return m_largeRegionSize;
    }

    always_inline size_t getAvailableMemorySize() const {
        return static_cast<size_t>(::atomicRead(m_bytesAvailable));
    }

    always_inline size_t getUsedMemorySize() const {
        return m_totalBytesAvailable - ::atomicRead(m_bytesAvailable);
    }

    always_inline bool tryAllocateMemory(const size_t bytesToAllocate) {
        if (::atomicSubtract(m_bytesAvailable, static_cast<ssize_t>(bytesToAllocate)) >= 0)
            return true;
        else {
            ::atomicAdd(m_bytesAvailable, static_cast<ssize_t>(bytesToAllocate));
            return false;
        }
    }

    always_inline void notifyMemoryDeallocated(const size_t bytesDeallocated) {
        ::atomicAdd(m_bytesAvailable, static_cast<ssize_t>(bytesDeallocated));
    }

};

#endif // MEMORYMANAGER_H_
