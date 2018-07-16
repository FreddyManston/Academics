// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef MEMORYREGION_H_
#define MEMORYREGION_H_

#include "../all.h"
#include "MemoryManager.h"

template<typename T>
class MemoryRegion : private Unmovable {

protected:

    T* m_data;
    aligned_size_t m_beginIndex;
    aligned_size_t m_endIndex;
    size_t m_maximumNumberOfItems;
    MemoryManager& m_memoryManager;
    uint8_t m_pageSizeShift;

    static uint8_t getPageSizeShift() {
        size_t number = ::getVMPageSize();
        uint8_t result = 0;
        while (number > 1) {
            result++;
            number >>= 1;
        }
        return result;
    }


    always_inline size_t getNumberOfPages(const size_t sizeInBytes) const {
        if (sizeInBytes == 0)
            return 0;
        else
            return ((sizeInBytes - 1) >> m_pageSizeShift) + 1;
    }

    always_inline size_t roundDownToPage(const size_t sizeInBytes) const {
        return (sizeInBytes >> m_pageSizeShift) << m_pageSizeShift;
    }

    void doEnsureBeginAtLeast(size_t beginIndexWhenUpdated, const size_t newBeginIndex);

    bool doEnsureEndAtLeast(const size_t endIndex, size_t newEndIndex);

public:

    typedef T ElementType;

    always_inline MemoryRegion(MemoryManager& memoryManager, const uint8_t pageSizeMultiplierPower = 0) :
        m_data(nullptr),
        m_beginIndex(0),
        m_endIndex(0),
        m_maximumNumberOfItems(0),
        m_memoryManager(memoryManager),
        m_pageSizeShift(getPageSizeShift() + pageSizeMultiplierPower)
    {
    }

    always_inline MemoryRegion(MemoryManager& memoryManager, const uint8_t pageSizeMultiplierPower, const size_t maximumNumberOfItems) : MemoryRegion(memoryManager, pageSizeMultiplierPower) {
        if (initialize(maximumNumberOfItems, false))
            ensureEndAtLeast(maximumNumberOfItems, 0);
    }

    always_inline ~MemoryRegion() {
        deinitialize();
    }

    always_inline MemoryManager& getMemoryManager() const {
        return m_memoryManager;
    }

    always_inline bool initialize(const size_t maximumNumberOfItems, const bool overcommit = true) {
        assert(maximumNumberOfItems > 0);
        deinitialize();
#ifdef WIN32
        m_data = reinterpret_cast<T*>(::VirtualAlloc(NULL, maximumNumberOfItems * sizeof(T), MEM_RESERVE, PAGE_READWRITE));
        if (m_data == nullptr)
            return false;
#else
        m_data = reinterpret_cast<T*>(::mmap(0, maximumNumberOfItems * sizeof(T), PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANON | (overcommit ? MAP_NORESERVE : 0), -1, 0));
        if (m_data == reinterpret_cast<T*>(MAP_FAILED)) {
            m_data = nullptr;
            return false;
        }
#endif
        m_maximumNumberOfItems = maximumNumberOfItems;
        return true;
    }

    always_inline bool initializeLarge(const bool overcommit = true) {
        return initialize(m_memoryManager.getLargeRegionSize() / sizeof(T), overcommit);
    }

    always_inline void deinitialize() {
        if (m_data != nullptr) {
#ifdef WIN32
            ::VirtualFree(m_data, 0, MEM_RELEASE);
#else
            const size_t beginIndexPage = roundDownToPage(m_beginIndex * sizeof(T));
            ::munmap(reinterpret_cast<uint8_t*>(m_data) + beginIndexPage, m_maximumNumberOfItems * sizeof(T) - beginIndexPage);
#endif
            m_memoryManager.notifyMemoryDeallocated((m_endIndex - m_beginIndex) * sizeof(T));
            m_beginIndex = 0;
            m_endIndex = 0;
            m_maximumNumberOfItems = 0;
            m_data = nullptr;
        }
    }

    always_inline bool isInitialized() const {
        return m_data != nullptr;
    }

    always_inline size_t getMaximumNumberOfItems() const {
        return ::atomicRead(m_maximumNumberOfItems);
    }

    always_inline size_t getBeginIndex() const {
        return ::atomicRead(m_beginIndex);
    }

    always_inline T* getBeginElement() const {
        return m_data + getBeginIndex();
    }

    always_inline size_t getEndIndex() const {
        return ::atomicRead(m_endIndex);
    }

    always_inline T* getEndElement() const {
        return m_data + getEndIndex();
    }

    always_inline bool isAfterBegin(const size_t index) const {
        return index >= ::atomicRead(m_beginIndex);
    }

    always_inline bool isAfterBeginElement(const T* element) const {
        return isAfterBegin(element - m_data);
    }

    always_inline bool isBeforeEnd(const size_t index, const size_t size) const {
        return index + size <= ::atomicRead(m_endIndex);
    }

    always_inline bool isBeforeEndElement(const T* element) const {
        return isBeforeEnd(element - m_data, 0);
    }

    always_inline bool isBeforeEndElement(const T* element, const size_t size) const {
        return isBeforeEnd(element - m_data, size);
    }

    always_inline void ensureBeginAtLeast(const size_t index) {
        assert(index <= ::atomicRead(m_endIndex));
        const size_t beginIndex = ::atomicRead(m_beginIndex);
        if (beginIndex < index)
            doEnsureBeginAtLeast(beginIndex, index);
    }

    always_inline void ensureBeginElementAtLeast(const T* element) {
        ensureBeginAtLeast(element - m_data);
    }

    always_inline bool ensureEndAtLeast(const size_t index, const size_t size) {
        const size_t endIndex = ::atomicRead(m_endIndex);
        const size_t endIndexNeeded = index + size;
        if (endIndexNeeded <= endIndex)
            return true;
        else
            return doEnsureEndAtLeast(endIndex, endIndexNeeded);
    }

    always_inline bool ensureEndElementAtLeast(const T* startElement, const size_t size) {
        return ensureEndAtLeast(startElement - m_data, size);
    }

    always_inline T* getData() const {
        return m_data;
    }

    always_inline T* getBegin() const {
        return m_data + ::atomicRead(m_beginIndex);
    }

    always_inline T* getEnd() const {
        return m_data + ::atomicRead(m_endIndex);
    }

    always_inline operator T*() const {
        return m_data;
    }

    always_inline T* operator+(const size_t index) const {
        return m_data + index;
    }

    always_inline T& operator[](const size_t index) const {
        return m_data[index];
    }

    always_inline void swap(MemoryRegion<T>& other) {
        T* const otherData = other.m_data;
        const size_t otherBeginIndex = other.m_beginIndex;
        const size_t otherEndIndex = other.m_endIndex;
        const size_t otherMaximumNumberOfItems = other.m_maximumNumberOfItems;
        const uint8_t otherPageSizeShift = other.m_pageSizeShift;

        other.m_data = m_data;
        other.m_beginIndex = m_beginIndex;
        other.m_endIndex = m_endIndex;
        other.m_maximumNumberOfItems = m_maximumNumberOfItems;
        other.m_pageSizeShift = m_pageSizeShift;

        m_data = otherData;
        m_beginIndex = otherBeginIndex;
        m_endIndex = otherEndIndex;
        m_maximumNumberOfItems = otherMaximumNumberOfItems;
        m_pageSizeShift = otherPageSizeShift;
    }

    __ALIGNED(MemoryRegion<T>)

};

template<typename T>
void MemoryRegion<T>::doEnsureBeginAtLeast(size_t beginIndexWhenUpdated, const size_t newBeginIndex) {
    size_t result;
    while ((result = ::atomicConditionalSetEx(m_beginIndex, beginIndexWhenUpdated, newBeginIndex)) != beginIndexWhenUpdated) {
        if (result >= newBeginIndex)
            return;
        beginIndexWhenUpdated = result;
    }
    const size_t beginIndexWhenUpdatedSize = beginIndexWhenUpdated * sizeof(T);
    const size_t newBeginIndexSize = newBeginIndex * sizeof(T);
    m_memoryManager.notifyMemoryDeallocated(newBeginIndexSize - beginIndexWhenUpdatedSize);
    const size_t beginIndexWhenUpdatedPage = roundDownToPage(beginIndexWhenUpdatedSize);
    const size_t decommitSize = roundDownToPage(newBeginIndexSize) - beginIndexWhenUpdatedPage;
    if (decommitSize != 0) {
        uint8_t* const decommitStart = reinterpret_cast<uint8_t*>(m_data) + beginIndexWhenUpdatedPage;
#ifdef WIN32
        ::VirtualFree(decommitStart, decommitSize, MEM_DECOMMIT);
#else
        ::munmap(decommitStart, decommitSize);
#endif
    }
}

template<typename T>
bool MemoryRegion<T>::doEnsureEndAtLeast(const size_t endIndex, size_t newEndIndex) {
    if (newEndIndex <= m_maximumNumberOfItems) {
        // Make sure that memory is allocated in pages.
        newEndIndex = std::min(m_maximumNumberOfItems, (getNumberOfPages(newEndIndex * sizeof(T)) << m_pageSizeShift) / sizeof(T));
        const size_t bytesToAllocate = (newEndIndex - endIndex) * sizeof(T);
        if (m_memoryManager.tryAllocateMemory(bytesToAllocate)) {
#ifdef WIN32
            const size_t endIndexSize = roundDownToPage(endIndex * sizeof(T));
            if (::VirtualAlloc(reinterpret_cast<uint8_t*>(m_data) + endIndexSize, bytesToAllocate, MEM_COMMIT, PAGE_READWRITE) != NULL) {
                // We might have committed over memory that has been decommitted since we read endIndex last time, so we need to fix that.
                const size_t beginIndex = ::atomicRead(m_beginIndex);
                if (beginIndex > endIndex) {
                    const size_t decommitSize = roundDownToPage(beginIndex * sizeof(T)) - endIndexSize;
                    if (decommitSize != 0)
                        ::VirtualFree(reinterpret_cast<uint8_t*>(m_data) + endIndexSize, decommitSize, MEM_DECOMMIT);
                }
#endif
                size_t endIndexWhenUpdated = endIndex;
                size_t result;
                while ((result = ::atomicConditionalSetEx(m_endIndex, endIndexWhenUpdated, newEndIndex)) != endIndexWhenUpdated) {
                    if (result >= newEndIndex) {
                        endIndexWhenUpdated = newEndIndex;
                        break;
                    }
                    endIndexWhenUpdated = result;
                }
                if (endIndexWhenUpdated > endIndex)
                    m_memoryManager.notifyMemoryDeallocated((endIndexWhenUpdated - endIndex) * sizeof(T));
                return true;
#ifdef WIN32
            }
            m_memoryManager.notifyMemoryDeallocated(bytesToAllocate);
#endif
        }
    }
    return false;
}

#endif /* MEMORYREGION_H_ */
