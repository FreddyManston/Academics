// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef LOCKFREEBITSET_H_
#define LOCKFREEBITSET_H_

#include "../all.h"
#include "../RDFStoreException.h"
#include "MemoryRegion.h"
#include "InputStream.h"
#include "OutputStream.h"

class MemoryManager;

class LockFreeBitSet : private Unmovable {

protected:

    static const uint64_t BITS_PER_ELEMENT = sizeof(uint64_t) * 8;

    MemoryRegion<uint64_t> m_elements;

public:

    always_inline LockFreeBitSet(MemoryManager& memoryManager) : m_elements(memoryManager) {
    }

    always_inline bool initialize(const size_t maximumNumberOfBits) {
        return m_elements.initialize(maximumNumberOfBits / BITS_PER_ELEMENT + 1);
    }

    always_inline bool initializeLarge() {
        return m_elements.initializeLarge();
    }

    always_inline bool ensureEndAtLeast(const size_t index) {
        return m_elements.ensureEndAtLeast(index / BITS_PER_ELEMENT, 1);
    }

    always_inline void deinitialize() {
        m_elements.deinitialize();
    }

    template<bool multithreaded>
    always_inline bool add(const size_t index) {
        if (multithreaded) {
            const size_t elementIndex = index / BITS_PER_ELEMENT;
            if (!m_elements.ensureEndAtLeast(elementIndex, 1))
                throw RDF_STORE_EXCEPTION("Memory exhausted in LockFreeBitSet.");
            uint64_t& element = m_elements[elementIndex];
            const size_t bitMask = (static_cast<uint64_t>(1) << (index % BITS_PER_ELEMENT));
            uint64_t existingValue;
            do {
                existingValue = ::atomicRead(element);
                if ((existingValue & bitMask) != 0)
                    return false;
            } while (!::atomicConditionalSet(element, existingValue, existingValue | bitMask));
            return true;
        }
        else {
            const size_t elementIndex = index / BITS_PER_ELEMENT;
            if (!m_elements.ensureEndAtLeast(elementIndex, 1))
                throw RDF_STORE_EXCEPTION("Memory exhausted in LockFreeBitSet.");
            const size_t bitMask = (static_cast<uint64_t>(1) << (index % BITS_PER_ELEMENT));
            if ((m_elements[elementIndex] & bitMask) == 0) {
                m_elements[elementIndex] |= bitMask;
                return true;
            }
            else
                return false;
        }
    }

    always_inline bool contains(const size_t index) const {
        const size_t elementIndex = index / BITS_PER_ELEMENT;
        if (!m_elements.isBeforeEnd(elementIndex, 1))
            return false;
        return (::atomicRead(m_elements[elementIndex]) & (static_cast<uint64_t>(1) << (index % BITS_PER_ELEMENT))) != 0;
    }

    always_inline void save(OutputStream& outputStream) const {
        outputStream.writeString("LockFreeBitSet");
        outputStream.writeMemoryRegion(m_elements);
    }

    always_inline void load(InputStream& inputStream) {
        if (!inputStream.checkNextString("LockFreeBitSet"))
            throw RDF_STORE_EXCEPTION("Invalid input file: cannot load LockFreeBitSet.");
        inputStream.readMemoryRegion(m_elements);
    }

};

#endif /* LOCKFREEBITSET_H_ */
