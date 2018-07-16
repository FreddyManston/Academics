// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef LOCKFREEQUEUE_H_
#define LOCKFREEQUEUE_H_

#include "../all.h"
#include "../RDFStoreException.h"
#include "MemoryRegion.h"
#include "InputStream.h"
#include "OutputStream.h"

class MemoryManager;

template<typename E>
class LockFreeQueue : private Unmovable {

protected:

    aligned_uint64_t m_head;
    aligned_uint64_t m_tail;
    MemoryRegion<E> m_elements;

public:

    always_inline LockFreeQueue(MemoryManager& memoryManager) : m_head(0), m_tail(0), m_elements(memoryManager) {
    }

    always_inline bool initialize(const size_t maximumNumberOfElements) {
        m_head = m_tail = 0;
        return m_elements.initialize(maximumNumberOfElements);
    }

    always_inline bool initializeLarge() {
        m_head = m_tail = 0;
        return m_elements.initializeLarge();
    }

    always_inline void deinitialize() {
        m_head = m_tail = 0;
        m_elements.deinitialize();
    }

    always_inline void appendUnprocessed(const LockFreeQueue<E>& other) {
        const size_t unprocessedSize = other.getUnprocessedSize();
        const size_t newHead = m_head + unprocessedSize;
        if (!m_elements.ensureEndAtLeast(newHead, 1))
            throw RDF_STORE_EXCEPTION("Memory exhausted in LockFreeQueue.");
        std::memcpy(m_elements + m_head, other.m_elements + other.m_tail, unprocessedSize * sizeof(E));
        m_head = newHead;
    }
    
    template<bool multithreaded>
    always_inline void enqueue(E const value) {
        assert(value != 0);
        if (multithreaded) {
            bool success;
            do {
                const uint64_t head = ::atomicRead(m_head);
                if (!m_elements.ensureEndAtLeast(head, 1))
                    throw RDF_STORE_EXCEPTION("Memory exhausted in LockFreeQueue.");
                success = ::atomicConditionalSet(m_elements[head], static_cast<E>(0), value);
                ::atomicConditionalSet(m_head, head, head + 1);
            } while (!success);
        }
        else {
            if (!m_elements.ensureEndAtLeast(m_head, 1))
                throw RDF_STORE_EXCEPTION("Memory exhausted in LockFreeQueue.");
            m_elements[m_head] = value;
            ++m_head;
        }
    }

    template<bool multithreaded>
    always_inline E dequeue() {
        return dequeue<multithreaded>(static_cast<uint64_t>(-1));
    }

    template<bool multithreaded>
    always_inline E dequeue(const uint64_t extractBeforePosition) {
        if (multithreaded) {
            while (true) {
                const uint64_t elementPosition = ::atomicRead(m_tail);
                if (elementPosition < extractBeforePosition && m_elements.isBeforeEnd(elementPosition, 1)) {
                    const E result = ::atomicRead(m_elements[elementPosition]);
                    if (result == 0)
                        return 0;
                    else if (::atomicConditionalSet(m_tail, elementPosition, elementPosition + 1))
                        return result;
                }
                else
                    return 0;
            }
        }
        else {
            if (m_tail < extractBeforePosition && m_elements.isBeforeEnd(m_tail, 1)) {
                const E result = m_elements[m_tail];
                if (result != 0)
                    ++m_tail;
                return result;
            }
            else
                return 0;
        }
    }
    
    always_inline E peekDequeue() const {
        const uint64_t tail = ::atomicRead(m_tail);
        if (m_elements.isBeforeEnd(tail, 1))
            return ::atomicRead(m_elements[tail]);
        else
            return 0;
    }

    always_inline void resetDequeuePosition(const uint64_t position = 0) {
        ::atomicWrite(m_tail, position);
    }

    always_inline bool canDequeue() const {
        return ::atomicRead(m_tail) != ::atomicRead(m_head);
    }

    always_inline size_t getUnprocessedSize() const {
        // We need to read m_tail before m_head so that we always return a nonnegative number
        const size_t tail = ::atomicRead(m_tail);
        const size_t head = ::atomicRead(m_head);
        return head - tail;
    }
            
    always_inline uint64_t getFirstFreePosition() const {
        return ::atomicRead(m_head);
    }

    always_inline uint64_t getNextDequeuePosition() const {
        return ::atomicRead(m_tail);
    }

    always_inline const E& operator[](const size_t index) const {
        return m_elements[index];
    }

    always_inline E& operator[](const size_t index) {
        return m_elements[index];
    }

    always_inline void save(OutputStream& outputStream) const {
        outputStream.writeString("LockFreeQueue");
        outputStream.write(m_head);
        outputStream.write(m_tail);
        outputStream.writeMemoryRegion(m_elements);
    }

    always_inline void load(InputStream& inputStream) {
        if (!inputStream.checkNextString("LockFreeQueue"))
            throw RDF_STORE_EXCEPTION("Invalid input file: cannot load LockFreeQueue.");
        m_head = inputStream.read<uint64_t>();
        m_tail = inputStream.read<uint64_t>();
        inputStream.readMemoryRegion(m_elements);
    }

    __ALIGNED(LockFreeQueue<E>)

};

#endif /* LOCKFREEQUEUE_H_ */
