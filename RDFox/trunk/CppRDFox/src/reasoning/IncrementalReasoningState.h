// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef INCREMENTALREASONINGSTATE_H_
#define INCREMENTALREASONINGSTATE_H_

#include "../Common.h"
#include "../RDFStoreException.h"
#include "../util/MemoryRegion.h"
#include "../util/LockFreeQueue.h"
#include "../util/LockFreeBitSet.h"

typedef uint8_t TupleFlags;

const TupleFlags GF_DELETED_NEW     = static_cast<TupleFlags>(0x01);
const TupleFlags GF_DELETED         = static_cast<TupleFlags>(0x02);
const TupleFlags GF_ADDED_NEW       = static_cast<TupleFlags>(0x04);
const TupleFlags GF_ADDED           = static_cast<TupleFlags>(0x08);
const TupleFlags GF_ADDED_MERGED    = static_cast<TupleFlags>(0x10);
const TupleFlags GF_DISPROVED       = static_cast<TupleFlags>(0x20);
const TupleFlags GF_NORM_PROVED     = static_cast<TupleFlags>(0x40);

const TupleFlags LF_PROVED_NEW      = static_cast<TupleFlags>(0x01);
const TupleFlags LF_PROVED          = static_cast<TupleFlags>(0x02);
const TupleFlags LF_PROVED_MERGED   = static_cast<TupleFlags>(0x04);
const TupleFlags LF_CHECKED         = static_cast<TupleFlags>(0x08);
const TupleFlags LF_DELAYED         = static_cast<TupleFlags>(0x10);

class IncrementalReasoningState : private Unmovable {

protected:

    MemoryRegion<TupleFlags> m_globalFlags;
    unique_ptr_vector<LockFreeQueue<TupleIndex> > m_initiallyDeletedByComponentLevel;
    LockFreeQueue<TupleIndex> m_deleteList;
    std::vector<uint64_t> m_deleteListEndByComponentLevel;
    unique_ptr_vector<LockFreeQueue<TupleIndex> > m_initiallyAddedByComponentLevel;
    LockFreeQueue<TupleIndex> m_addedList;
    std::vector<uint64_t> m_addedListEndByComponentLevel;
    LockFreeBitSet m_updatedEquivalenceClassesSet;
    MemoryRegion<TupleFlags> m_currentLevelFlags;
    LockFreeQueue<TupleIndex> m_provedList;

    always_inline static TupleFlags getFlags(const MemoryRegion<TupleFlags>& flags, const TupleIndex tupleIndex) {
        if (tupleIndex < flags.getEndIndex())
            return ::atomicRead(flags[tupleIndex]);
        else
            return 0;
    }

    template<bool multithreaded>
    always_inline static bool addFlags(MemoryRegion<TupleFlags>& flags, const size_t elementIndex, const TupleFlags flagsToAdd) {
        if (!flags.ensureEndAtLeast(elementIndex, 1))
            throw RDF_STORE_EXCEPTION("Out of memory while resizing the flags array.");
        if (multithreaded) {
            TupleFlags existingFlags;
            TupleFlags newFlags;
            do {
                existingFlags = ::atomicRead(flags[elementIndex]);
                newFlags = (existingFlags | flagsToAdd);
                if (newFlags == existingFlags)
                    return false;
            } while (!::atomicConditionalSet(flags[elementIndex], existingFlags, newFlags));
            return true;
        }
        else {
            const TupleFlags existingFlags = flags[elementIndex];
            const TupleFlags newFlags = (existingFlags | flagsToAdd);
            if (newFlags == existingFlags)
                return false;
            else {
                flags[elementIndex] = newFlags;
                return true;
            }
        }
    }

public:

    IncrementalReasoningState(MemoryManager& memoryManager);

    void initializeGlobal(const size_t maxComponentLevel);
    
    void initializeCurrentLevel();

    always_inline size_t getMaxComponentLevel() const {
        return m_initiallyDeletedByComponentLevel.size() - 1;
    }

    always_inline TupleFlags getGlobalFlags(const TupleIndex tupleIndex) const {
        return getFlags(m_globalFlags, tupleIndex);
    }
    
    template<bool multithreaded>
    always_inline bool addGlobalFlags(const TupleIndex tupleIndex, const TupleFlags flagsToAdd) {
        return addFlags<multithreaded>(m_globalFlags, static_cast<size_t>(tupleIndex), flagsToAdd);
    }

    always_inline LockFreeQueue<TupleIndex>& getInitiallyDeletedList(const size_t componentLevel) {
        return *m_initiallyDeletedByComponentLevel[componentLevel];
    }
    
    always_inline LockFreeQueue<TupleIndex>& getDeleteList() {
        return m_deleteList;
    }

    always_inline uint64_t& getDeleteListEnd(const size_t componentLevel) {
        return m_deleteListEndByComponentLevel[componentLevel];
    }

    always_inline LockFreeQueue<TupleIndex>& getInitiallyAddedList(const size_t componentLevel) {
        return *m_initiallyAddedByComponentLevel[componentLevel];
    }
    
    always_inline LockFreeQueue<TupleIndex>& getAddedList() {
        return m_addedList;
    }
    
    always_inline uint64_t& getAddedListEnd(const size_t componentLevel) {
        return m_addedListEndByComponentLevel[componentLevel];
    }

    always_inline TupleFlags getCurrentLevelFlags(const TupleIndex tupleIndex) const {
        return getFlags(m_currentLevelFlags, tupleIndex);
    }
    
    template<bool multithreaded>
    always_inline bool addCurrentLevelFlags(const TupleIndex tupleIndex, const TupleFlags flagsToAdd) {
        return addFlags<multithreaded>(m_currentLevelFlags, static_cast<size_t>(tupleIndex), flagsToAdd);
    }

    always_inline LockFreeQueue<TupleIndex>& getProvedList() {
        return m_provedList;
    }
    
};

#endif /* INCREMENTALREASONINGSTATE_H_ */
