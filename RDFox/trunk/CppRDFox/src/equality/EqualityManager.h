// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef EQUALITYMANAGER_H_
#define EQUALITYMANAGER_H_

#include "../Common.h"
#include "../RDFStoreException.h"
#include "../util/MemoryRegion.h"

class MemoryManager;
class ComponentStatistics;
class InputStream;
class OutputStream;

// EqualityManager

class EqualityManager : private Unmovable {

protected:

    static const size_t ENTRY_SIZE = 2;

    MemoryRegion<ResourceID> m_data;

public:

    EqualityManager(MemoryManager& memoryManager);

    bool initialize();

    always_inline ResourceID getAfterLastResourceID() const {
        return static_cast<ResourceID>(m_data.getEndIndex() / ENTRY_SIZE);
    }

    always_inline bool isNormal(ResourceID resourceID) const {
        const size_t offset = resourceID * ENTRY_SIZE;
        return !m_data.isBeforeEnd(offset, ENTRY_SIZE) || m_data[offset] == INVALID_RESOURCE_ID;
    }

    always_inline bool isNormal(const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes) const {
        for (std::vector<ArgumentIndex>::const_iterator iterator = argumentIndexes.begin(); iterator != argumentIndexes.end(); ++iterator)
            if (!isNormal(argumentsBuffer[*iterator]))
                return false;
        return true;
    }

    always_inline ResourceID normalize(ResourceID resourceID) const {
        ResourceID nextResourceID;
        while (true) {
            const size_t offset = resourceID * ENTRY_SIZE;
            if (m_data.isBeforeEnd(offset, ENTRY_SIZE) && (nextResourceID = ::atomicRead(m_data[offset])) != INVALID_RESOURCE_ID)
                resourceID = nextResourceID;
            else
                return resourceID;
        }
    }

    always_inline bool normalize(std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes) const {
        bool atLeastOneChange = false;
        for (std::vector<ArgumentIndex>::const_iterator iterator = argumentIndexes.begin(); iterator != argumentIndexes.end(); ++iterator) {
            const ResourceID resourceID = argumentsBuffer[*iterator];
            const ResourceID canonicalResourceID = normalize(resourceID);
            if (resourceID != canonicalResourceID) {
                atLeastOneChange = true;
                argumentsBuffer[*iterator] = canonicalResourceID;
            }
        }
        return atLeastOneChange;
    }

    always_inline bool normalize(const std::vector<ResourceID>& sourceArgumentsBuffer, std::vector<ResourceID>& targetArgumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes) const {
        bool atLeastOneChange = false;
        for (std::vector<ArgumentIndex>::const_iterator iterator = argumentIndexes.begin(); iterator != argumentIndexes.end(); ++iterator) {
            const ArgumentIndex argumentIndex = *iterator;
            const ResourceID resourceID = sourceArgumentsBuffer[argumentIndex];
            const ResourceID canonicalResourceID = normalize(resourceID);
            targetArgumentsBuffer[argumentIndex] = canonicalResourceID;
            if (resourceID != canonicalResourceID)
                atLeastOneChange = true;
        }
        return atLeastOneChange;
    }

    always_inline bool normalize(const std::vector<ResourceID>& sourceArgumentsBuffer, const std::vector<ArgumentIndex>& sourceArgumentIndexes, std::vector<ResourceID>& targetArgumentsBuffer, const std::vector<ArgumentIndex>& targetArgumentIndexes) const {
        bool atLeastOneChange = false;
        std::vector<ArgumentIndex>::const_iterator sourceIterator = sourceArgumentIndexes.begin();
        std::vector<ArgumentIndex>::const_iterator targetIterator = targetArgumentIndexes.begin();
        while (sourceIterator != sourceArgumentIndexes.end()) {
            const ResourceID resourceID = sourceArgumentsBuffer[*sourceIterator];
            const ResourceID canonicalResourceID = normalize(resourceID);
            targetArgumentsBuffer[*targetIterator] = canonicalResourceID;
            if (resourceID != canonicalResourceID)
                atLeastOneChange = true;
            ++sourceIterator;
            ++targetIterator;
        }
        return atLeastOneChange;
    }

    always_inline ResourceID getNextEqual(const ResourceID resourceID) const {
        const size_t offset = resourceID * ENTRY_SIZE;
        if (m_data.isBeforeEnd(offset, ENTRY_SIZE))
            return ::atomicRead(m_data[offset + 1]);
        else
            return INVALID_RESOURCE_ID;
    }

    always_inline uint32_t getEquivalenceClassSize(ResourceID resourceID) const {
        assert(isNormal(resourceID));
        size_t offset = resourceID * ENTRY_SIZE;
        if (m_data.isBeforeEnd(offset, ENTRY_SIZE)) {
            uint32_t size = 1;
            while ((resourceID = ::atomicRead(m_data[offset + 1])) != INVALID_RESOURCE_ID) {
                ++size;
                offset = resourceID * ENTRY_SIZE;
            }
            return size;
        }
        else
            return 1;
    }

    always_inline size_t multiplyByEquivalenceClassSizes(size_t number, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes) const {
        for (std::vector<ArgumentIndex>::const_iterator iterator = argumentIndexes.begin(); iterator != argumentIndexes.end(); ++iterator)
            number *= getEquivalenceClassSize(argumentsBuffer[*iterator]);
        return number;
    }

    template<bool parallel>
    bool merge(const ResourceID toMergeID, const ResourceID mergeIntoID);

    always_inline void unrepresent(ResourceID resourceID) {
        assert(isNormal(resourceID));
        while (resourceID != INVALID_RESOURCE_ID) {
            const size_t offset = resourceID * ENTRY_SIZE;
            if (m_data.isBeforeEnd(offset, ENTRY_SIZE)) {
                resourceID = ::atomicRead(m_data[offset + 1]);
                ::atomicWrite(m_data[offset], INVALID_RESOURCE_ID);
            }
            else
                return;
        }
    }

    always_inline void breakEquals(ResourceID resourceID) {
        assert(isNormal(resourceID));
        while (resourceID != INVALID_RESOURCE_ID) {
            const size_t offset = resourceID * ENTRY_SIZE;
            if (m_data.isBeforeEnd(offset, ENTRY_SIZE)) {
                resourceID = ::atomicRead(m_data[offset + 1]);
                ::atomicWrite(m_data[offset + 1], INVALID_RESOURCE_ID);
            }
            else
                return;
        }
    }

    always_inline void copyEquivalenceClass(ResourceID resourceID, const EqualityManager& otherEqualityManager) {
        assert(isNormal(resourceID));
        while (resourceID != INVALID_RESOURCE_ID) {
            const size_t offset = resourceID * ENTRY_SIZE;
            if (m_data.isBeforeEnd(offset, ENTRY_SIZE)) {
                resourceID = ::atomicRead(m_data[offset + 1]);
                if (otherEqualityManager.m_data.isBeforeEnd(offset, ENTRY_SIZE)) {
                    ::atomicWrite(m_data[offset], ::atomicRead(otherEqualityManager.m_data[offset]));
                    ::atomicWrite(m_data[offset + 1], ::atomicRead(otherEqualityManager.m_data[offset + 1]));
                }
                else {
                    ::atomicWrite(m_data[offset], INVALID_RESOURCE_ID);
                    ::atomicWrite(m_data[offset + 1], INVALID_RESOURCE_ID);
                }
            }
            else
                return;
        }
    }

    void save(OutputStream& outputStream) const;

    void load(InputStream& inputStream);

    std::unique_ptr<ComponentStatistics> getComponentStatistics() const;

    __ALIGNED(EqualityManager)

};

template<>
always_inline bool EqualityManager::merge<false>(const ResourceID toMergeID, const ResourceID mergeIntoID) {
    assert(toMergeID != INVALID_RESOURCE_ID);
    assert(mergeIntoID != INVALID_RESOURCE_ID);
    const size_t toMergeOffset = toMergeID * ENTRY_SIZE;
    size_t mergeIntoEndOffset = mergeIntoID * ENTRY_SIZE;
    if (!m_data.ensureEndAtLeast(std::max(toMergeOffset, mergeIntoEndOffset), ENTRY_SIZE))
        throw RDF_STORE_EXCEPTION("Memory exhausted.");
    m_data[toMergeOffset] = mergeIntoID;
    ResourceID nextResourceID;
    while ((nextResourceID = m_data[mergeIntoEndOffset + 1]) != INVALID_RESOURCE_ID)
        mergeIntoEndOffset = nextResourceID * ENTRY_SIZE;
    m_data[mergeIntoEndOffset + 1] = toMergeID;
    return true;
}

template<>
always_inline bool EqualityManager::merge<true>(const ResourceID toMergeID, const ResourceID mergeIntoID) {
    assert(toMergeID != INVALID_RESOURCE_ID);
    assert(mergeIntoID != INVALID_RESOURCE_ID);
    const size_t toMergeOffset = toMergeID * ENTRY_SIZE;
    size_t mergeIntoEndOffset = mergeIntoID * ENTRY_SIZE;
    if (!m_data.ensureEndAtLeast(std::max(toMergeOffset, mergeIntoEndOffset), ENTRY_SIZE))
        throw RDF_STORE_EXCEPTION("Memory exhausted.");
    if (::atomicConditionalSet(m_data[toMergeOffset], INVALID_RESOURCE_ID, mergeIntoID)) {
        do {
            ResourceID nextResourceID;
            while ((nextResourceID = ::atomicRead(m_data[mergeIntoEndOffset + 1])) != INVALID_RESOURCE_ID)
                mergeIntoEndOffset = nextResourceID * ENTRY_SIZE;
        } while (!::atomicConditionalSet(m_data[mergeIntoEndOffset + 1], INVALID_RESOURCE_ID, toMergeID));
        return true;
    }
    else
        return false;
}

#endif /* EQUALITYMANAGER_H_ */
