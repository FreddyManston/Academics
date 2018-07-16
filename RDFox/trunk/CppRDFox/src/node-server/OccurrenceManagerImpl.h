// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef OCCURRENCEMANAGERIMPL_H_
#define OCCURRENCEMANAGERIMPL_H_

#include "../RDFStoreException.h"
#include "OccurrenceManager.h"

// OccurrenceManager::iterator

always_inline OccurrenceManager::iterator::iterator(ImmutableOccurrenceSet OccurrenceSet, const uint8_t numberOfNodes, const NodeID currentNodeID) : m_occurrenceSet(OccurrenceSet), m_numberOfNodes(numberOfNodes), m_currentNodeID(currentNodeID) {
}

always_inline OccurrenceManager::iterator::iterator(const iterator& other) : m_occurrenceSet(other.m_occurrenceSet), m_numberOfNodes(other.m_numberOfNodes), m_currentNodeID(other.m_currentNodeID) {
}

always_inline OccurrenceManager::iterator::iterator(iterator&& other) : m_occurrenceSet(other.m_occurrenceSet), m_numberOfNodes(other.m_numberOfNodes), m_currentNodeID(other.m_currentNodeID) {
}

always_inline OccurrenceManager::iterator& OccurrenceManager::iterator::operator=(const iterator& other) {
    m_occurrenceSet = other.m_occurrenceSet;
    m_numberOfNodes = other.m_numberOfNodes;
    m_currentNodeID = other.m_currentNodeID;
    return *this;
}

always_inline bool OccurrenceManager::iterator::operator==(const iterator& other) const {
    return m_occurrenceSet == other.m_occurrenceSet && m_numberOfNodes == other.m_numberOfNodes && m_currentNodeID == other.m_currentNodeID;
}

always_inline bool OccurrenceManager::iterator::isAtEnd() const {
    return m_currentNodeID == m_numberOfNodes;
}

always_inline OccurrenceManager::iterator& OccurrenceManager::iterator::operator++() {
    ++m_currentNodeID;
    return *this;
}

always_inline bool OccurrenceManager::iterator::operator*() const {
    return (m_occurrenceSet[m_currentNodeID >> 3] & (1 << (m_currentNodeID & 0x7))) != 0;
}

always_inline NodeID OccurrenceManager::iterator::getCurrentNodeID() const {
    return m_currentNodeID;
}

// OccurrenceManager

always_inline MemoryManager& OccurrenceManager::getMemoryManager() {
    return m_data.getMemoryManager();
}

always_inline uint8_t OccurrenceManager::getArity() const {
    return m_arity;
}

always_inline size_t OccurrenceManager::getSetWidth() const {
    return m_setWidth;
}

always_inline size_t OccurrenceManager::getResourceWidth() const {
    return m_resourceWidth;
}

always_inline ResourceID OccurrenceManager::getAfterLastResourceID() const {
    return static_cast<ResourceID>(m_data.getEndIndex() / m_resourceWidth);
}

always_inline ImmutableOccurrenceSet OccurrenceManager::getOccurrenceSet(const ResourceID resourceID, const uint8_t position) const {
    const size_t resourceIDStart = resourceID * m_resourceWidth;
    if (m_data.isBeforeEnd(resourceIDStart, m_resourceWidth))
        return m_data + resourceIDStart + position * m_setWidth;
    else
        return m_fullSets.get() + position * m_setWidth;
}

always_inline OccurrenceSet OccurrenceManager::getOccurrenceSetSafe(const ResourceID resourceID, const uint8_t position) {
    const size_t resourceIDStart = resourceID * m_resourceWidth;
    if (!m_data.isBeforeEnd(resourceIDStart, m_resourceWidth))
        extendTo(resourceIDStart);
    return m_data + resourceIDStart + position * m_setWidth;
}

always_inline ImmutableOccurrenceSet OccurrenceManager::getOccurrenceSetSafe(const ResourceID resourceID, const uint8_t position) const {
    return const_cast<OccurrenceManager*>(this)->getOccurrenceSetSafe(resourceID, position);
}

always_inline bool OccurrenceManager::contains(const ResourceID resourceID, const uint8_t position, const NodeID nodeID) const {
    return contains(getOccurrenceSet(resourceID, position), nodeID);
}

always_inline bool OccurrenceManager::contains(ImmutableOccurrenceSet occurrenceSet, const NodeID nodeID) {
    return (occurrenceSet[nodeID >> 3] & (1 << (nodeID & 0x7))) != 0;
}

always_inline void OccurrenceManager::add(const ResourceID resourceID, const uint8_t position, const NodeID nodeID) {
    add(getOccurrenceSetSafe(resourceID, position), nodeID);
}

always_inline void OccurrenceManager::add(OccurrenceSet occurrenceSet, const NodeID nodeID) {
    occurrenceSet[nodeID >> 3] |= (1 << (nodeID & 0x7));
}

always_inline void OccurrenceManager::copyAll(ImmutableOccurrenceSet sourcePosition0OccurrenceSet, OccurrenceSet targetPosition0OccurrenceSet) const {
    ::memcpy(targetPosition0OccurrenceSet, sourcePosition0OccurrenceSet, m_resourceWidth);
}

always_inline void OccurrenceManager::clearAll(OccurrenceSet targetPosition0OccurrenceSet) const {
    ::memset(targetPosition0OccurrenceSet, 0, m_resourceWidth);
}

always_inline void OccurrenceManager::intersectWith(OccurrenceSet targetOccurrenceSet, ImmutableOccurrenceSet intersectWithOccurrenceSet) const {
    for (uint32_t index = 0; index < m_setWidth; ++index)
        targetOccurrenceSet[index] &= intersectWithOccurrenceSet[index];
}

always_inline void OccurrenceManager::makeFullSet(OccurrenceSet targetOccurrenceSet) const {
    for (uint32_t index = 0; index < m_setWidth; ++index)
        targetOccurrenceSet[index] = 0xFF;
}

always_inline bool OccurrenceManager::isFullSet(ImmutableOccurrenceSet occurrenceSet) const {
    const uint32_t numberOfCompleteBytes = m_numberOfNodes >> 3;
    for (uint32_t index = 0; index < numberOfCompleteBytes; ++index)
        if (occurrenceSet[index] != 0xFF)
            return false;
    const uint8_t nodesInLastByte = static_cast<uint8_t>(m_numberOfNodes) & 0x7;
    if (nodesInLastByte == 0)
        return true;
    const uint8_t lastByteMask = (0xFF >> (8 - nodesInLastByte));
    return (occurrenceSet[numberOfCompleteBytes] & lastByteMask) == lastByteMask;
}

always_inline OccurrenceManager::iterator OccurrenceManager::begin(ImmutableOccurrenceSet occurrenceSet) const {
    return iterator(occurrenceSet, m_numberOfNodes, 0);
}

#endif // OCCURRENCEMANAGERIMPL_H_
