// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "IncrementalReasoningState.h"

IncrementalReasoningState::IncrementalReasoningState(MemoryManager& memoryManager) :
    m_globalFlags(memoryManager),
    m_initiallyDeletedByComponentLevel(),
    m_deleteList(memoryManager),
    m_deleteListEndByComponentLevel(),
    m_initiallyAddedByComponentLevel(),
    m_addedList(memoryManager),
    m_addedListEndByComponentLevel(),
    m_updatedEquivalenceClassesSet(memoryManager),
    m_currentLevelFlags(memoryManager),
    m_provedList(memoryManager)
{
}

void IncrementalReasoningState::initializeGlobal(const size_t maxComponentLevel) {
    while (m_initiallyDeletedByComponentLevel.size() <= maxComponentLevel) {
        m_initiallyDeletedByComponentLevel.push_back(std::unique_ptr<LockFreeQueue<TupleIndex> >(new LockFreeQueue<TupleIndex>(m_globalFlags.getMemoryManager())));
        m_deleteListEndByComponentLevel.push_back(0);
        m_initiallyAddedByComponentLevel.push_back(std::unique_ptr<LockFreeQueue<TupleIndex> >(new LockFreeQueue<TupleIndex>(m_globalFlags.getMemoryManager())));
        m_addedListEndByComponentLevel.push_back(0);
    }
    if (!m_globalFlags.initializeLarge())
        throw RDF_STORE_EXCEPTION("Cannot initialize the global flags array.");
    for (auto iterator = m_initiallyDeletedByComponentLevel.begin(); iterator != m_initiallyDeletedByComponentLevel.end(); ++iterator)
        if (!(*iterator)->initializeLarge())
            throw RDF_STORE_EXCEPTION("Cannot initialize the initially deleted list.");
    if (!m_deleteList.initializeLarge())
        throw RDF_STORE_EXCEPTION("Cannot initialize the deleted list.");
    for (auto iterator = m_initiallyAddedByComponentLevel.begin(); iterator != m_initiallyAddedByComponentLevel.end(); ++iterator)
        if (!(*iterator)->initializeLarge())
            throw RDF_STORE_EXCEPTION("Cannot initialize the initially added list.");
    if (!m_addedList.initializeLarge())
        throw RDF_STORE_EXCEPTION("Cannot initialize the deleted list.");
}

void IncrementalReasoningState::initializeCurrentLevel() {
    if (!m_currentLevelFlags.initializeLarge())
        throw RDF_STORE_EXCEPTION("Cannot initialize the current level flags array.");
    if (!m_provedList.initializeLarge())
        throw RDF_STORE_EXCEPTION("Cannot initialize the proved list.");
}
