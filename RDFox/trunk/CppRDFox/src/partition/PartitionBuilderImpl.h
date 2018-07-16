// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "../RDFStoreException.h"
#include "../dictionary/Dictionary.h"
#include "../node-server/OccurrenceManagerImpl.h"
#include "PartitionBuilder.h"

// PartitionBuilder::PartitionsAndOwnerwshipAssignment

template<class O>
template<typename... Args>
always_inline PartitionBuilder<O>::PartitionsAndOwnerwshipAssignment::PartitionsAndOwnerwshipAssignment(const std::vector<InputConsumer*>& partitions, Args&&... ownershipAssignmentArguments) :
    O(std::forward<Args>(ownershipAssignmentArguments)...),
    m_partitions(partitions)
{
}

// PartitionBuilder

template<class O>
always_inline void PartitionBuilder<O>::addOwner(const ResourceID resourceID, const uint8_t position, const NodeID nodeID) {
    if (m_seenResourceIDs.add<false>(resourceID))
        m_occurrenceManager.clearAll(m_occurrenceManager.getOccurrenceSetSafe(resourceID, 0));
    m_occurrenceManager.add(m_occurrenceManager.getOccurrenceSetSafe(resourceID, position), nodeID);
}

template<class O>
template<typename... Args>
PartitionBuilder<O>::PartitionBuilder(Dictionary& dictionary, OccurrenceManager& occurrenceManager, const std::vector<InputConsumer*>& partitions, Args&&... ownershipAssignmentArguments) :
    m_dictionary(dictionary),
    m_occurrenceManager(occurrenceManager),
    m_partitionsAndOwnershipAssignment(partitions, std::forward<Args>(ownershipAssignmentArguments)...),
    m_seenResourceIDs(occurrenceManager.getMemoryManager())
{
    if (!m_seenResourceIDs.initializeLarge())
        throw RDF_STORE_EXCEPTION("Cannot initialize the bit-set for visited resources.");
}

template<class O>
PartitionBuilder<O>::~PartitionBuilder() {
}

template<class O>
void PartitionBuilder<O>::start() {
    for (auto iterator = m_partitionsAndOwnershipAssignment.m_partitions.begin(); iterator != m_partitionsAndOwnershipAssignment.m_partitions.end(); ++iterator)
        (*iterator)->start();
}

template<class O>
void PartitionBuilder<O>::reportError(const size_t line, const size_t column, const char* const errorDescription) {
    std::ostringstream message;
    message << "Error at line = " << line << ", column = " << column << ": " << errorDescription;
    throw RDF_STORE_EXCEPTION(message.str());
}

template<class O>
void PartitionBuilder<O>::consumeTriple(const size_t line, const size_t column, const ResourceText& subject, const ResourceText& predicate, const ResourceText& object) {
    const ResourceID subjectID = m_dictionary.resolveResource(subject);
    const ResourceID predicateID = m_dictionary.resolveResource(predicate);
    const ResourceID objectID = m_dictionary.resolveResource(object);
    const NodeID nodeID = m_partitionsAndOwnershipAssignment.getOwner(subjectID, predicateID, objectID);
    addOwner(subjectID, 0, nodeID);
    addOwner(predicateID, 1, nodeID);
    addOwner(objectID, 2, nodeID);
    m_partitionsAndOwnershipAssignment.m_partitions[nodeID]->consumeTriple(line, column, subject, predicate, object);
}

template<class O>
void PartitionBuilder<O>::consumeRule(const size_t line, const size_t column, const Rule& rule) {
}

template<class O>
void PartitionBuilder<O>::finish() {
    for (auto iterator = m_partitionsAndOwnershipAssignment.m_partitions.begin(); iterator != m_partitionsAndOwnershipAssignment.m_partitions.end(); ++iterator)
        (*iterator)->finish();
}
