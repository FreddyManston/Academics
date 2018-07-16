// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef PARTITIONBUILDER_H_
#define PARTITIONBUILDER_H_

#include "../Common.h"
#include "../formats/InputConsumer.h"
#include "../util/LockFreeBitSet.h"

class Dictionary;
class OccurrenceManager;

template<class O>
class PartitionBuilder : public InputConsumer {
    
protected:

    struct PartitionsAndOwnerwshipAssignment : public O {
        const std::vector<InputConsumer*>& m_partitions;

        template<typename... Args>
        PartitionsAndOwnerwshipAssignment(const std::vector<InputConsumer*>& partitions, Args&&... ownershipAssignmentArguments);

    };

    Dictionary& m_dictionary;
    OccurrenceManager& m_occurrenceManager;
    PartitionsAndOwnerwshipAssignment m_partitionsAndOwnershipAssignment;
    LockFreeBitSet m_seenResourceIDs;

    void addOwner(const ResourceID resourceID, const uint8_t position, const NodeID nodeID);

public:

    template<typename... Args>
    PartitionBuilder(Dictionary& dictionary, OccurrenceManager& occurrenceManager, const std::vector<InputConsumer*>& partitions, Args&&... ownershipAssignmentArguments);
    
    virtual ~PartitionBuilder();
    
    virtual void start();
    
    virtual void reportError(const size_t line, const size_t column, const char* const errorDescription);
    
    virtual void consumeTriple(const size_t line, const size_t column, const ResourceText& subject, const ResourceText& predicate, const ResourceText& object);
    
    virtual void consumeRule(const size_t line, const size_t column, const Rule& rule);
    
    virtual void finish();
    
};

#endif /* PARTITIONBUILDER_H_ */
