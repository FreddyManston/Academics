// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef OCCURRENCEMANAGER_H_
#define OCCURRENCEMANAGER_H_

#include "../Common.h"
#include "../util/MemoryRegion.h"

class MemoryManager;
class Dictionary;
class DataStore;
class ResourceIDMapper;
class InputSource;
class OutputStream;

typedef uint8_t* OccurrenceSet;
typedef const uint8_t* ImmutableOccurrenceSet;

class OccurrenceManager : private Unmovable {

protected:

    mutable MemoryRegion<uint8_t> m_data;
    const uint8_t m_numberOfNodes;
    const uint8_t m_arity;
    const uint32_t m_setWidth;
    const uint32_t m_resourceWidth;
    std::unique_ptr<uint8_t[]> m_fullSets;

    void extendTo(const size_t resourceIDStart);

public:

    static const NodeID LOAD_RESOURCES_FOR_ALL_NODES = static_cast<NodeID>(-1);

    class iterator {

    protected:

        ImmutableOccurrenceSet m_occurrenceSet;
        uint8_t m_numberOfNodes;
        NodeID m_currentNodeID;

    public:

        iterator(ImmutableOccurrenceSet OccurrenceSet, const uint8_t numberOfNodes, const NodeID currentNodeID = 0);

        iterator(const iterator& other);

        iterator(iterator&& other);

        iterator& operator=(const iterator& other);

        bool operator==(const iterator& other) const;

        bool isAtEnd() const;

        iterator& operator++();
        
        bool operator*() const;

        NodeID getCurrentNodeID() const;

    };

    OccurrenceManager(MemoryManager& memoryManager, const uint8_t numberOfNodes, const uint8_t arity);

    MemoryManager& getMemoryManager();

    void initialize();

    void loadFromInputSource(Prefixes& prefixes, InputSource& inputSource, Dictionary& dictionary, ResourceIDMapper& resourceIDMapper, const NodeID myNodeID);

    void save(Prefixes& prefixes, const Dictionary& dictionary, OutputStream& outputStream) const;

    void save(const Dictionary& dictionary, OutputStream& outputStream) const;

    uint8_t getArity() const;

    size_t getSetWidth() const;

    size_t getResourceWidth() const;

    ResourceID getAfterLastResourceID() const;

    ImmutableOccurrenceSet getOccurrenceSet(const ResourceID resourceID, const uint8_t position) const;

    OccurrenceSet getOccurrenceSetSafe(const ResourceID resourceID, const uint8_t position);

    ImmutableOccurrenceSet getOccurrenceSetSafe(const ResourceID resourceID, const uint8_t position) const;

    bool contains(const ResourceID resourceID, const uint8_t position, const NodeID nodeID) const;

    static bool contains(ImmutableOccurrenceSet occurrenceSet, const NodeID nodeID);

    void add(const ResourceID resourceID, const uint8_t position, const NodeID nodeID);

    static void add(OccurrenceSet occurrenceSet, const NodeID nodeID);
    
    void copyAll(ImmutableOccurrenceSet sourcePosition0OccurrenceSet, OccurrenceSet targetPosition0OccurrenceSet) const;

    void clearAll(OccurrenceSet targetPosition0OccurrenceSet) const;

    void intersectWith(OccurrenceSet targetOccurrenceSet, ImmutableOccurrenceSet intersectWithOccurrenceSet) const;

    void makeFullSet(OccurrenceSet targetOccurrenceSet) const;

    bool isFullSet(ImmutableOccurrenceSet occurrenceSet) const;

    iterator begin(ImmutableOccurrenceSet occurrenceSet) const;

};

#endif // OCCURRENCEMANAGER_H_
