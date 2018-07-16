// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef SEQUENTIALTRIPLELIST_H_
#define SEQUENTIALTRIPLELIST_H_

#include "../../Common.h"
#include "../../util/MemoryRegion.h"

class MemoryManager;
class ComponentStatistics;
class InputStream;
class OutputStream;

class SequentialTripleList : private Unmovable {

public:

    typedef ResourceID StoreResourceIDType;
    typedef TupleIndex StoreTripleIndexType;

protected:

    MemoryRegion<uint8_t> m_data;
    TupleIndex m_firstFreeTripleIndex;

public:

    static const bool IS_CONCURRENT = false;

    SequentialTripleList(MemoryManager& memoryManager);

    bool initialize(const size_t initialNumberOfTriples);

    TupleStatus getTripleStatus(const TupleIndex tupleIndex) const;

    TupleStatus setTripleStatus(const TupleIndex tupleIndex, const TupleStatus tupleStatus);

    bool setTripleStatusConditional(const TupleIndex tupleIndex, const TupleStatus expectedTupleStatus, const TupleStatus newTupleStatus);

    void getTriple(const TupleIndex tupleIndex, TupleStatus& tupleStatus, ResourceID& s, ResourceID& p, ResourceID& o, TupleIndex& nextS, TupleIndex& nextP, TupleIndex& nextO) const;

    void getTripleStatusAndResourceIDs(const TupleIndex tupleIndex, TupleStatus& tupleStatus, ResourceID& s, ResourceID& p, ResourceID& o) const;

    void getResourceIDs(const TupleIndex tupleIndex, ResourceID& s, ResourceID& p, ResourceID& o) const;

    void getResourceIDs(const TupleIndex tupleIndex, const ResourceComponent component1, const ResourceComponent component2, ResourceID& value1, ResourceID& value2) const;

    ResourceID getResourceID(const TupleIndex tupleIndex, const ResourceComponent component) const;

    ResourceID getS(const TupleIndex tupleIndex) const;

    ResourceID getP(const TupleIndex tupleIndex) const;

    ResourceID getO(const TupleIndex tupleIndex) const;

    TupleIndex getNext(const TupleIndex tupleIndex, const ResourceComponent component) const;

    void setNext(const TupleIndex tupleIndex, const ResourceComponent component, const TupleIndex nextTripleIndex);

    bool setNextConditional(const TupleIndex tupleIndex, const ResourceComponent component, const TupleIndex testTripleIndex, const TupleIndex nextTripleIndex);

    TupleIndex getFirstWriteTripleIndex() const;

    TupleIndex getFirstTripleIndex() const;

    TupleIndex getFirstFreeTripleIndex() const;

    TupleIndex getNextTripleIndex(const TupleIndex tupleIndex) const;

    TupleIndex add(const ResourceID s, const ResourceID p, const ResourceID o);

    bool supportsWindowedAdds() const;

    TupleIndex reserveAddWindow(const size_t windowSize);

    void addAt(const TupleIndex tupleIndex, const ResourceID s, const ResourceID p, const ResourceID o);

    void truncate(const TupleIndex newFirstFreeTripleIndex);

    size_t getExactTripleCount(const TupleStatus tupleStatusMask, const TupleStatus tupleStatusExpectedValue) const;

    size_t getApproximateTripleCount() const;

    void save(OutputStream& outputStream) const;

    void load(InputStream& inputStream);

    std::unique_ptr<ComponentStatistics> getComponentStatistics() const;

};

#endif /* SEQUENTIALTRIPLELIST_H_ */
