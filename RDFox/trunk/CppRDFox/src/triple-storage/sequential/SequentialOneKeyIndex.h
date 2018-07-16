// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef SEQUENTIALONEKEYINDEX_H_
#define SEQUENTIALONEKEYINDEX_H_

#include "../../Common.h"
#include "../../util/MemoryRegion.h"

class MemoryManager;
class ComponentStatistics;
class InputStream;
class OutputStream;

class SequentialOneKeyIndex : private Unmovable {

protected:

    MemoryRegion<uint8_t> m_buckets;

public:

    SequentialOneKeyIndex(MemoryManager& memoryManager);

    MemoryManager& getMemoryManager() const;

    bool initialize();

    bool clearCounts();

    bool extendToResourceID(const ResourceID resourceID);

    TupleIndex getHeadTripleIndex(const ResourceID resourceID) const;

    void setHeadTripleIndex(const ResourceID resourceID, const TupleIndex tupleIndex);

    bool setHeadTripleIndexConditional(const ResourceID resourceID, const TupleIndex testTripleIndex, const TupleIndex tupleIndex);

    size_t getTripleCount(const ResourceID resourceID) const;

    size_t incrementTripleCount(const ResourceID resourceID, const uint32_t amount = 1);

    size_t decrementTripleCount(const ResourceID resourceID, const uint32_t amount = 1);

    void save(OutputStream& outputStream) const;

    void load(InputStream& inputStream);

    void printContents(std::ostream& output) const;

    std::unique_ptr<ComponentStatistics> getComponentStatistics() const;

};

#endif /* SEQUENTIALONEKEYINDEX_H_ */
