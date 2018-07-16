// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef DATAPOOL_H_
#define DATAPOOL_H_

#include "../Common.h"
#include "../util/MemoryRegion.h"
#include "DictionaryUsageContext.h"

class MemoryManager;
class ComponentStatistics;
class InputStream;
class OutputStream;

class DataPool : private Unmovable {

protected:

    aligned_uint64_t m_nextFreeLocation;
    MemoryRegion<uint8_t> m_data;

public:

    static const uint64_t INVALID_CHUNK_INDEX = 0;

    static const uint64_t USAGE_CONTEXT_WINDOW = 64 * 1024;

    DataPool(MemoryManager& memoryManager);

    void initialize();

    template<bool is_parallel, typename ALIGNMENT_TYPE>
    uint64_t newDataChunk(DictionaryUsageContext* dictionaryUsageContext, const size_t chunkSize, const size_t safetyBuffer = 0);

    uint8_t* getDataFor(const uint64_t chunkIndex);

    const uint8_t* getDataFor(const uint64_t chunkIndex) const;

    void save(OutputStream& outputStream) const;

    void load(InputStream& inputStream);

    std::unique_ptr<ComponentStatistics> getComponentStatistics() const;

    __ALIGNED(DataPool)

};

#endif /* DATAPOOL_H_ */
