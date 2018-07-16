// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "MemoryRegion.h"
#include "ParallelHashTableImpl.h"

const size_t g_numberOfLogicalProecssors = ::getNumberOfLogicalProcessors();

static always_inline size_t getNmberOfInsertionsElements() {
    return g_numberOfLogicalProecssors * (static_cast<size_t>(MAX_OBJECT_ID) + 1);
}

static MemoryManager s_memoryManager(sizeof(size_t) * getNmberOfInsertionsElements());

static MemoryRegion<size_t> g_localInsertionCountsRegion(s_memoryManager, 0, getNmberOfInsertionsElements());

size_t* g_localInsertionCounts = g_localInsertionCountsRegion.getData();
