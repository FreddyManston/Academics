// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "TripleTableImpl.h"

void UnformattedLoadingThread::run() {
    std::vector<ResourceID> argumentsBuffer;
    argumentsBuffer.insert(argumentsBuffer.begin(), 3, INVALID_RESOURCE_ID);
    std::vector<ArgumentIndex> argumentIndexes;
    argumentIndexes.push_back(0);
    argumentIndexes.push_back(1);
    argumentIndexes.push_back(2);
    for (const uint8_t* current = m_firstResource; current != m_afterLastResource;) {
        const TupleStatus tupleStatus = *reinterpret_cast<const TupleStatus*>(current);
        current += sizeof(TupleStatus);
        argumentsBuffer[0] = *reinterpret_cast<const ResourceID*>(current);
        current += sizeof(ResourceID);
        argumentsBuffer[1] = *reinterpret_cast<const ResourceID*>(current);
        current += sizeof(ResourceID);
        argumentsBuffer[2] = *reinterpret_cast<const ResourceID*>(current);
        current += sizeof(ResourceID);
        m_table.addTuple(argumentsBuffer, argumentIndexes, 0, tupleStatus);
    }
}

