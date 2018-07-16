// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef RESOURCEIDGRAPH_H_
#define RESOURCEIDGRAPH_H_

#include "../Common.h"

// ResourceIDPartition

class ResourceIDPartition {

protected:

    void* m_partitionData;

public:

    ResourceIDPartition(void* const partitionData);

    ResourceIDPartition(const ResourceIDPartition& other) = delete;

    ResourceIDPartition(ResourceIDPartition&& other);

    ~ResourceIDPartition();

    ResourceIDPartition& operator=(const ResourceIDPartition& other) = delete;

    ResourceIDPartition& operator=(ResourceIDPartition&& other);

    NodeID operator[](const ResourceID resourceID) const;
    
};

// ResourceIDGraph

class ResourceIDGraph : private Unmovable {

protected:

    const bool m_useVertexWeights;
    unique_ptr_vector<std::unordered_set<ResourceID> > m_adjacencyArray;
    std::vector<int64_t> m_vertexWeights;
    size_t m_edgeCount; // Counts each edge twice (as they are stored twice)

public:

    ResourceIDGraph(const bool useVertexWeights);

    void addNode(const ResourceID resourceID, const bool incrementNodeWeight);

    void addEdge(const ResourceID resourceID1, const ResourceID resourceID2);
    
    ResourceIDPartition partition(std::ostream* output, const size_t numberOfPartitions);

};

#endif /* RESOURCEIDGRAPH_H_ */
