// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include <metis.h>

#undef INT32_MIN
#undef INT32_MAX
#undef INT64_MIN
#undef INT64_MAX

#include "../all.h"
#include "../RDFStoreException.h"
#include "ResourceIDGraph.h"

// ResourceIDPartition

static_assert(sizeof(int64_t) == sizeof(idx_t), "The sizes of int64_t and idx_t do not match!");

ResourceIDPartition::ResourceIDPartition(void* const partitionData) :
    m_partitionData(partitionData)
{
}

ResourceIDPartition::ResourceIDPartition(ResourceIDPartition&& other) :
    m_partitionData(other.m_partitionData)
{
    other.m_partitionData = nullptr;
}

ResourceIDPartition::~ResourceIDPartition() {
    delete [] reinterpret_cast<idx_t*>(m_partitionData);
}

ResourceIDPartition& ResourceIDPartition::operator=(ResourceIDPartition&& other) {
    if (this != &other) {
        delete [] reinterpret_cast<idx_t*>(m_partitionData);
        m_partitionData = other.m_partitionData;
        other.m_partitionData = nullptr;
    }
    return *this;
}

NodeID ResourceIDPartition::operator[](const ResourceID resourceID) const {
    return static_cast<NodeID>(reinterpret_cast<idx_t*>(m_partitionData)[resourceID]);
}

// ResourceIDGraph

ResourceIDGraph::ResourceIDGraph(const bool useVertexWeights) :
    m_useVertexWeights(useVertexWeights),
    m_adjacencyArray(),
    m_vertexWeights(),
    m_edgeCount(0)
{
}

void ResourceIDGraph::addNode(const ResourceID resourceID, const bool incrementNodeWeight) {
    while (m_adjacencyArray.size() <= resourceID)
        m_adjacencyArray.push_back(std::unique_ptr<std::unordered_set<ResourceID> >(new std::unordered_set<ResourceID>()));
    if (m_useVertexWeights) {
        while (m_vertexWeights.size() <= resourceID)
            m_vertexWeights.push_back(0);
        if (incrementNodeWeight)
            ++m_vertexWeights[resourceID];
    }
}

void ResourceIDGraph::addEdge(const ResourceID subjectID, const ResourceID objectID) {
    if (m_adjacencyArray[subjectID]->insert(objectID).second)
        ++m_edgeCount;
    if (m_adjacencyArray[objectID]->insert(subjectID).second)
        ++m_edgeCount;
}

ResourceIDPartition ResourceIDGraph::partition(std::ostream* output, const size_t numberOfPartitions) {
    // Convert adjacency graph to METIS inputs
    const size_t nodeCount = m_adjacencyArray.size();
    const size_t edgeCount = m_edgeCount;
    std::unique_ptr<idx_t[]> xadj(new idx_t[nodeCount + 1]); // The adjacency list start points
    std::unique_ptr<idx_t[]> adjncy(new idx_t[edgeCount]); // The adjacency list
    xadj[0] = 0;
    size_t nodeIndex = 0;
    size_t adjacencyOffset = 0;
    if (output != nullptr)
        *output << "Converting adjacency graph to METIS input." << std::endl;
    for (auto sourceIterator = m_adjacencyArray.begin(); sourceIterator != m_adjacencyArray.end(); ++sourceIterator) {
        std::unordered_set<ResourceID>& adjacencyList = **sourceIterator;
        for (auto targetIterator = adjacencyList.begin(); targetIterator != adjacencyList.end(); ++targetIterator ) {
            adjncy[adjacencyOffset] = *targetIterator;
            ++adjacencyOffset;
        }
        ++nodeIndex;
        if (output != nullptr && nodeIndex % 1000000 == 0)
            *output << nodeIndex << "/" << nodeCount << " nodes converted." << std::endl;
        xadj[nodeIndex] = adjacencyOffset;
    }
    // Free the graph
    std::vector<int64_t> vertexWeights;
    {
        m_vertexWeights.swap(vertexWeights);
        unique_ptr_vector<std::unordered_set<ResourceID> > emptyVector;
        m_adjacencyArray.swap(emptyVector);
        m_edgeCount = 0;
    }
    idx_t nvtxs = static_cast<idx_t>(nodeCount); // The number of vertices in the graph
    idx_t ncon = 1; // The number of balancing constraints. It should be at least 1
    idx_t *vsize = nullptr; // The size of the vertices for computing the total communication volume
    idx_t *adjwgt = nullptr; // The weights of the edges
    idx_t nparts = static_cast<idx_t>(numberOfPartitions); // The number of parts to partition the graph.
    real_t *tpwgts = nullptr;
    real_t *ubvec = nullptr;
    idx_t *options = nullptr;
    idx_t objval; // This variable stores the edge-cut or the total communication volume of the partitioning solution
    std::unique_ptr<idx_t[]> part(new idx_t[nvtxs]); // This is a vector that stores the partition vector of the graph
    // Partition with METIS
    if (output != nullptr)
        *output << "Partitioning with METIS." << std::endl;
    int runStatus;
    if (m_useVertexWeights)
        runStatus = ::METIS_PartGraphKway(&nvtxs, &ncon, xadj.get(), adjncy.get(), &vertexWeights[0], vsize, adjwgt, &nparts, tpwgts, ubvec, options, & objval, part.get());
    else
        runStatus = ::METIS_PartGraphKway(&nvtxs, &ncon, xadj.get(), adjncy.get(), nullptr, vsize, adjwgt, &nparts, tpwgts, ubvec, options, & objval, part.get());
    switch (runStatus) {
    case METIS_ERROR_INPUT:
    case METIS_ERROR_MEMORY:
    case METIS_ERROR:
        throw RDF_STORE_EXCEPTION("Cannot partition the graph due to an error in METIS.");
    }
    ResourceIDPartition resourceIDPartition(part.get());
    part.release();
    return resourceIDPartition;
}
