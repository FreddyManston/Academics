// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef DEPENDENCYGRAPH_H_
#define DEPENDENCYGRAPH_H_

#include "../Common.h"
#include "../util/SequentialHashTable.h"

class MemoryManager;
class BodyLiteralInfo;
class HeadAtomInfo;
class RuleInfo;
class DependencyGraphEdge;

// DependencyGraphNode

class DependencyGraphNode : private Unmovable {

    friend class DependencyGraphEdge;
    friend class DependencyGraph;

protected:

    const size_t m_nodeID;
    const std::vector<ResourceID> m_arguments;
    std::unordered_map<BodyLiteralInfo*, size_t> m_bodyOccurrences;
    size_t m_indexedPosition;
    DependencyGraphNode* m_nextIndexedNode;
    DependencyGraphNode* m_previousIndexedNode;
    DependencyGraphEdge* m_firstIncomingEdge;
    DependencyGraphEdge* m_firstOutgoingEdge;
    std::vector<DependencyGraphNode*> m_unifiesWith;
    std::vector<HeadAtomInfo*> m_headAtomInfos;
    size_t m_componentIndex;
    size_t m_componentLevel;
    size_t m_dfsIndex;
    size_t m_dfsLowlink;
    bool m_dfsOnStack;

    void addBodyOccurrence(BodyLiteralInfo* const bodyLiteralInfo);

    void removeBodyOccurrence(BodyLiteralInfo* const bodyLiteralInfo);

    void addHeadAtomInfo(HeadAtomInfo& headAtomInfo);

    void removeHeadAtomInfo(HeadAtomInfo& headAtomInfo);

    void addUnifiesWith(DependencyGraphNode& node);

    void removeUnifiesWith(DependencyGraphNode& node);

    bool unifiesWith(const DependencyGraphNode& node) const;

    bool covers(const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes) const;
    
    bool isUnused() const;

public:

    DependencyGraphNode(const size_t nodeID, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes);

    always_inline size_t getNodeID() const {
        return m_nodeID;
    }

    always_inline const std::vector<ResourceID>& getArguments() const {
        return m_arguments;
    }

    always_inline const DependencyGraphEdge* getFirstIncomingEdge() const {
        return m_firstIncomingEdge;
    }

    always_inline const DependencyGraphEdge* getFirstOutgoingEdge() const {
        return m_firstOutgoingEdge;
    }

    always_inline const std::vector<DependencyGraphNode*>& getUnifiesWith() const {
        return m_unifiesWith;
    }

    always_inline size_t getComponentLevel() const {
        return m_componentLevel;
    }

    always_inline const std::vector<HeadAtomInfo*>& getHeadAtomInfos() const {
        return m_headAtomInfos;
    }

};

// DependencyGraphEdge

class DependencyGraphEdge : private Unmovable {

    friend class DependencyGraphNode;
    friend class DependencyGraph;

protected:

    size_t m_ruleCount;
    const bool m_isPositive;
    DependencyGraphNode& m_from;
    DependencyGraphNode& m_to;
    DependencyGraphEdge* m_nextFromEdge;
    DependencyGraphEdge* m_previousFromEdge;
    DependencyGraphEdge* m_nextToEdge;
    DependencyGraphEdge* m_previousToEdge;

public:

    DependencyGraphEdge(const bool isPositive, DependencyGraphNode& from, DependencyGraphNode& to);

    always_inline const DependencyGraphNode& getFrom() const {
        return m_from;
    }

    always_inline const DependencyGraphNode& getTo() const {
        return m_to;
    }

    always_inline bool isPositive() const {
        return m_isPositive;
    }

};

// DependencyGraph

class DependencyGraph : private Unmovable {

protected:

    class NodePolicy {

    public:

        static const size_t BUCKET_SIZE = sizeof(DependencyGraphNode*);

        struct BucketContents {
            DependencyGraphNode* m_node;
        };

        always_inline static void getBucketContents(const uint8_t* const bucket, BucketContents& bucketContents) {
            bucketContents.m_node = *reinterpret_cast<DependencyGraphNode* const *>(bucket);
        }

        always_inline static BucketStatus getBucketContentsStatus(const BucketContents& bucketContents, const size_t valuesHashCode, DependencyGraphNode* const node) {
            if (bucketContents.m_node == nullptr)
                return BUCKET_EMPTY;
            else if (bucketContents.m_node == node)
                return BUCKET_CONTAINS;
            else
                return BUCKET_NOT_CONTAINS;
        }

        always_inline static BucketStatus getBucketContentsStatus(const BucketContents& bucketContents, const size_t valuesHashCode, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes) {
            if (bucketContents.m_node == nullptr)
                return BUCKET_EMPTY;
            else {
                const std::vector<ResourceID>& nodeArguments = bucketContents.m_node->getArguments();
                if (nodeArguments.size() != argumentIndexes.size())
                    return BUCKET_NOT_CONTAINS;
                auto nodeArgumentsIterator = nodeArguments.begin();
                auto argumentIndexesIterator = argumentIndexes.begin();
                while (nodeArgumentsIterator != nodeArguments.end()) {
                    if (*nodeArgumentsIterator != argumentsBuffer[*argumentIndexesIterator])
                        return BUCKET_NOT_CONTAINS;
                    ++nodeArgumentsIterator;
                    ++argumentIndexesIterator;
                }
                return BUCKET_CONTAINS;
            }
        }

        always_inline static bool setBucketContentsIfEmpty(uint8_t* const bucket, const BucketContents& bucketContents) {
            if (getDependencyGraphNode(bucket) == nullptr) {
                setDependencyGraphNode(bucket, bucketContents.m_node);
                return true;
            }
            else
                return false;
        }

        always_inline static bool isBucketContentsEmpty(const BucketContents& bucketContents) {
            return bucketContents.m_node == nullptr;
        }

        always_inline static size_t getBucketContentsHashCode(const BucketContents& bucketContents) {
            return hashCodeFor(bucketContents.m_node);
        }

        always_inline static size_t hashCodeFor(DependencyGraphNode* node) {
            const std::vector<ResourceID>& nodeArguments = node->getArguments();
            size_t hash = 0;
            for (auto iterator = nodeArguments.begin(); iterator != nodeArguments.end(); ++iterator) {
                hash += *iterator;
                hash += (hash << 10);
                hash ^= (hash >> 6);
            }
            hash += (hash << 3);
            hash ^= (hash >> 11);
            hash += (hash << 15);
            return hash;
        }

        always_inline static size_t hashCodeFor(const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes) {
            size_t hash = 0;
            for (auto iterator = argumentIndexes.begin(); iterator != argumentIndexes.end(); ++iterator) {
                hash += argumentsBuffer[*iterator];
                hash += (hash << 10);
                hash ^= (hash >> 6);
            }
            hash += (hash << 3);
            hash ^= (hash >> 11);
            hash += (hash << 15);
            return hash;
        }

        always_inline static void makeBucketEmpty(uint8_t* const bucket) {
            setDependencyGraphNode(bucket, nullptr);
        }

        always_inline static const DependencyGraphNode* getDependencyGraphNode(const uint8_t* const bucket) {
            return *reinterpret_cast<const DependencyGraphNode* const *>(bucket);
        }

        always_inline static void setDependencyGraphNode(uint8_t* const bucket, DependencyGraphNode* const node) {
            *reinterpret_cast<DependencyGraphNode**>(bucket) = node;
        }

    };

    class EdgePolicy {

    public:

        static const size_t BUCKET_SIZE = sizeof(DependencyGraphEdge*);

        struct BucketContents {
            DependencyGraphEdge* m_edge;
        };

        always_inline static void getBucketContents(const uint8_t* const bucket, BucketContents& bucketContents) {
            bucketContents.m_edge = *reinterpret_cast<DependencyGraphEdge* const *>(bucket);
        }

        always_inline static BucketStatus getBucketContentsStatus(const BucketContents& bucketContents, const size_t valuesHashCode, DependencyGraphEdge* const edge) {
            if (bucketContents.m_edge == nullptr)
                return BUCKET_EMPTY;
            else if (bucketContents.m_edge == edge)
                return BUCKET_CONTAINS;
            else
                return BUCKET_NOT_CONTAINS;
        }

        always_inline static BucketStatus getBucketContentsStatus(const BucketContents& bucketContents, const size_t valuesHashCode, const bool isPositive, const DependencyGraphNode& from, const DependencyGraphNode& to) {
            if (bucketContents.m_edge == nullptr)
                return BUCKET_EMPTY;
            else if (bucketContents.m_edge->isPositive() == isPositive && &bucketContents.m_edge->getFrom() == &from && &bucketContents.m_edge->getTo() == &to)
                return BUCKET_CONTAINS;
            else
                return BUCKET_NOT_CONTAINS;
        }

        always_inline static bool setBucketContentsIfEmpty(uint8_t* const bucket, const BucketContents& bucketContents) {
            if (getDependencyGraphEdge(bucket) == nullptr) {
                setDependencyGraphEdge(bucket, bucketContents.m_edge);
                return true;
            }
            else
                return false;
        }

        always_inline static bool isBucketContentsEmpty(const BucketContents& bucketContents) {
            return bucketContents.m_edge == nullptr;
        }

        always_inline static size_t getBucketContentsHashCode(const BucketContents& bucketContents) {
            return hashCodeFor(bucketContents.m_edge);
        }

        always_inline static size_t hashCodeFor(DependencyGraphEdge* edge) {
            return hashCodeFor(edge->isPositive(), edge->getFrom(), edge->getTo());
        }

        always_inline static size_t hashCodeFor(const bool isPositive, const DependencyGraphNode& from, const DependencyGraphNode& to) {
            size_t hash = 0;

            hash += static_cast<size_t>(isPositive);
            hash += (hash << 10);
            hash ^= (hash >> 6);

            hash += from.getNodeID();
            hash += (hash << 10);
            hash ^= (hash >> 6);

            hash += to.getNodeID();
            hash += (hash << 10);
            hash ^= (hash >> 6);

            hash += (hash << 3);
            hash ^= (hash >> 11);
            hash += (hash << 15);

            return hash;
        }

        always_inline static void makeBucketEmpty(uint8_t* const bucket) {
            setDependencyGraphEdge(bucket, nullptr);
        }
        
        always_inline static const DependencyGraphEdge* getDependencyGraphEdge(const uint8_t* const bucket) {
            return *reinterpret_cast<const DependencyGraphEdge* const *>(bucket);
        }

        always_inline static void setDependencyGraphEdge(uint8_t* const bucket, DependencyGraphEdge* const edge) {
            *reinterpret_cast<DependencyGraphEdge**>(bucket) = edge;
        }

    };

    const TermArray& m_termArray;
    size_t m_nextNodeID;
    SequentialHashTable<NodePolicy> m_nodes;
    SequentialHashTable<EdgePolicy> m_edges;
    std::vector<std::vector<DependencyGraphNode*> > m_indexedNodesByPosition;
    size_t m_firstRuleComponentLevel;
    size_t m_maxComponentLevel;
    std::vector<std::vector<const DependencyGraphNode*> > m_unstratifiedComponents;
    bool m_wasUpdated;

    std::unique_ptr<DependencyGraphNode> newNode(const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes);

    DependencyGraphNode& getNode(const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes);

    void deleteNode(DependencyGraphNode* node);

    DependencyGraphEdge& getEdge(const bool isPositive, DependencyGraphNode& from, DependencyGraphNode& to);

    void deleteEdge(DependencyGraphEdge* edge);

    void deleteNodesEdges();

    void addBodyLiteralInfo(const bool addEdge, RuleInfo& ruleInfo, DependencyGraphNode& headNode, BodyLiteralInfo* bodyLiteralInfo);

    void removeBodyLiteralInfo(const bool addEdge, RuleInfo& ruleInfo, DependencyGraphNode& headNode, BodyLiteralInfo* bodyLiteralInfo);

public:

    DependencyGraph(TermArray& termArray, MemoryManager& memoryManager);

    ~DependencyGraph();

    void initialize();

    void addRuleInfo(RuleInfo& ruleInfo);

    void removeRuleInfo(RuleInfo& ruleInfo);

    bool updateComponents();

    always_inline size_t getFirstRuleComponentLevel() const {
        return m_firstRuleComponentLevel;
    }

    always_inline size_t getMaxComponentLevel() const {
        return m_maxComponentLevel;
    }

    always_inline bool isStratified() const {
        return m_unstratifiedComponents.empty();
    }

    always_inline const std::vector<std::vector<const DependencyGraphNode*> >& getUnstratifiedComponents() const {
        return m_unstratifiedComponents;
    }

    size_t getComponentLevel(const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes) const;

};

#endif /* DEPENDENCYGRAPH_H_ */
