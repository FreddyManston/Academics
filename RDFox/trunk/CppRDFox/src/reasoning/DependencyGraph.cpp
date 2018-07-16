// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "../RDFStoreException.h"
#include "../util/SequentialHashTableImpl.h"
#include "../util/ThreadContext.h"
#include "RuleIndex.h"
#include "DependencyGraph.h"

// DependencyGraphNode

always_inline void DependencyGraphNode::addBodyOccurrence(BodyLiteralInfo* const bodyLiteralInfo) {
    ++m_bodyOccurrences[bodyLiteralInfo];
}

always_inline void DependencyGraphNode::removeBodyOccurrence(BodyLiteralInfo* const bodyLiteralInfo) {
    auto iterator = m_bodyOccurrences.find(bodyLiteralInfo);
    if ((--iterator->second) == 0)
        m_bodyOccurrences.erase(iterator);
}

always_inline void DependencyGraphNode::addHeadAtomInfo(HeadAtomInfo& headAtomInfo) {
    m_headAtomInfos.push_back(&headAtomInfo);
}

always_inline void DependencyGraphNode::removeHeadAtomInfo(HeadAtomInfo& headAtomInfo) {
    for (auto iterator = m_headAtomInfos.begin(); iterator != m_headAtomInfos.end(); ++iterator)
        if (*iterator == &headAtomInfo) {
            m_headAtomInfos.erase(iterator);
            return;
        }
}

always_inline void DependencyGraphNode::addUnifiesWith(DependencyGraphNode& node) {
    m_unifiesWith.push_back(&node);
}

always_inline void DependencyGraphNode::removeUnifiesWith(DependencyGraphNode& node) {
    for (auto iterator = m_unifiesWith.begin(); iterator != m_unifiesWith.end(); ++iterator)
        if (*iterator == &node) {
            m_unifiesWith.erase(iterator);
            return;
        }
}

bool DependencyGraphNode::unifiesWith(const DependencyGraphNode& node) const {
    if (m_arguments.size() != node.m_arguments.size())
        return false;
    auto iterator1 = m_arguments.begin();
    auto iterator2 = node.m_arguments.begin();
    while (iterator1 != m_arguments.end()) {
        if (*iterator1 != INVALID_RESOURCE_ID && *iterator2 != INVALID_RESOURCE_ID && *iterator1 != *iterator2)
            return false;
        ++iterator1;
        ++iterator2;
    }
    return true;
}

always_inline bool DependencyGraphNode::covers(const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes) const {
    if (m_arguments.size() != argumentIndexes.size())
        return false;
    auto iterator1 = m_arguments.begin();
    auto iterator2 = argumentIndexes.begin();
    while (iterator1 != m_arguments.end()) {
        if (*iterator1 != INVALID_RESOURCE_ID && *iterator1 != argumentsBuffer[*iterator2])
            return false;
        ++iterator1;
        ++iterator2;
    }
    return true;
}

bool DependencyGraphNode::isUnused() const {
    return m_firstOutgoingEdge == nullptr && m_firstIncomingEdge == nullptr && m_headAtomInfos.empty();
}


DependencyGraphNode::DependencyGraphNode(const size_t nodeID, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes) :
    m_nodeID(nodeID),
    m_arguments(),
    m_bodyOccurrences(),
    m_indexedPosition(static_cast<size_t>(-1)),
    m_nextIndexedNode(nullptr),
    m_previousIndexedNode(nullptr),
    m_firstIncomingEdge(nullptr),
    m_firstOutgoingEdge(nullptr),
    m_unifiesWith(),
    m_headAtomInfos(),
    m_componentIndex(0),
    m_componentLevel(0),
    m_dfsIndex(0),
    m_dfsLowlink(0),
    m_dfsOnStack(false)
{
    for (auto iterator = argumentIndexes.begin(); iterator != argumentIndexes.end(); ++iterator)
        const_cast<std::vector<ResourceID>&>(m_arguments).push_back(argumentsBuffer[*iterator]);
    if (argumentIndexes.size() == 3 && argumentsBuffer[argumentIndexes[1]] == RDF_TYPE_ID && argumentsBuffer[argumentIndexes[2]] != INVALID_RESOURCE_ID)
        m_indexedPosition = 2;
    else if (argumentIndexes.size() == 3 && argumentsBuffer[argumentIndexes[1]] != INVALID_RESOURCE_ID)
        m_indexedPosition = 1;
    else {
        for (auto iterator = m_arguments.rbegin(); m_indexedPosition == static_cast<size_t>(-1) && iterator != m_arguments.rend(); ++iterator)
            if (*iterator != INVALID_RESOURCE_ID)
                m_indexedPosition = (m_arguments.rend() - iterator) - 1;
    }
}

// DependencyGraphEdge

DependencyGraphEdge::DependencyGraphEdge(const bool isPositive, DependencyGraphNode& from, DependencyGraphNode& to) :
    m_ruleCount(0), m_isPositive(isPositive), m_from(from), m_to(to),
    m_nextFromEdge(from.m_firstOutgoingEdge), m_previousFromEdge(nullptr),
    m_nextToEdge(to.m_firstIncomingEdge), m_previousToEdge(nullptr)
{
    // Update from
    if (from.m_firstOutgoingEdge != nullptr)
        from.m_firstOutgoingEdge->m_previousFromEdge = this;
    from.m_firstOutgoingEdge = this;
    // Update to
    if (to.m_firstIncomingEdge != nullptr)
        to.m_firstIncomingEdge->m_previousToEdge = this;
    to.m_firstIncomingEdge = this;
}

// DependencyGraph

std::unique_ptr<DependencyGraphNode> DependencyGraph::newNode(const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes) {
    std::unique_ptr<DependencyGraphNode> newNode(new DependencyGraphNode(m_nextNodeID++, argumentsBuffer, argumentIndexes));
    for (const uint8_t* bucket = m_nodes.getFirstBucket(); bucket != m_nodes.getAfterLastBucket(); bucket += m_nodes.getPolicy().BUCKET_SIZE) {
        DependencyGraphNode* node = const_cast<DependencyGraphNode*>(m_nodes.getPolicy().getDependencyGraphNode(bucket));
        if (node != nullptr && newNode->unifiesWith(*node)) {
            newNode->addUnifiesWith(*node);
            node->addUnifiesWith(*newNode);
        }
    }
    if (newNode->m_indexedPosition != static_cast<size_t>(-1)) {
        while (newNode->m_indexedPosition >= m_indexedNodesByPosition.size())
            m_indexedNodesByPosition.emplace_back();
        const ResourceID indexedResourceID = newNode->m_arguments[newNode->m_indexedPosition];
        std::vector<DependencyGraphNode*>& indexedNodes = m_indexedNodesByPosition[newNode->m_indexedPosition];
        if (indexedResourceID >= indexedNodes.size())
            indexedNodes.insert(indexedNodes.end(), indexedResourceID - indexedNodes.size() + 1, nullptr);
        DependencyGraphNode*& indexedNodesHead = indexedNodes[indexedResourceID];
        newNode->m_nextIndexedNode = indexedNodesHead;
        if (indexedNodesHead != nullptr)
            indexedNodesHead->m_previousIndexedNode = newNode.get();
        indexedNodesHead = newNode.get();
    }
    return newNode;
}

DependencyGraphNode& DependencyGraph::getNode(const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes) {
    SequentialHashTable<NodePolicy>::BucketDescriptor bucketDescriptor;
    ThreadContext& threadContext = ThreadContext::getCurrentThreadContext();
    m_nodes.acquireBucket(threadContext, bucketDescriptor, argumentsBuffer, argumentIndexes);
    if (m_nodes.continueBucketSearch(threadContext, bucketDescriptor, argumentsBuffer, argumentIndexes) == BUCKET_EMPTY) {
        bucketDescriptor.m_bucketContents.m_node = newNode(argumentsBuffer, argumentIndexes).release();
        m_nodes.getPolicy().setDependencyGraphNode(bucketDescriptor.m_bucket, bucketDescriptor.m_bucketContents.m_node);
        m_nodes.acknowledgeInsert(threadContext, bucketDescriptor);
    }
    m_nodes.releaseBucket(threadContext, bucketDescriptor);
    return *bucketDescriptor.m_bucketContents.m_node;
}

void DependencyGraph::deleteNode(DependencyGraphNode* node) {
    for (auto iterator = node->m_unifiesWith.begin(); iterator != node->m_unifiesWith.end(); ++iterator)
        (*iterator)->removeUnifiesWith(*node);
    if (node->m_indexedPosition != static_cast<size_t>(-1)) {
        if (node->m_previousIndexedNode != nullptr)
            node->m_previousIndexedNode->m_nextIndexedNode = node->m_nextIndexedNode;
        else {
            const ResourceID indexedResourceID = node->m_arguments[node->m_indexedPosition];
            m_indexedNodesByPosition[node->m_indexedPosition][indexedResourceID] = node->m_nextIndexedNode;
        }
        if (node->m_nextIndexedNode != nullptr)
            node->m_nextIndexedNode->m_previousIndexedNode = node->m_previousIndexedNode;
    }
    SequentialHashTable<NodePolicy>::BucketDescriptor bucketDescriptor;
    ThreadContext& threadContext = ThreadContext::getCurrentThreadContext();
    m_nodes.acquireBucket(threadContext, bucketDescriptor, node);
    m_nodes.continueBucketSearch(threadContext, bucketDescriptor, node);
    m_nodes.deleteBucket(threadContext, bucketDescriptor);
    m_nodes.releaseBucket(threadContext, bucketDescriptor);
    delete node;
}

DependencyGraphEdge& DependencyGraph::getEdge(const bool isPositive, DependencyGraphNode& from, DependencyGraphNode& to) {
    SequentialHashTable<EdgePolicy>::BucketDescriptor bucketDescriptor;
    ThreadContext& threadContext = ThreadContext::getCurrentThreadContext();
    m_edges.acquireBucket(threadContext, bucketDescriptor, isPositive, from, to);
    if (m_edges.continueBucketSearch(threadContext, bucketDescriptor, isPositive, from, to) == BUCKET_EMPTY) {
        bucketDescriptor.m_bucketContents.m_edge = new DependencyGraphEdge(isPositive, from, to);
        m_edges.getPolicy().setDependencyGraphEdge(bucketDescriptor.m_bucket, bucketDescriptor.m_bucketContents.m_edge);
        m_edges.acknowledgeInsert(threadContext, bucketDescriptor);
    }
    m_edges.releaseBucket(threadContext, bucketDescriptor);
    return *bucketDescriptor.m_bucketContents.m_edge;
}

void DependencyGraph::deleteEdge(DependencyGraphEdge* edge) {
    // Update from
    if (edge->m_previousFromEdge == nullptr)
        edge->m_from.m_firstOutgoingEdge = edge->m_nextFromEdge;
    else
        edge->m_previousFromEdge->m_nextFromEdge = edge->m_nextFromEdge;
    if (edge->m_nextFromEdge != nullptr)
        edge->m_nextFromEdge->m_previousFromEdge = edge->m_previousFromEdge;
    // Update to
    if (edge->m_previousToEdge == nullptr)
        edge->m_to.m_firstIncomingEdge = edge->m_nextToEdge;
    else
        edge->m_previousToEdge->m_nextToEdge = edge->m_nextToEdge;
    if (edge->m_nextToEdge != nullptr)
        edge->m_nextToEdge->m_previousToEdge = edge->m_previousToEdge;
    // Delete the edge from the hash table
    SequentialHashTable<EdgePolicy>::BucketDescriptor bucketDescriptor;
    ThreadContext& threadContext = ThreadContext::getCurrentThreadContext();
    m_edges.acquireBucket(threadContext, bucketDescriptor, edge);
    m_edges.continueBucketSearch(threadContext, bucketDescriptor, edge);
    m_edges.deleteBucket(threadContext, bucketDescriptor);
    m_edges.releaseBucket(threadContext, bucketDescriptor);
    delete edge;
}

void DependencyGraph::deleteNodesEdges() {
    if (m_nodes.getFirstBucket() != nullptr) {
        for (const uint8_t* bucket = m_nodes.getFirstBucket(); bucket != m_nodes.getAfterLastBucket(); bucket += m_nodes.getPolicy().BUCKET_SIZE) {
            const DependencyGraphNode* node = m_nodes.getPolicy().getDependencyGraphNode(bucket);
            if (node != nullptr)
                delete node;
        }
    }
    if (m_edges.getFirstBucket() != nullptr) {
        for (const uint8_t* bucket = m_edges.getFirstBucket(); bucket != m_edges.getAfterLastBucket(); bucket += m_edges.getPolicy().BUCKET_SIZE) {
            const DependencyGraphEdge* edge = m_edges.getPolicy().getDependencyGraphEdge(bucket);
            if (edge != nullptr)
                delete edge;
        }
    }
}

always_inline void DependencyGraph::addBodyLiteralInfo(const bool addEdge, RuleInfo& ruleInfo, DependencyGraphNode& headNode, BodyLiteralInfo* bodyLiteralInfo) {
    const FormulaType literalType = bodyLiteralInfo->getLiteral()->getType();
    if (literalType == ATOM_FORMULA) {
        DependencyGraphNode& bodyNode = getNode(ruleInfo.getDefaultArgumentsBuffer(), bodyLiteralInfo->getArgumentIndexes());
        bodyNode.addBodyOccurrence(bodyLiteralInfo);
        if (addEdge) {
            DependencyGraphEdge& edge = getEdge(true, bodyNode, headNode);
            if (++edge.m_ruleCount == 1)
                m_wasUpdated = true;
        }
    }
    else if (literalType == NEGATION_FORMULA || literalType == AGGREGATE_FORMULA) {
        const std::vector<AtomicFormula>& atomicFormulas = (literalType == NEGATION_FORMULA ? to_reference_cast<Negation>(bodyLiteralInfo->getLiteral()).getAtomicFormulas() : to_reference_cast<Aggregate>(bodyLiteralInfo->getLiteral()).getAtomicFormulas());
        for (auto atomicFormulaIterator = atomicFormulas.begin(); atomicFormulaIterator != atomicFormulas.end(); ++atomicFormulaIterator) {
            if ((*atomicFormulaIterator)->getType() == ATOM_FORMULA) {
                const std::vector<Term>& arguments = (*atomicFormulaIterator)->getArguments();
                std::vector<ArgumentIndex> argumentIndexes;
                for (auto argumentIterator = arguments.begin(); argumentIterator != arguments.end(); ++argumentIterator)
                    argumentIndexes.push_back(m_termArray.getPosition(*argumentIterator));
                DependencyGraphNode& bodyNode = getNode(ruleInfo.getDefaultArgumentsBuffer(), argumentIndexes);
                bodyNode.addBodyOccurrence(bodyLiteralInfo);
                if (addEdge) {
                    DependencyGraphEdge& edge = getEdge(false, bodyNode, headNode);
                    if (++edge.m_ruleCount == 1)
                        m_wasUpdated = true;
                }
            }
        }
    }
}

always_inline void DependencyGraph::removeBodyLiteralInfo(const bool removeEdge, RuleInfo& ruleInfo, DependencyGraphNode& headNode, BodyLiteralInfo* bodyLiteralInfo) {
    const FormulaType literalType = bodyLiteralInfo->getLiteral()->getType();
    if (literalType == ATOM_FORMULA) {
        DependencyGraphNode& bodyNode = getNode(ruleInfo.getDefaultArgumentsBuffer(), bodyLiteralInfo->getArgumentIndexes());
        bodyNode.removeBodyOccurrence(bodyLiteralInfo);
        if (removeEdge) {
            DependencyGraphEdge& edge = getEdge(true, bodyNode, headNode);
            if (--edge.m_ruleCount == 0) {
                m_wasUpdated = true;
                deleteEdge(&edge);
                if (bodyNode.isUnused() && &bodyNode != &headNode)
                    deleteNode(&bodyNode);
            }
        }
    }
    else if (literalType == NEGATION_FORMULA || literalType == AGGREGATE_FORMULA) {
        const std::vector<AtomicFormula>& atomicFormulas = (literalType == NEGATION_FORMULA ? to_reference_cast<Negation>(bodyLiteralInfo->getLiteral()).getAtomicFormulas() : to_reference_cast<Aggregate>(bodyLiteralInfo->getLiteral()).getAtomicFormulas());
        for (auto atomicFormulaIterator = atomicFormulas.begin(); atomicFormulaIterator != atomicFormulas.end(); ++atomicFormulaIterator) {
            if ((*atomicFormulaIterator)->getType() == ATOM_FORMULA) {
                const std::vector<Term>& arguments = (*atomicFormulaIterator)->getArguments();
                std::vector<ArgumentIndex> argumentIndexes;
                for (auto argumentIterator = arguments.begin(); argumentIterator != arguments.end(); ++argumentIterator)
                    argumentIndexes.push_back(m_termArray.getPosition(*argumentIterator));
                DependencyGraphNode& bodyNode = getNode(ruleInfo.getDefaultArgumentsBuffer(), argumentIndexes);
                bodyNode.removeBodyOccurrence(bodyLiteralInfo);
                if (removeEdge) {
                    DependencyGraphEdge& edge = getEdge(false, bodyNode, headNode);
                    if (--edge.m_ruleCount == 0) {
                        m_wasUpdated = true;
                        deleteEdge(&edge);
                        if (bodyNode.isUnused() && &bodyNode != &headNode)
                            deleteNode(&bodyNode);
                    }
                }
            }
        }
    }
}

DependencyGraph::DependencyGraph(TermArray& termArray, MemoryManager& memoryManager) :
    m_termArray(termArray),
    m_nextNodeID(0),
    m_nodes(memoryManager, HASH_TABLE_LOAD_FACTOR),
    m_edges(memoryManager, HASH_TABLE_LOAD_FACTOR),
    m_indexedNodesByPosition(),
    m_firstRuleComponentLevel(static_cast<size_t>(-1)),
    m_maxComponentLevel(0),
    m_unstratifiedComponents(),
    m_wasUpdated(false)
{
}

DependencyGraph::~DependencyGraph() {
    deleteNodesEdges();
}

void DependencyGraph::initialize() {
    deleteNodesEdges();
    m_indexedNodesByPosition.clear();
    m_firstRuleComponentLevel = static_cast<size_t>(-1);
    m_nextNodeID = m_maxComponentLevel = 0;
    m_unstratifiedComponents.clear();
    m_wasUpdated = false;
    if (!m_nodes.initialize(HASH_TABLE_INITIAL_SIZE) || !m_edges.initialize(HASH_TABLE_INITIAL_SIZE))
        throw RDF_STORE_EXCEPTION("Cannot initialize DependencyGraph.");
}

void DependencyGraph::addRuleInfo(RuleInfo& ruleInfo) {
    const size_t numberOfHeadAtoms = ruleInfo.getNumberOfHeadAtoms();
    for (size_t index = 0; index < numberOfHeadAtoms; ++index) {
        HeadAtomInfo& headAtomInfo = ruleInfo.getHeadAtomInfo(index);
        DependencyGraphNode& headNode = getNode(ruleInfo.getDefaultArgumentsBuffer(), headAtomInfo.getHeadArgumentIndexes());
        if (headNode.m_headAtomInfos.empty())
            m_wasUpdated = true;
        headNode.addHeadAtomInfo(headAtomInfo);
        std::vector<RuleInfo::PivotPositiveEvaluationPlan>& pivotPositiveEvaluationPlans = ruleInfo.m_pivotPositiveEvaluationPlans;
        std::vector<RuleInfo::PivotNegationEvaluationPlan>& pivotNegationEvaluationPlans = ruleInfo.m_pivotNegationEvaluationPlans;
        // We must go through all plans (both positive and negation) so that all body literals are properly registered in the graph.
        // However, we should add the rule just once, when we process the first plan; this is ensured by the conditions on "addRule".
        for (auto iterator = ruleInfo.m_pivotPositiveEvaluationPlans.begin(); iterator != ruleInfo.m_pivotPositiveEvaluationPlans.end(); ++iterator) {
            BodyLiteralInfo* bodyLiteralInfo = iterator->m_lastBodyLiteralInfo.get();
            while (bodyLiteralInfo != nullptr) {
                addBodyLiteralInfo(iterator == pivotPositiveEvaluationPlans.begin(), ruleInfo, headNode, bodyLiteralInfo);
                bodyLiteralInfo = bodyLiteralInfo->getParent();
            }
        }
        for (auto iterator = pivotNegationEvaluationPlans.begin(); iterator != pivotNegationEvaluationPlans.end(); ++iterator) {
            BodyLiteralInfo* bodyLiteralInfo = iterator->m_lastBodyLiteralInfo.get();
            while (bodyLiteralInfo != nullptr) {
                addBodyLiteralInfo(pivotPositiveEvaluationPlans.empty() && iterator == pivotNegationEvaluationPlans.begin(), ruleInfo, headNode, bodyLiteralInfo);
                bodyLiteralInfo = bodyLiteralInfo->getParent();
            }
        }
    }
}

void DependencyGraph::removeRuleInfo(RuleInfo& ruleInfo) {
    const size_t numberOfHeadAtoms = ruleInfo.getNumberOfHeadAtoms();
    for (size_t index = 0; index < numberOfHeadAtoms; ++index) {
        HeadAtomInfo& headAtomInfo = ruleInfo.getHeadAtomInfo(index);
        DependencyGraphNode& headNode = getNode(ruleInfo.getDefaultArgumentsBuffer(), headAtomInfo.getHeadArgumentIndexes());
        headNode.removeHeadAtomInfo(headAtomInfo);
        std::vector<RuleInfo::PivotPositiveEvaluationPlan>& pivotPositiveEvaluationPlans = ruleInfo.m_pivotPositiveEvaluationPlans;
        std::vector<RuleInfo::PivotNegationEvaluationPlan>& pivotNegationEvaluationPlans = ruleInfo.m_pivotNegationEvaluationPlans;
        // We must undo the effects of addRuleInfo() in the opposite order. Thus, if there is a pivot-positive plan, we remove the rule
        // when we process the last plan; otherwise, we remove the rule when we process the last pivot-negation plan.
        for (auto iterator = pivotNegationEvaluationPlans.rbegin(); iterator != pivotNegationEvaluationPlans.rend(); ++iterator) {
            BodyLiteralInfo* bodyLiteralInfo = iterator->m_lastBodyLiteralInfo.get();
            while (bodyLiteralInfo != nullptr) {
                removeBodyLiteralInfo(pivotPositiveEvaluationPlans.empty() && iterator == pivotNegationEvaluationPlans.rend() - 1, ruleInfo, headNode, bodyLiteralInfo);
                bodyLiteralInfo = bodyLiteralInfo->getParent();
            }
        }
        for (auto iterator = ruleInfo.m_pivotPositiveEvaluationPlans.rbegin(); iterator != ruleInfo.m_pivotPositiveEvaluationPlans.rend(); ++iterator) {
            BodyLiteralInfo* bodyLiteralInfo = iterator->m_lastBodyLiteralInfo.get();
            while (bodyLiteralInfo != nullptr) {
                removeBodyLiteralInfo(iterator == pivotPositiveEvaluationPlans.rend() - 1, ruleInfo, headNode, bodyLiteralInfo);
                bodyLiteralInfo = bodyLiteralInfo->getParent();
            }
        }
        if (headNode.isUnused()) {
            m_wasUpdated = true;
            deleteNode(&headNode);
        }
    }
}

bool DependencyGraph::updateComponents() {
    if (m_wasUpdated) {
        // This is the Tarjan's algorithm, but implemented in a nonrecursive fashion:
        // on large rule sets a recursive version would very likely blow the stack.
        // The recursive version is given here:
        // https://en.wikipedia.org/wiki/Tarjan's_strongly_connected_components_algorithm

        // An auxiliary structure used to encode the DFS state
        struct DFSRecord {
            DependencyGraphNode* m_node;
            const DependencyGraphEdge* m_edge;
            std::vector<DependencyGraphNode*>::const_iterator m_unifiesWith;
            size_t m_returnAddress;

            DFSRecord(DependencyGraphNode* node) : m_node(node), m_edge(m_node->getFirstOutgoingEdge()), m_unifiesWith(m_node->m_unifiesWith.begin()), m_returnAddress(0) {
            }
        };

        // Intialization
        size_t nextComponentIndex = 0;
        size_t dfsIndex = 1;
        std::stack<DFSRecord> dfsStack;
        std::stack<DependencyGraphNode*> explorationStack;
        std::vector<DependencyGraphNode*> sortedNodes;
        for (const uint8_t* bucket = m_nodes.getFirstBucket(); bucket != m_nodes.getAfterLastBucket(); bucket += m_nodes.getPolicy().BUCKET_SIZE) {
            DependencyGraphNode* node = const_cast<DependencyGraphNode*>(m_nodes.getPolicy().getDependencyGraphNode(bucket));
            if (node != nullptr) {
                node->m_componentIndex = node->m_dfsIndex = node->m_dfsLowlink = 0;
                node->m_dfsOnStack = false;
            }
        }
        // Outer loop that iterates through all node
        for (const uint8_t* bucket = m_nodes.getFirstBucket(); bucket != m_nodes.getAfterLastBucket(); bucket += m_nodes.getPolicy().BUCKET_SIZE) {
            DependencyGraphNode* node = const_cast<DependencyGraphNode*>(m_nodes.getPolicy().getDependencyGraphNode(bucket));
            if (node != nullptr && node->m_dfsIndex == 0) {
                // This where strongconnect(v) starts; argument 'v' is in 'node'
                updateComponents_start:
                // Push the node on the stack and initialize
                dfsStack.emplace(node);
                DFSRecord* current = &dfsStack.top();
                current->m_node->m_dfsIndex = current->m_node->m_dfsLowlink = dfsIndex++;
                explorationStack.push(current->m_node);
                current->m_node->m_dfsOnStack = true;
                // Process the regular edges
                while (current->m_edge != nullptr) {
                    if (current->m_edge->getTo().m_dfsIndex == 0) {
                        // Do a recursive call by storing the parameter into 'node' and jumping to the start
                        current->m_returnAddress = 1;
                        node = const_cast<DependencyGraphNode*>(&current->m_edge->getTo());
                        goto updateComponents_start;
                        updateComponents_return_address_1:
                        current->m_node->m_dfsLowlink = std::min(current->m_node->m_dfsLowlink, current->m_edge->getTo().m_dfsLowlink);
                    }
                    else if (current->m_edge->getTo().m_dfsOnStack)
                        current->m_node->m_dfsLowlink = std::min(current->m_node->m_dfsLowlink, current->m_edge->getTo().m_dfsIndex);
                    current->m_edge = current->m_edge->m_nextFromEdge;
                }
                // Process the unification edges
                while (current->m_unifiesWith != current->m_node->m_unifiesWith.end()) {
                    if ((*current->m_unifiesWith)->m_dfsIndex == 0) {
                        // Do a recursive call by storing the parameter into 'node' and jumping to the start
                        current->m_returnAddress = 2;
                        node = *current->m_unifiesWith;
                        goto updateComponents_start;
                        updateComponents_return_address_2:
                        current->m_node->m_dfsLowlink = std::min(current->m_node->m_dfsLowlink, (*current->m_unifiesWith)->m_dfsLowlink);
                    }
                    else if ((*current->m_unifiesWith)->m_dfsOnStack)
                        current->m_node->m_dfsLowlink = std::min(current->m_node->m_dfsLowlink, (*current->m_unifiesWith)->m_dfsIndex);
                    ++current->m_unifiesWith;
                }
                // Produce the current SCC
                if (current->m_node->m_dfsLowlink == current->m_node->m_dfsIndex) {
                    DependencyGraphNode* stackNode;
                    do {
                        stackNode = explorationStack.top();
                        explorationStack.pop();
                        stackNode->m_dfsOnStack = false;
                        stackNode->m_componentIndex = nextComponentIndex;
                        sortedNodes.push_back(stackNode);
                    } while (stackNode != current->m_node);
                    ++nextComponentIndex;
                }
                // This is the end of strongconnect(v) so we pop the stack and return if needed
                dfsStack.pop();
                if (!dfsStack.empty()) {
                    current = &dfsStack.top();
                    switch (current->m_returnAddress) {
                    case 1:
                        goto updateComponents_return_address_1;
                    case 2:
                        goto updateComponents_return_address_2;
                    }
                }
            }
        }
        // We next analyze the components.
        m_firstRuleComponentLevel = static_cast<size_t>(-1);
        m_maxComponentLevel = 0;
        std::set<size_t> unstratifiedComponentIndexes;
        std::vector<size_t> componentLevelsByComponentIndex(nextComponentIndex);
        // Identify components that contain a node with a 'true' (i.e., non-unification) edge.
        // To assign components to levels, we process nodes in topological order and 'push' levels to nodes at the ends of outgoing edges.
        for (auto iterator = sortedNodes.rbegin(); iterator != sortedNodes.rend(); ++iterator) {
            DependencyGraphNode& fromNode = **iterator;
            const size_t fromNodeLevel = fromNode.m_componentLevel = componentLevelsByComponentIndex[fromNode.m_componentIndex];
            for (const DependencyGraphEdge* edge = fromNode.getFirstOutgoingEdge(); edge != nullptr; edge = edge->m_nextFromEdge) {
                DependencyGraphNode& toNode = const_cast<DependencyGraphNode&>(edge->getTo());
                if (fromNode.m_componentIndex > toNode.m_componentIndex)
                    componentLevelsByComponentIndex[toNode.m_componentIndex] = std::max(componentLevelsByComponentIndex[toNode.m_componentIndex], fromNodeLevel + 1);
                // Check stratification.
                if (!edge->isPositive() && fromNode.m_componentIndex == toNode.m_componentIndex)
                    unstratifiedComponentIndexes.insert(fromNode.m_componentIndex);
            }
            // We do not need to iterate over fromNode.m_unifiesWith because all such nodes are in the same component
            if (fromNodeLevel > m_maxComponentLevel)
                m_maxComponentLevel = fromNodeLevel;
        }
        // We finally update the atoms and the rules.
        for (auto iterator = sortedNodes.begin(); iterator != sortedNodes.end(); ++iterator) {
            DependencyGraphNode& node = **iterator;
            const size_t componentLevel = node.m_componentLevel;
            for (auto iterator = node.m_bodyOccurrences.begin(); iterator != node.m_bodyOccurrences.end(); ++iterator)
                iterator->first->m_componentLevel = componentLevel;
            if (!node.m_headAtomInfos.empty()) {
                if (m_firstRuleComponentLevel > componentLevel)
                    m_firstRuleComponentLevel = componentLevel;
                for (auto iterator = node.m_headAtomInfos.begin(); iterator != node.m_headAtomInfos.end(); ++iterator)
                    (*iterator)->m_componentLevel = componentLevel;
            }
        }
        // Record unstratified componetns, if any exist.
        m_unstratifiedComponents.clear();
        if (!unstratifiedComponentIndexes.empty()) {
            std::unordered_map<size_t, std::vector<const DependencyGraphNode*> > nodesByComponentIndex;
            for (auto iterator = sortedNodes.rbegin(); iterator != sortedNodes.rend(); ++iterator)
                nodesByComponentIndex[(*iterator)->m_componentIndex].push_back(*iterator);
            for (auto iterator = unstratifiedComponentIndexes.begin(); iterator != unstratifiedComponentIndexes.end(); ++iterator)
                m_unstratifiedComponents.emplace_back(nodesByComponentIndex[*iterator]);
        }
        m_wasUpdated = false;
        return true;
    }
    else
        return false;
}

size_t DependencyGraph::getComponentLevel(const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes) const {
    auto iterator1 = argumentIndexes.begin();
    auto iterator2 = m_indexedNodesByPosition.begin();
    while (iterator1 != argumentIndexes.end() && iterator2 != m_indexedNodesByPosition.end()) {
        const std::vector<DependencyGraphNode*>& indexedNodes = *iterator2;
        const ResourceID resourceID = argumentsBuffer[*iterator1];
        if (resourceID < indexedNodes.size()) {
            for (DependencyGraphNode* node = indexedNodes[resourceID]; node != nullptr; node = node->m_nextIndexedNode)
                if (node->covers(argumentsBuffer, argumentIndexes))
                    return node->m_componentLevel;
        }
        ++iterator1;
        ++iterator2;
    }
    return 0;
}
