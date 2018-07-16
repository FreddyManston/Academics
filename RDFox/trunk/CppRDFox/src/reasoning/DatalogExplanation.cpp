// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "../RDFStoreException.h"
#include "../dictionary/Dictionary.h"
#include "../equality/EqualityManager.h"
#include "../storage/DataStore.h"
#include "../storage/TupleTable.h"
#include "../storage/TupleIterator.h"
#include "../util/Mutex.h"
#include "../util/SequentialHashTableImpl.h"
#include "../util/ThreadContext.h"
#include "../util/Vocabulary.h"
#include "DatalogExplanation.h"
#include "RuleIndexImpl.h"

// DatalogExplanationAtomNode

template<class T>
always_inline bool isSupportingAtom(T& target, const TupleIndex tupleIndex, const TupleStatus tupleStatus) {
    return (tupleStatus & (TUPLE_STATUS_IDB | TUPLE_STATUS_IDB_MERGED)) == TUPLE_STATUS_IDB;
}

template<class MT>
void DatalogExplanationAtomNode<MT>::loadNode() {
    typename  MT::MutexHolderType mutexHolder(m_datalogExplanationProvider.m_dataStoreMutex);
    ThreadContext& threadContext = ThreadContext::getCurrentThreadContext();
    std::vector<ArgumentIndex> argumentIndexes;
    const size_t numberOfArguments = m_tupleBuffer.size();
    for (ArgumentIndex argumentIndex = 0; argumentIndex < numberOfArguments; ++argumentIndex)
        argumentIndexes.push_back(argumentIndex);
    auto filters = m_datalogExplanationProvider.m_ruleIndex.template setTupleIteratorFilters<SUPPORTING_FACTS_TUPLE_ITERATOR_FILTER>(*this,
        [](DatalogExplanationAtomNodeType& target, const TupleIndex tupleIndex, const TupleStatus tupleStatus) {
            return (tupleStatus & (TUPLE_STATUS_IDB | TUPLE_STATUS_IDB_MERGED)) == TUPLE_STATUS_IDB;
        },
        [](DatalogExplanationAtomNodeType& target, const TupleIndex tupleIndex, const TupleStatus tupleStatus) {
            return (tupleStatus & (TUPLE_STATUS_IDB | TUPLE_STATUS_IDB_MERGED)) == TUPLE_STATUS_IDB;
        }
    );
    for (size_t indexingPatternNumber = 0; indexingPatternNumber < 8; ++indexingPatternNumber) {
        for (HeadAtomInfo* headAtomInfo = m_datalogExplanationProvider.m_ruleIndex.getMatchingHeadAtomInfos(threadContext, m_tupleBuffer, argumentIndexes, indexingPatternNumber); headAtomInfo != nullptr; headAtomInfo = headAtomInfo->getNextMatchingHeadAtomInfo(m_tupleBuffer, argumentIndexes)) {
            SupportingFactsEvaluator& supportingFactsEvaluator = headAtomInfo->getSupportingFactsEvaluatorPrototype(m_tupleBuffer, argumentIndexes);
            const std::vector<std::pair<Variable, ArgumentIndex> >& variableIndexes = headAtomInfo->getRuleInfo().getSupportingFactsVariableIndexes();
            for (size_t multiplicity = supportingFactsEvaluator.open(threadContext); multiplicity != 0; multiplicity = supportingFactsEvaluator.advance()) {
                std::vector<ResourceID> substitutionValues;
                std::vector<const ExplanationAtomNode*> children;
                for (auto iterator = variableIndexes.begin(); iterator != variableIndexes.end(); ++iterator)
                    substitutionValues.push_back(supportingFactsEvaluator.getArgumentsBuffer()[iterator->second]);
                for (size_t bodyIndex = 0; bodyIndex < supportingFactsEvaluator.getNumberOfBodyLiterals(); ++bodyIndex) {
                    if (headAtomInfo->isSupportingBodyAtom<false>(bodyIndex)) {
                        const TupleIterator& bodyLiteralTupleIterator = supportingFactsEvaluator.getBodyLiteral(bodyIndex);
                        TupleIndex tupleIndex;
                        TupleStatus tupleStatus;
                        bodyLiteralTupleIterator.getCurrentTupleInfo(tupleIndex, tupleStatus);
                        ExplanationAtomNode& childNode = m_datalogExplanationProvider.getNodeInternal(bodyLiteralTupleIterator.getArgumentsBuffer(), bodyLiteralTupleIterator.getArgumentIndexes(), tupleStatus);
                        children.push_back(&childNode);
                    }
                }
                m_children.push_back(std::unique_ptr<ExplanationRuleInstanceNode>(new DatalogExplanationHeadAtomInfoInstanceNode<MT>(m_datalogExplanationProvider, *this, *headAtomInfo, std::move(substitutionValues), std::move(children))));
            }
        }
    }
    if (m_datalogExplanationProvider.m_dataStore.getEqualityAxiomatizationType() != EQUALITY_AXIOMATIZATION_OFF) {
        for (ArgumentIndex argumentIndex = 0; argumentIndex < numberOfArguments; ++argumentIndex) {
            const ResourceID resourceID = m_tupleBuffer[argumentIndex];
            if (m_datalogExplanationProvider.m_equalityManager.getNextEqual(resourceID) != INVALID_RESOURCE_ID) {
                std::vector<ResourceID> substitutionValues(m_tupleBuffer);
                std::vector<const ExplanationAtomNode*> children;
                std::vector<ResourceID> argumentsBuffer;
                argumentsBuffer.push_back(resourceID);
                argumentsBuffer.push_back(OWL_SAME_AS_ID);
                argumentsBuffer.push_back(resourceID);
                children.push_back(&m_datalogExplanationProvider.getNodeInternal(argumentsBuffer, argumentIndexes, TUPLE_STATUS_INVALID));
                children.push_back(this);
                m_children.push_back(std::unique_ptr<ExplanationRuleInstanceNode>(new DatalogExplanationAnyRuleInstanceNode<MT>(m_datalogExplanationProvider, *this, std::move(substitutionValues), std::move(children), m_datalogExplanationProvider.m_replacementRulesByArgumentIndex[argumentIndex], 0, m_datalogExplanationProvider.m_replacementVariablesByArgumentIndex)));
            }
        }
    }
    m_loaded = true;
}

template<class MT>
bool DatalogExplanationAtomNode<MT>::heightComputed() const {
    return m_type != IDB_ATOM || m_height != static_cast<size_t>(-1);
}

template<class MT>
void DatalogExplanationAtomNode<MT>::getLeaves(std::queue<DatalogExplanationAtomNodeType*>& unprocessedNodes) {
    std::queue<DatalogExplanationAtomNodeType*> queue;
    std::unordered_set<DatalogExplanationAtomNodeType*> visitedNodes;
    queue.push(this);
    visitedNodes.insert(this);
    while (!queue.empty()) {
        DatalogExplanationAtomNodeType* node = queue.front();
        queue.pop();
        if (node->heightComputed())
            unprocessedNodes.push(node);
        else {
            const unique_ptr_vector<ExplanationRuleInstanceNode>& childRuleInstances = node->getChildren();
            for (auto ruleInstanceIterator = childRuleInstances.begin(); ruleInstanceIterator != childRuleInstances.end(); ++ruleInstanceIterator) {
                ExplanationRuleInstanceNode* expalantionRuleInstanceNode = ruleInstanceIterator->get();
                const std::vector<const ExplanationAtomNode*>& childAtoms = expalantionRuleInstanceNode->getChildren();
                if (childAtoms.empty()) {
                    node->m_height = 1;
                    node->m_shortestChild = expalantionRuleInstanceNode;
                    unprocessedNodes.push(node);
                    break;
                }
                else {
                    for (auto nodeIterator = childAtoms.begin(); nodeIterator != childAtoms.end(); ++nodeIterator) {
                        DatalogExplanationAtomNodeType* childNode = const_cast<DatalogExplanationAtomNodeType*>(static_cast<const DatalogExplanationAtomNodeType*>(*nodeIterator));
                        if (visitedNodes.insert(childNode).second)
                            queue.push(childNode);
                    }
                }
            }
        }
    }
}

template<class MT>
void DatalogExplanationAtomNode<MT>::computeHeight() {
    std::queue<DatalogExplanationAtomNodeType*> unprocessedNodes;
    getLeaves(unprocessedNodes);
    while (!unprocessedNodes.empty()) {
        DatalogExplanationAtomNodeType* node = unprocessedNodes.front();
        unprocessedNodes.pop();
        assert(node->heightComputed());
        std::vector<ExplanationRuleInstanceNode*>& ruleInstanceParents = node->m_knownParents;
        for (auto ruleInstanceIterator = ruleInstanceParents.begin(); ruleInstanceIterator != ruleInstanceParents.end(); ++ruleInstanceIterator) {
            ExplanationRuleInstanceNode* parentRuleInstance = *ruleInstanceIterator;
            DatalogExplanationAtomNodeType* parentNode = static_cast<DatalogExplanationAtomNodeType*>(const_cast<ExplanationAtomNode*>(&parentRuleInstance->getParent()));
            if (!parentNode->heightComputed()) {
                size_t maxHeight = 0;
                const std::vector<const ExplanationAtomNode*>& children = parentRuleInstance->getChildren();
                for (auto nodeIterator = children.begin(); maxHeight != static_cast<size_t>(-1) && nodeIterator != children.end(); ++nodeIterator)
                    maxHeight = std::max(maxHeight, static_cast<const DatalogExplanationAtomNodeType*>(*nodeIterator)->m_height);
                if (maxHeight != static_cast<size_t>(-1)) {
                    parentNode->m_height = maxHeight + 1;
                    parentNode->m_shortestChild = parentRuleInstance;
                    unprocessedNodes.push(parentNode);
                }
            }
        }
    }
}

template<class MT>
DatalogExplanationAtomNode<MT>::DatalogExplanationAtomNode(DatalogExplanationProvider<MT>& datalogExplanationProvider, const std::vector<ResourceID>& tupleBuffer, const AtomNodeType type) :
    m_datalogExplanationProvider(datalogExplanationProvider),
    m_tupleBuffer(tupleBuffer),
    m_type(type),
    m_loaded(m_type != ExplanationAtomNode::AtomNodeType::IDB_ATOM),
    m_height(m_type == ExplanationAtomNode::AtomNodeType::IDB_ATOM || m_type == ExplanationAtomNode::AtomNodeType::FALSE_ATOM ? static_cast<size_t>(-1) : 0),
    m_knownParents(),
    m_children(),
    m_shortestChild(nullptr)
{
}

template<class MT>
always_inline size_t DatalogExplanationAtomNode<MT>::getWorkerIndex() {
    return 0;
}

template<class MT>
const std::vector<ResourceID>& DatalogExplanationAtomNode<MT>::getTupleBuffer() const {
    return m_tupleBuffer;
}

template<class MT>
ExplanationAtomNode::AtomNodeType DatalogExplanationAtomNode<MT>::getType() const {
    return m_type;
}

template<class MT>
Atom DatalogExplanationAtomNode<MT>::getAtom() const {
    std::vector<Term> arguments;
    const Dictionary& dictionary = m_datalogExplanationProvider.m_dataStore.getDictionary();
    LogicFactory logicFactory = m_datalogExplanationProvider.m_ruleIndex.getLogicFactory();
    ResourceText resourceText;
    for (auto iterator = m_tupleBuffer.begin(); iterator != m_tupleBuffer.end(); ++iterator) {
        if (!dictionary.getResource(*iterator, resourceText))
            throw RDF_STORE_EXCEPTION("Cannot resolve resource ID in the dictionary.");
        arguments.push_back(logicFactory->getResourceByName(resourceText));
    }
    return logicFactory->getAtom(logicFactory->getRDFPredicate(), arguments);
}

template<class MT>
const unique_ptr_vector<ExplanationRuleInstanceNode>& DatalogExplanationAtomNode<MT>::getChildren() const {
    if (!m_loaded)
        const_cast<DatalogExplanationAtomNode<MT>*>(this)->loadNode();
    return m_children;
}

template<class MT>
size_t DatalogExplanationAtomNode<MT>::getHeight() const {
    if (!heightComputed())
        const_cast<DatalogExplanationAtomNode<MT>*>(this)->computeHeight();
    return m_height;
}

template<class MT>
ExplanationRuleInstanceNode* DatalogExplanationAtomNode<MT>::getShortestChild() const {
    if (!heightComputed())
        const_cast<DatalogExplanationAtomNode<MT>*>(this)->computeHeight();
    return m_shortestChild;
}

// AbstractDatalogExplanationRuleInstanceNode

template<class MT>
AbstractDatalogExplanationRuleInstanceNode<MT>::AbstractDatalogExplanationRuleInstanceNode(DatalogExplanationProvider<MT>& datalogExplanationProvider, const DatalogExplanationAtomNode<MT>& parent, std::vector<ResourceID>&& substitutionValues, std::vector<const ExplanationAtomNode*>&& children) :
      m_datalogExplanationProvider(datalogExplanationProvider),
      m_parent(parent),
      m_substitutionValues(std::move(substitutionValues)),
      m_children(std::move(children))
{
    for (auto iterator = m_children.begin(); iterator != m_children.end(); ++iterator)
        const_cast<DatalogExplanationAtomNodeType*>(static_cast<const DatalogExplanationAtomNodeType*>(*iterator))->m_knownParents.push_back(this);
}

template<class MT>
const ExplanationAtomNode& AbstractDatalogExplanationRuleInstanceNode<MT>::getParent() const {
    return m_parent;
}

template<class MT>
size_t AbstractDatalogExplanationRuleInstanceNode<MT>::getSubstitutionSize() const {
    return m_substitutionValues.size();
}

template<class MT>
ResourceID AbstractDatalogExplanationRuleInstanceNode<MT>::getSubstitutionValue(const size_t index) const {
    return m_substitutionValues[index];
}

template<class MT>
Rule AbstractDatalogExplanationRuleInstanceNode<MT>::getRuleInstance() const {
    const Rule& rule = getRule();
    const Dictionary& dictionary = m_datalogExplanationProvider.m_dataStore.getDictionary();
    Substitution substitution;
    ResourceText resourceText;
    for (size_t index = 0; index < m_substitutionValues.size(); ++index) {
        const ResourceID resourceID = m_substitutionValues[index];
        if (!dictionary.getResource(resourceID, resourceText))
            throw RDF_STORE_EXCEPTION("Cannot resolve resource ID in the dictionary.");
        substitution[getSubstitutionVariable(index)] = rule->getFactory()->getResourceByName(resourceText);
    }
    const std::vector<Literal>& body = rule->getBody();
    Atom newHead = rule->getHead(getHeadAtomIndex())->apply(substitution);
    std::vector<Literal> newBody;
    for (auto iterator = body.begin(); iterator != body.end(); ++iterator)
        newBody.push_back((*iterator)->apply(substitution));
    return rule->getFactory()->getRule(newHead, newBody);
}

template<class MT>
const std::vector<const ExplanationAtomNode*>& AbstractDatalogExplanationRuleInstanceNode<MT>::getChildren() const {
    return m_children;
}

// DatalogExplanationHeadAtomInfoInstanceNode

template<class MT>
DatalogExplanationHeadAtomInfoInstanceNode<MT>::DatalogExplanationHeadAtomInfoInstanceNode(DatalogExplanationProvider<MT>& datalogExplanationProvider, const DatalogExplanationAtomNode<MT>& parent, const HeadAtomInfo& headAtomInfo, std::vector<ResourceID>&& substitutionValues, std::vector<const ExplanationAtomNode*>&& children) :
    AbstractDatalogExplanationRuleInstanceNode<MT>(datalogExplanationProvider, parent, std::move(substitutionValues), std::move(children)),
    m_headAtomInfo(headAtomInfo)
{
}

template<class MT>
const Rule& DatalogExplanationHeadAtomInfoInstanceNode<MT>::getRule() const {
    return m_headAtomInfo.getRuleInfo().getRule();
}

template<class MT>
size_t DatalogExplanationHeadAtomInfoInstanceNode<MT>::getHeadAtomIndex() const {
    return m_headAtomInfo.getHeadAtomIndex();
}

template<class MT>
const Variable& DatalogExplanationHeadAtomInfoInstanceNode<MT>::getSubstitutionVariable(const size_t index) const {
    return m_headAtomInfo.getRuleInfo().getSupportingFactsVariableIndexes()[index].first;
}

// DatalogExplanationAnyRuleInstanceNode

template<class MT>
DatalogExplanationAnyRuleInstanceNode<MT>::DatalogExplanationAnyRuleInstanceNode(DatalogExplanationProvider<MT>& datalogExplanationProvider, const DatalogExplanationAtomNode<MT>& parent, std::vector<ResourceID>&& substitutionValues, std::vector<const ExplanationAtomNode*>&& children, const Rule& rule, const size_t headAtomIndex, const std::vector<Variable>& substitutionVariables) :
    AbstractDatalogExplanationRuleInstanceNode<MT>(datalogExplanationProvider, parent, std::move(substitutionValues), std::move(children)),
    m_rule(rule),
    m_headAtomIndex(headAtomIndex),
    m_substitutionVariables(substitutionVariables)
{
}

template<class MT>
const Rule& DatalogExplanationAnyRuleInstanceNode<MT>::getRule() const {
    return m_rule;
}

template<class MT>
size_t DatalogExplanationAnyRuleInstanceNode<MT>::getHeadAtomIndex() const {
    return m_headAtomIndex;
}

template<class MT>
const Variable& DatalogExplanationAnyRuleInstanceNode<MT>::getSubstitutionVariable(const size_t index) const {
    return m_substitutionVariables[index];
}

// DatalogExplanationProvider::NodePolicy

template<class MT>
always_inline void DatalogExplanationProvider<MT>::NodePolicy::getBucketContents(const uint8_t* const bucket, typename DatalogExplanationProvider::NodePolicy::BucketContents& bucketContents) {
    bucketContents.m_node = *reinterpret_cast<DatalogExplanationAtomNodeType* const *>(bucket);
}

template<class MT>
always_inline BucketStatus DatalogExplanationProvider<MT>::NodePolicy::getBucketContentsStatus(const BucketContents& bucketContents, const size_t valuesHashCode, DatalogExplanationAtomNodeType* const node) {
    if (bucketContents.m_node == nullptr)
        return BUCKET_EMPTY;
    else if (bucketContents.m_node == node)
        return BUCKET_CONTAINS;
    else
        return BUCKET_NOT_CONTAINS;
}

template<class MT>
always_inline BucketStatus DatalogExplanationProvider<MT>::NodePolicy::getBucketContentsStatus(const BucketContents& bucketContents, const size_t valuesHashCode, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes) {
    if (bucketContents.m_node == nullptr)
        return BUCKET_EMPTY;
    else {
        const std::vector<ResourceID>& tupleBuffer = bucketContents.m_node->getTupleBuffer();
        if (tupleBuffer.size() != argumentIndexes.size())
            return BUCKET_NOT_CONTAINS;
        auto nodeArgumentsIterator = tupleBuffer.begin();
        auto argumentIndexesIterator = argumentIndexes.begin();
        while (nodeArgumentsIterator != tupleBuffer.end()) {
            if (*nodeArgumentsIterator != argumentsBuffer[*argumentIndexesIterator])
                return BUCKET_NOT_CONTAINS;
            ++nodeArgumentsIterator;
            ++argumentIndexesIterator;
        }
        return BUCKET_CONTAINS;
    }
}

template<class MT>
always_inline bool DatalogExplanationProvider<MT>::NodePolicy::setBucketContentsIfEmpty(uint8_t* const bucket, const BucketContents& bucketContents) {
    if (getDatalogExplanationAtomNode(bucket) == nullptr) {
        setDatalogExplanationAtomNode(bucket, bucketContents.m_node);
        return true;
    }
    else
        return false;
}

template<class MT>
always_inline bool DatalogExplanationProvider<MT>::NodePolicy::isBucketContentsEmpty(const BucketContents& bucketContents) {
    return bucketContents.m_node == nullptr;
}

template<class MT>
always_inline size_t DatalogExplanationProvider<MT>::NodePolicy::getBucketContentsHashCode(const BucketContents& bucketContents) {
    return hashCodeFor(bucketContents.m_node);
}

template<class MT>
always_inline size_t DatalogExplanationProvider<MT>::NodePolicy::hashCodeFor(DatalogExplanationAtomNodeType* node) {
    const std::vector<ResourceID>& tupleBuffer = node->getTupleBuffer();
    size_t hash = 0;
    for (auto iterator = tupleBuffer.begin(); iterator != tupleBuffer.end(); ++iterator) {
        hash += *iterator;
        hash += (hash << 10);
        hash ^= (hash >> 6);
    }
    hash += (hash << 3);
    hash ^= (hash >> 11);
    hash += (hash << 15);
    return hash;
}

template<class MT>
always_inline size_t DatalogExplanationProvider<MT>::NodePolicy::hashCodeFor(const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes) {
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

template<class MT>
always_inline void DatalogExplanationProvider<MT>::NodePolicy::makeBucketEmpty(uint8_t* const bucket) {
    setDatalogExplanationAtomNode(bucket, nullptr);
}

template<class MT>
always_inline const typename DatalogExplanationProvider<MT>::DatalogExplanationAtomNodeType* DatalogExplanationProvider<MT>::NodePolicy::getDatalogExplanationAtomNode(const uint8_t* const bucket) {
    return *reinterpret_cast<const DatalogExplanationAtomNodeType* const *>(bucket);
}

template<class MT>
always_inline void DatalogExplanationProvider<MT>::NodePolicy::setDatalogExplanationAtomNode(uint8_t* const bucket, DatalogExplanationAtomNodeType* const node) {
    *reinterpret_cast<DatalogExplanationAtomNodeType**>(bucket) = node;
}

// DatalogExplanationProvider

template<class MT>
ExplanationAtomNode::AtomNodeType DatalogExplanationProvider<MT>::getType(ThreadContext& threadContext, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes) {
    for (m_tupleBuffer[0] = argumentsBuffer[argumentIndexes[0]]; m_tupleBuffer[0] != INVALID_RESOURCE_ID; m_tupleBuffer[0] = m_equalityManager.getNextEqual(m_tupleBuffer[0]))
        for (m_tupleBuffer[1] = argumentsBuffer[argumentIndexes[1]]; m_tupleBuffer[1] != INVALID_RESOURCE_ID; m_tupleBuffer[1] = m_equalityManager.getNextEqual(m_tupleBuffer[1]))
            for (m_tupleBuffer[2] = argumentsBuffer[argumentIndexes[2]]; m_tupleBuffer[2] != INVALID_RESOURCE_ID; m_tupleBuffer[2] = m_equalityManager.getNextEqual(m_tupleBuffer[2]))
                if (m_tupleBuffer[0] != argumentsBuffer[argumentIndexes[0]] || m_tupleBuffer[1] != argumentsBuffer[argumentIndexes[1]] || m_tupleBuffer[2] != argumentsBuffer[argumentIndexes[2]]) {
                    const TupleIndex tupleIndex = m_tripleTable.getTupleIndex(threadContext, m_tupleBuffer, m_argumentIndexes);
                    if (tupleIndex != INVALID_TUPLE_INDEX && (m_tripleTable.getTupleStatus(tupleIndex) & TUPLE_STATUS_EDB) == TUPLE_STATUS_EDB)
                        return ExplanationAtomNode::EQUAL_TO_EDB_ATOM;
                }
    return ExplanationAtomNode::IDB_ATOM;
}

template<class MT>
ExplanationAtomNode& DatalogExplanationProvider<MT>::getNodeInternal(const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, TupleStatus tupleStatus) {
    typename SequentialHashTable<NodePolicy>::BucketDescriptor bucketDescriptor;
    ThreadContext& threadContext = ThreadContext::getCurrentThreadContext();
    m_nodes.acquireBucket(threadContext, bucketDescriptor, argumentsBuffer, argumentIndexes);
    if (m_nodes.continueBucketSearch(threadContext, bucketDescriptor, argumentsBuffer, argumentIndexes) == BUCKET_EMPTY) {
        std::vector<ResourceID> tupleBuffer;
        for (auto iterator = argumentIndexes.begin(); iterator != argumentIndexes.end(); ++iterator)
            tupleBuffer.push_back(argumentsBuffer[*iterator]);
        if (tupleStatus == TUPLE_STATUS_INVALID) {
            const TupleIndex tupleIndex = m_tripleTable.getTupleIndex(threadContext, tupleBuffer, argumentIndexes);
            if (tupleIndex != INVALID_TUPLE_INDEX)
                tupleStatus = m_tripleTable.getTupleStatus(tupleIndex);
        }
        ExplanationAtomNode::AtomNodeType type;
        if ((tupleStatus & (TUPLE_STATUS_IDB | TUPLE_STATUS_IDB_MERGED)) != TUPLE_STATUS_IDB)
            type = ExplanationAtomNode::FALSE_ATOM;
        else if ((tupleStatus & TUPLE_STATUS_EDB) == TUPLE_STATUS_EDB)
            type = ExplanationAtomNode::EDB_ATOM;
        else {
            if (m_dataStore.getEqualityAxiomatizationType() != EQUALITY_AXIOMATIZATION_OFF)
                type = getType(threadContext, argumentsBuffer, argumentIndexes);
            else
                type = ExplanationAtomNode::IDB_ATOM;
        }
        bucketDescriptor.m_bucketContents.m_node = new DatalogExplanationAtomNodeType(*this, tupleBuffer, type);
        m_nodes.getPolicy().setDatalogExplanationAtomNode(bucketDescriptor.m_bucket, bucketDescriptor.m_bucketContents.m_node);
        m_nodes.acknowledgeInsert(threadContext, bucketDescriptor);
    }
    m_nodes.releaseBucket(threadContext, bucketDescriptor);
    return *bucketDescriptor.m_bucketContents.m_node;
}

template<class MT>
DatalogExplanationProvider<MT>::DatalogExplanationProvider(const DataStore& dataStore, MutexType& dataStoreMutex, RuleIndex& ruleIndex) :
    m_dataStore(dataStore),
    m_tripleTable(m_dataStore.getTupleTable("internal$rdf")),
    m_equalityManager(m_dataStore.getEqualityManager()),
    m_dataStoreMutex(dataStoreMutex),
    m_ruleIndex(ruleIndex),
    m_nodes(m_dataStore.getMemoryManager(), HASH_TABLE_LOAD_FACTOR),
    m_tupleBuffer(3, INVALID_RESOURCE_ID),
    m_argumentIndexes(),
    m_replacementVariablesByArgumentIndex(),
    m_replacementRulesByArgumentIndex()
{
    m_argumentIndexes.push_back(0);
    m_argumentIndexes.push_back(1);
    m_argumentIndexes.push_back(2);
    if (!m_nodes.initialize(HASH_TABLE_INITIAL_SIZE))
        throw RDF_STORE_EXCEPTION("Memory exhaused while initializing DatalogExplanationProvider.");
    LogicFactory& logicFactory = m_ruleIndex.getLogicFactory();
    m_replacementVariablesByArgumentIndex.push_back(logicFactory->getVariable("X1"));
    m_replacementVariablesByArgumentIndex.push_back(logicFactory->getVariable("X2"));
    m_replacementVariablesByArgumentIndex.push_back(logicFactory->getVariable("X3"));
    Atom atom = logicFactory->getRDFAtom(m_replacementVariablesByArgumentIndex[0], m_replacementVariablesByArgumentIndex[1], m_replacementVariablesByArgumentIndex[2]);
    ResourceByName owlSameAs = logicFactory->getIRIReference(OWL_SAME_AS);
    for (ArgumentIndex argumentIndex = 0; argumentIndex < 3; ++argumentIndex) {
        std::vector<Literal> body;
        body.push_back(logicFactory->getRDFAtom(m_replacementVariablesByArgumentIndex[argumentIndex], owlSameAs, m_replacementVariablesByArgumentIndex[argumentIndex]));
        body.push_back(atom);
        m_replacementRulesByArgumentIndex.push_back(logicFactory->getRule(atom, body));
    }
}

template<class MT>
DatalogExplanationProvider<MT>::~DatalogExplanationProvider() {
    if (m_nodes.getFirstBucket() != nullptr) {
        for (const uint8_t* bucket = m_nodes.getFirstBucket(); bucket != m_nodes.getAfterLastBucket(); bucket += m_nodes.getPolicy().BUCKET_SIZE) {
            const DatalogExplanationAtomNodeType* node = m_nodes.getPolicy().getDatalogExplanationAtomNode(bucket);
            if (node != nullptr)
                delete node;
        }
    }
}

template<class MT>
const ExplanationAtomNode& DatalogExplanationProvider<MT>::getNode(const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes) {
    typename MutexType::MutexHolderType mutexHolder(m_dataStoreMutex);
    if (m_dataStore.getEqualityAxiomatizationType() != EQUALITY_AXIOMATIZATION_OFF && m_equalityManager.normalize(argumentsBuffer, argumentIndexes, m_tupleBuffer, m_argumentIndexes))
        return getNodeInternal(m_tupleBuffer, m_argumentIndexes, TUPLE_STATUS_INVALID);
    else
        return getNodeInternal(argumentsBuffer, argumentIndexes, TUPLE_STATUS_INVALID);
}

template<class MT>
const ExplanationAtomNode& DatalogExplanationProvider<MT>::getNode(const Atom& atom) {
    if (atom->getPredicate()->getName() != "internal$rdf" || atom->getNumberOfArguments() != 3)
        throw RDF_STORE_EXCEPTION("Explanations can be produced only for RDF atoms.");
    ThreadContext& threadContext = ThreadContext::getCurrentThreadContext();
    Dictionary& dictionary = m_dataStore.getDictionary();
    const size_t numberOfArguments = atom->getNumberOfArguments();
    std::vector<ResourceID> tupleBuffer;
    for (ArgumentIndex argumentIndex = 0; argumentIndex < numberOfArguments; ++argumentIndex) {
        const Term& term = atom->getArgument(argumentIndex);
        ResourceID resourceID;
        switch (term->getType()) {
        case VARIABLE:
            throw RDF_STORE_EXCEPTION("Only ground atoms can be explained.");
        case RESOURCE_BY_ID:
            resourceID = to_reference_cast<ResourceByID>(term).getResourceID();
            break;
        case RESOURCE_BY_NAME:
            resourceID = dictionary.tryResolveResource(threadContext, to_reference_cast<ResourceByName>(term).getResourceText());
            break;
        default:
            throw RDF_STORE_EXCEPTION("Unexpected type of argument for the atom.");
        }
        if (resourceID == INVALID_RESOURCE_ID) {
            std::ostringstream message;
            message << "Term '" << term->toString(Prefixes::s_emptyPrefixes) << "' cannot be resolved in the dictionary.";
            throw RDF_STORE_EXCEPTION(message.str());
        }
        tupleBuffer.push_back(resourceID);
    }
    return getNode(tupleBuffer, m_argumentIndexes);
}

template class DatalogExplanationProvider<Mutex>;
template class DatalogExplanationProvider<NullMutex>;
