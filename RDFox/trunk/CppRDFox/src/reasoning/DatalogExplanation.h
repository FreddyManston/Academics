// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef DATALOGEXPLANATION_H_
#define DATALOGEXPLANATION_H_

#include "../storage/Explanation.h"
#include "../util/SequentialHashTable.h"

class DataStore;
class RuleIndex;
class Mutex;
class HeadAtomInfo;
class TupleTable;
class EqualityManager;

template<class MT>
class DatalogExplanationProvider;
template<class MT>
class AbstractDatalogExplanationRuleInstanceNode;
template<class MT>
class DatalogExplanationRuleInstanceNode;
template<class MT>
class DatalogExplanationSameAsInstanceNode;

// DatalogExplanationAtomNode

template<class MT>
class DatalogExplanationAtomNode : public ExplanationAtomNode {

    friend class SupportingFactsEvaluator;

protected:

    typedef DatalogExplanationAtomNode<MT> DatalogExplanationAtomNodeType;
    typedef DatalogExplanationRuleInstanceNode<MT> DatalogExplanationRuleInstanceNodeType;
    typedef DatalogExplanationSameAsInstanceNode<MT> DatalogExplanationSameAsInstanceNodeType;

    friend class AbstractDatalogExplanationRuleInstanceNode<MT>;

    DatalogExplanationProvider<MT>& m_datalogExplanationProvider;
    const std::vector<ResourceID> m_tupleBuffer;
    const AtomNodeType m_type;
    bool m_loaded;
    size_t m_height;
    std::vector<ExplanationRuleInstanceNode*> m_knownParents;
    unique_ptr_vector<ExplanationRuleInstanceNode> m_children;
    ExplanationRuleInstanceNode* m_shortestChild;

    void loadNode();

    bool heightComputed() const;

    void getLeaves(std::queue<DatalogExplanationAtomNodeType*>& unprocessedNodes);

    void computeHeight();

public:

    DatalogExplanationAtomNode(DatalogExplanationProvider<MT>& datalogExplanationProvider, const std::vector<ResourceID>& tupleBuffer, const AtomNodeType type);

    static size_t getWorkerIndex();

    virtual const std::vector<ResourceID>& getTupleBuffer() const;

    virtual AtomNodeType getType() const;

    virtual Atom getAtom() const;

    virtual const unique_ptr_vector<ExplanationRuleInstanceNode>& getChildren() const;

    virtual size_t getHeight() const;

    virtual ExplanationRuleInstanceNode* getShortestChild() const;

};

// AbstractDatalogExplanationRuleInstanceNode

template<class MT>
class AbstractDatalogExplanationRuleInstanceNode : public ExplanationRuleInstanceNode {

protected:

    typedef DatalogExplanationAtomNode<MT> DatalogExplanationAtomNodeType;
    typedef DatalogExplanationRuleInstanceNode<MT> DatalogExplanationRuleInstanceNodeType;
    typedef DatalogExplanationSameAsInstanceNode<MT> DatalogExplanationSameAsInstanceNodeType;

    friend DatalogExplanationAtomNodeType;

    DatalogExplanationProvider<MT>& m_datalogExplanationProvider;
    const DatalogExplanationAtomNodeType& m_parent;
    const std::vector<ResourceID> m_substitutionValues;
    std::vector<const ExplanationAtomNode*> m_children;

public:

    AbstractDatalogExplanationRuleInstanceNode(DatalogExplanationProvider<MT>& datalogExplanationProvider, const DatalogExplanationAtomNodeType& parent, std::vector<ResourceID>&& substitutionValues, std::vector<const ExplanationAtomNode*>&& children);

    virtual const ExplanationAtomNode& getParent() const;

    virtual size_t getSubstitutionSize() const;

    virtual ResourceID getSubstitutionValue(const size_t index) const;

    virtual Rule getRuleInstance() const;

    virtual const std::vector<const ExplanationAtomNode*>& getChildren() const;
    
};

// DatalogExplanationHeadAtomInfoInstanceNode

template<class MT>
class DatalogExplanationHeadAtomInfoInstanceNode : public AbstractDatalogExplanationRuleInstanceNode<MT> {

protected:

    friend class DatalogExplanationAtomNode<MT>;

    const HeadAtomInfo& m_headAtomInfo;

public:

    DatalogExplanationHeadAtomInfoInstanceNode(DatalogExplanationProvider<MT>& datalogExplanationProvider, const DatalogExplanationAtomNode<MT>& parent, const HeadAtomInfo& headAtomInfo, std::vector<ResourceID>&& substitutionValues, std::vector<const ExplanationAtomNode*>&& children);

    virtual const Rule& getRule() const;

    virtual size_t getHeadAtomIndex() const;

    virtual const Variable& getSubstitutionVariable(const size_t index) const;

};

// DatalogExplanationAnyRuleInstanceNode

template<class MT>
class DatalogExplanationAnyRuleInstanceNode : public AbstractDatalogExplanationRuleInstanceNode<MT> {

protected:

    Rule m_rule;
    const size_t m_headAtomIndex;
    std::vector<Variable> m_substitutionVariables;

public:

    DatalogExplanationAnyRuleInstanceNode(DatalogExplanationProvider<MT>& datalogExplanationProvider, const DatalogExplanationAtomNode<MT>& parent, std::vector<ResourceID>&& substitutionValues, std::vector<const ExplanationAtomNode*>&& children, const Rule& rule, const size_t headAtomIndex, const std::vector<Variable>& substitutionVariables);

    virtual const Rule& getRule() const;

    virtual size_t getHeadAtomIndex() const;

    virtual const Variable& getSubstitutionVariable(const size_t index) const;

};

// DatalogExplanationProvider

template<class MT>
class DatalogExplanationProvider : public ExplanationProvider {

    friend class DatalogExplanationAtomNode<MT>;
    friend class AbstractDatalogExplanationRuleInstanceNode<MT>;
    friend class DatalogExplanationAnyRuleInstanceNode<MT>;

protected:

    typedef DatalogExplanationAtomNode<MT> DatalogExplanationAtomNodeType;

    class NodePolicy {

    public:

        static const size_t BUCKET_SIZE = sizeof(DatalogExplanationAtomNodeType*);

        struct BucketContents {
            DatalogExplanationAtomNodeType* m_node;
        };

        static void getBucketContents(const uint8_t* const bucket, BucketContents& bucketContents);

        static BucketStatus getBucketContentsStatus(const BucketContents& bucketContents, const size_t valuesHashCode, DatalogExplanationAtomNodeType* const node);

        static BucketStatus getBucketContentsStatus(const BucketContents& bucketContents, const size_t valuesHashCode, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes);

        static bool setBucketContentsIfEmpty(uint8_t* const bucket, const BucketContents& bucketContents);

        static bool isBucketContentsEmpty(const BucketContents& bucketContents);

        static size_t getBucketContentsHashCode(const BucketContents& bucketContents);

        static size_t hashCodeFor(DatalogExplanationAtomNodeType* const node);

        static size_t hashCodeFor(const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes);

        static void makeBucketEmpty(uint8_t* const bucket);

        static const DatalogExplanationAtomNodeType* getDatalogExplanationAtomNode(const uint8_t* const bucket);

        static void setDatalogExplanationAtomNode(uint8_t* const bucket, DatalogExplanationAtomNodeType* const node);
        
    };

    typedef MT MutexType;

    const DataStore& m_dataStore;
    const TupleTable& m_tripleTable;
    const EqualityManager& m_equalityManager;
    MutexType& m_dataStoreMutex;
    RuleIndex& m_ruleIndex;
    SequentialHashTable<NodePolicy> m_nodes;
    std::vector<ResourceID> m_tupleBuffer;
    std::vector<ArgumentIndex> m_argumentIndexes;
    std::vector<Variable> m_replacementVariablesByArgumentIndex;
    std::vector<Rule> m_replacementRulesByArgumentIndex;

    ExplanationAtomNode::AtomNodeType getType(ThreadContext& threadContext, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes);

    ExplanationAtomNode& getNodeInternal(const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, TupleStatus tupleStatus);

public:

    DatalogExplanationProvider(const DataStore& dataStore, MutexType& dataStoreMutex, RuleIndex& ruleIndex);

    ~DatalogExplanationProvider();

    virtual const ExplanationAtomNode& getNode(const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes);

    virtual const ExplanationAtomNode& getNode(const Atom& atom);

};

#endif /* DATALOGEXPLANATION_H_ */
