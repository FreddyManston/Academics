// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef RULEINDEX_H_
#define RULEINDEX_H_

#include "../Common.h"
#include "../logic/Logic.h"
#include "../dictionary/ResourceValueCache.h"
#include "../querying/TermArray.h"
#include "../equality/EqualityManager.h"
#include "../storage/ArgumentIndexSet.h"
#include "../storage/TupleFilter.h"
#include "../storage/TupleTable.h"
#include "../storage/TupleIterator.h"
#include "../util/MemoryManager.h"
#include "../util/SmartPointer.h"
#include "../util/SequentialHashTable.h"
#include "DependencyGraph.h"

class InputStream;
class OutputStream;
class DataStore;
template<typename E> class LockFreeQueue;
class MaterializationMonitor;
template<class ObjectType, class PatternType> class LiteralPatternIndex;
class HeadAtomInfoPatternMain;
class PivotPositiveBodyLiteralInfoPatternMain;
class PivotPositiveBodyLiteralInfoPatternIncremental;
class PivotUnderlyingNegationLiteralInfoPatternMain;
class BodyLiteralInfo;
class PivotPositiveBodyLiteralInfo;
class PivotNegationBodyLiteralInfo;
class NonPivotBodyLiteralInfo;
template<class PBT> class UnderlyingLiteralInfo;
template<class PBT> class PivotUnderlyingLiteralInfo;
template<class PBT> class NonPivotUnderlyingLiteralInfo;
template<class PBT> class UnderlyingLiteralInfoIndex;
class HeadAtomInfo;
class RuleInfo;
class RuleIndex;
template<class PatternType> class PivotPositiveBodyLiteralInfoByPatternIndex;
template<class PatternType> class HeadAtomInfoByPatternIndex;
template<class PatternType> class PivotUnderlyingLiteralInfoByPatternIndex;

typedef SmartPointer<BodyLiteralInfo> BodyLiteralInfoPtr;
typedef SmartPointer<PivotPositiveBodyLiteralInfo> PivotPositiveBodyLiteralInfoPtr;
typedef SmartPointer<PivotNegationBodyLiteralInfo> PivotNegationBodyLiteralInfoPtr;
typedef SmartPointer<NonPivotBodyLiteralInfo> NonPivotBodyLiteralInfoPtr;
typedef UnderlyingLiteralInfo<PivotNegationBodyLiteralInfo> UnderlyingNegationLiteralInfo;
typedef PivotUnderlyingLiteralInfo<PivotNegationBodyLiteralInfo> PivotUnderlyingNegationLiteralInfo;
typedef NonPivotUnderlyingLiteralInfo<PivotNegationBodyLiteralInfo> NonPivotUnderlyingNegationLiteralInfo;
typedef UnderlyingLiteralInfoIndex<PivotNegationBodyLiteralInfo> UnderlyingNegationLiteralInfoIndex;
typedef HeadAtomInfoByPatternIndex<HeadAtomInfoPatternMain> HeadAtomInfoByPatternIndexMain;
typedef PivotPositiveBodyLiteralInfoByPatternIndex<PivotPositiveBodyLiteralInfoPatternMain> PivotPositiveBodyLiteralInfoByPatternIndexMain;
typedef PivotPositiveBodyLiteralInfoByPatternIndex<PivotPositiveBodyLiteralInfoPatternIncremental> PivotPositiveBodyLiteralInfoByPatternIndexIncremental;
typedef PivotUnderlyingLiteralInfoByPatternIndex<PivotUnderlyingNegationLiteralInfoPatternMain> PivotUnderlyingNegationLiteralInfoByPatternIndexMain;

enum ComponentLevelFilter { ALL_IN_COMPONENT = 0, WITH_PIVOT_IN_COMPONENT = 1, ALL_COMPONENTS = 2 };

enum LiteralPosition { BEFORE_PIVOT_ATOM, PIVOT_ATOM, AFTER_PIVOT_ATOM };

// TupleIteratorFilters

enum TupleIteratorFilterType { MAIN_TUPLE_ITERATOR_FILTER, INCREMENTAL_TUPLE_ITERATOR_FILTER, SUPPORTING_FACTS_TUPLE_ITERATOR_FILTER };

template<TupleIteratorFilterType FilterType, typename TargetType, typename PositiveCheckType, typename NegativeCheckType>
class TupleIteratorFilters {

protected:

    template<typename CheckType>
    struct Filter : public TupleFilter {

        TargetType& m_target;
        CheckType m_check;

        Filter(TargetType& target, CheckType check) : m_target(target), m_check(check) {
        }

        virtual bool processTuple(const void* const tupleFilterContext, const TupleIndex tupleIndex, const TupleStatus tupleStatus) const {
            return m_check(m_target, tupleIndex, tupleStatus);
        }

    };

    RuleIndex* m_ruleIndex;
    Filter<PositiveCheckType> m_positiveTupleFilter;
    Filter<NegativeCheckType> m_negativeTupleFilter;

    void setFilters(const size_t workerIndex, TupleFilter* positiveTupleFilter, TupleFilter* negativeTupleFilter);

public:

    TupleIteratorFilters(RuleIndex* ruleIndex, TargetType& target, PositiveCheckType positiveCheck, NegativeCheckType negativeCheck);

    TupleIteratorFilters(TupleIteratorFilters<FilterType, TargetType, PositiveCheckType, NegativeCheckType>&& other);

    ~TupleIteratorFilters();

    TupleIteratorFilters(const TupleIteratorFilters<FilterType, TargetType, PositiveCheckType, NegativeCheckType>&) = delete;

    TupleIteratorFilters<FilterType, TargetType, PositiveCheckType, NegativeCheckType>& operator=(const TupleIteratorFilters<FilterType, TargetType, PositiveCheckType, NegativeCheckType>&) = delete;

    TupleIteratorFilters<FilterType, TargetType, PositiveCheckType, NegativeCheckType>& operator=(TupleIteratorFilters<FilterType, TargetType, PositiveCheckType, NegativeCheckType>&&) = delete;

};

// BodyLiteralInfoFilter

enum BodyLiteralInfoFilterType {
    MAIN_POSITIVE_BEFORE_PIVOT,
    MAIN_POSITIVE_AFTER_PIVOT,
    MAIN_NEGATIVE_SINGLE_ATOM_BEFORE_PIVOT,
    MAIN_NEGATIVE_MULTIPLE_ATOMS_BEFORE_PIVOT,
    MAIN_NEGATIVE_AT_PIVOT_OR_AFTER_PIVOT,
    INCREMENTAL_POSITIVE_BEFORE_PIVOT,
    INCREMENTAL_POSITIVE_AFTER_PIVOT,
    INCREMENTAL_NEGATIVE
};

template<BodyLiteralInfoFilterType FilterType, typename TargetType, typename CheckType>
class BodyLiteralInfoFilter : public TupleFilter {

protected:

    RuleIndex* m_ruleIndex;
    TargetType& m_target;
    CheckType m_check;

    void setFilter(TupleFilter* tupleFilter);

public:

    BodyLiteralInfoFilter(RuleIndex* ruleIndex, TargetType& target, CheckType check);

    BodyLiteralInfoFilter(BodyLiteralInfoFilter<FilterType, TargetType, CheckType>&& other);

    ~BodyLiteralInfoFilter();

    BodyLiteralInfoFilter(const BodyLiteralInfoFilter<FilterType, TargetType, CheckType>&) = delete;

    BodyLiteralInfoFilter<FilterType, TargetType, CheckType>& operator=(const BodyLiteralInfoFilter<FilterType, TargetType, CheckType>&) = delete;

    BodyLiteralInfoFilter<FilterType, TargetType, CheckType>& operator=(BodyLiteralInfoFilter<FilterType, TargetType, CheckType>&&) = delete;

    virtual bool processTuple(const void* const tupleFilterContext, const TupleIndex tupleIndex, const TupleStatus tupleStatus) const {
        return m_check(m_target, *reinterpret_cast<const BodyLiteralInfo*>(tupleFilterContext), tupleIndex, tupleStatus);
    }

};

// UnderlyingLiteralInfoFilter

enum UnderlyingLiteralInfoFilterType {
    UNDERLYING_BEFORE_PIVOT,
    UNDERLYING_AFTER_PIVOT
};

template<UnderlyingLiteralInfoFilterType FilterType, typename TargetType, typename CheckType>
class UnderlyingLiteralInfoFilter : public TupleFilter {

protected:

    RuleIndex* m_ruleIndex;
    TargetType& m_target;
    CheckType m_check;

    void setFilter(TupleFilter* tupleFilter);

public:

    UnderlyingLiteralInfoFilter(RuleIndex* ruleIndex, TargetType& target, CheckType check);

    UnderlyingLiteralInfoFilter(UnderlyingLiteralInfoFilter<FilterType, TargetType, CheckType>&& other);

    ~UnderlyingLiteralInfoFilter();

    UnderlyingLiteralInfoFilter(const UnderlyingLiteralInfoFilter<FilterType, TargetType, CheckType>&) = delete;

    UnderlyingLiteralInfoFilter<FilterType, TargetType, CheckType>& operator=(const UnderlyingLiteralInfoFilter<FilterType, TargetType, CheckType>&) = delete;

    UnderlyingLiteralInfoFilter<FilterType, TargetType, CheckType>& operator=(UnderlyingLiteralInfoFilter<FilterType, TargetType, CheckType>&&) = delete;

    virtual bool processTuple(const void* const tupleFilterContext, const TupleIndex tupleIndex, const TupleStatus tupleStatus) const;
    
};

// ApplicationManager

class ApplicationManager : private Unmovable {

protected:

    struct CompareVariables {
        size_t m_index1;
        size_t m_index2;

        CompareVariables(const size_t index1, const size_t index2) : m_index1(index1), m_index2(index2) {
        }
    };

    struct ArgumentToBuffer {
        size_t m_sourceIndex;
        ArgumentIndex m_targetIndex;

        ArgumentToBuffer(const size_t sourceIndex, const ArgumentIndex targetIndex) : m_sourceIndex(sourceIndex), m_targetIndex(targetIndex) {
        }
    };

    std::vector<CompareVariables> m_compareVariables;
    std::vector<ArgumentToBuffer> m_copyToBuffer;

public:

    ApplicationManager(const TermArray& termArray, const Literal& literal, const ArgumentIndexSet* variablesBoundBefore);

    bool satisfiesEqualities(const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes);

    bool prepareApply(const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, std::vector<ResourceID>& targetArgumentsBuffer);

};

// LiteralPattern

template<class ObjectType, class PatternType>
class LiteralPattern : private Unmovable {

protected:

    friend class LiteralPatternIndex<ObjectType, PatternType>;

    std::vector<ResourceID> m_originalIndexingPattern;
    std::vector<ResourceID> m_currentIndexingPattern;
    size_t m_indexingPatternNumber;
    ObjectType* m_previousIndexed;
    ObjectType* m_nextIndexed;
    bool m_inByPatternIndex;

public:

    LiteralPattern(const std::vector<ResourceID>& defaultArgumentsBuffer, const EqualityManager& equalityManager, const TermArray& termArray, const Literal& literal);

    ObjectType* getPreviousIndexed() const {
        return m_previousIndexed;
    }

    ObjectType* getNextIndexed() const {
        return m_nextIndexed;
    }

    void ensureNormalized(const EqualityManager& equalityManager);

};

// HeadAtomInfoPatternMain

class HeadAtomInfoPatternMain : public LiteralPattern<HeadAtomInfo, HeadAtomInfoPatternMain> {

public:

    HeadAtomInfoPatternMain(const std::vector<ResourceID>& defaultArgumentsBuffer, const EqualityManager& equalityManager, const TermArray& termArray, const Literal& literal);

    LiteralPatternIndex<HeadAtomInfo, HeadAtomInfoPatternMain>& getLiteralPatternIndex();

};

// PivotPositiveBodyLiteralInfoPatternMain

class PivotPositiveBodyLiteralInfoPatternMain : public LiteralPattern<PivotPositiveBodyLiteralInfo, PivotPositiveBodyLiteralInfoPatternMain> {

public:

    PivotPositiveBodyLiteralInfoPatternMain(const std::vector<ResourceID>& defaultArgumentsBuffer, const EqualityManager& equalityManager, const TermArray& termArray, const Literal& literal);

    LiteralPatternIndex<PivotPositiveBodyLiteralInfo, PivotPositiveBodyLiteralInfoPatternMain>& getLiteralPatternIndex();

};

// PivotPositiveBodyLiteralInfoPatternIncremental

class PivotPositiveBodyLiteralInfoPatternIncremental : public LiteralPattern<PivotPositiveBodyLiteralInfo, PivotPositiveBodyLiteralInfoPatternIncremental> {

public:

    PivotPositiveBodyLiteralInfoPatternIncremental(const std::vector<ResourceID>& defaultArgumentsBuffer, const EqualityManager& equalityManager, const TermArray& termArray, const Literal& literal);

    LiteralPatternIndex<PivotPositiveBodyLiteralInfo, PivotPositiveBodyLiteralInfoPatternIncremental>& getLiteralPatternIndex();

};

// PivotUnderlyingNegationLiteralInfoPatternMain

class PivotUnderlyingNegationLiteralInfoPatternMain : public LiteralPattern<PivotUnderlyingNegationLiteralInfo, PivotUnderlyingNegationLiteralInfoPatternMain> {

public:

    typedef PivotUnderlyingNegationLiteralInfo PivotUnderlyingLiteralInfoType;

    PivotUnderlyingNegationLiteralInfoPatternMain(const std::vector<ResourceID>& defaultArgumentsBuffer, const EqualityManager& equalityManager, const TermArray& termArray, const Literal& literal);

    LiteralPatternIndex<PivotUnderlyingNegationLiteralInfo, PivotUnderlyingNegationLiteralInfoPatternMain>& getLiteralPatternIndex();

};

// LiteralInfoBase

template<class InfoType, class NonPivotInfoType>
class LiteralInfoBase : private Unmovable {

    template<class T>
    friend class LiteralInfoIndexBase;

protected:

    RuleIndex& m_ruleIndex;
    mutable int32_t m_referenceCount;
    const Literal m_literal;
    const size_t m_hashCode;
    NonPivotInfoType* m_firstChild;
    const std::vector<ArgumentIndex> m_argumentIndexes;
    ArgumentIndexSet m_variablesBoundAfterLiteral;
    std::unique_ptr<uint8_t[]> m_componentLevelFilters[ALL_COMPONENTS];

    static always_inline size_t hashCodeFor(const InfoType* const parent, const Literal& literal, const LiteralPosition literalPosition) {
        return ((parent == 0 ? 0 : parent->m_hashCode) * 23 + literal->hash()) * 23 + static_cast<size_t>(literalPosition) * 7;
    }

    virtual void setThreadCapacity(const size_t numberOfThreads) = 0;

    virtual void ensureThreadReady(const size_t threadIndex) = 0;

    void resizeComponentLevelFilters();

    void clearComponentLevelFilters();

    void addToComponentLevelFilter(const ComponentLevelFilter componentLevelFilter, const size_t componentLevel);

    template<ComponentLevelFilter componentLevelFilter>
    always_inline bool isInComponentLevelFilter(const size_t componentLevel) const {
        if (componentLevelFilter == ALL_COMPONENTS)
            return true;
        else
            return (m_componentLevelFilters[componentLevelFilter][componentLevel >> 3] & (static_cast<uint8_t>(1) << (componentLevel & 0x7))) != 0;
    }

public:

    LiteralInfoBase(RuleIndex& ruleIndex, const ArgumentIndexSet& literalVariables, const std::vector<ArgumentIndex>& argumentIndexes, const Literal& literal, const size_t hashCode);

    always_inline const Literal& getLiteral() const {
        return m_literal;
    }

    always_inline size_t getHashCode() const {
        return m_hashCode;
    }

    virtual InfoType* getParent() const = 0;

    virtual LiteralPosition getLiteralPosition() const = 0;

    always_inline const std::vector<ArgumentIndex>& getArgumentIndexes() const {
        return m_argumentIndexes;
    }

    template<uint8_t ruleType>
    const std::vector<ResourceID>& getArgumentsBuffer(const size_t threadIndex) const;

};

// BodyLiteralInfo

class BodyLiteralInfo : public LiteralInfoBase<BodyLiteralInfo, NonPivotBodyLiteralInfo> {

    friend SmartPointer<BodyLiteralInfo>::ReferenceManagerType;
    friend class RuleInfo;
    friend class NonPivotBodyLiteralInfo;
    friend class DependencyGraph;
    friend class RuleIndex;

protected:

    size_t m_componentLevel;
    std::vector<HeadAtomInfo*> m_lastLiteralForHeadAtoms;

    void addHeadAtomInfo(HeadAtomInfo& headAtomInfo);

    void removeHeadAtomInfo(HeadAtomInfo& headAtomInfo);

    template<bool isMain, ComponentLevelFilter componentLevelFilter, bool callMonitor, class ConsumerType>
    void literalMatched(ThreadContext& threadContext, const size_t workerIndex, const size_t componentLevel, MaterializationMonitor* const materializationMonitor, ConsumerType consumer);

public:

    BodyLiteralInfo(RuleIndex& ruleIndex, const ArgumentIndexSet& literalVariables, const std::vector<ArgumentIndex>& argumentIndexes, const Literal& literal, const size_t hashCode);

    virtual ~BodyLiteralInfo();

    always_inline size_t getComponentLevel() const {
        return m_componentLevel;
    }

};

// PivotPositiveBodyLiteralInfo

class PivotPositiveBodyLiteralInfo : public ApplicationManager, public PivotPositiveBodyLiteralInfoPatternMain, public PivotPositiveBodyLiteralInfoPatternIncremental, public BodyLiteralInfo {

    friend SmartPointer<PivotPositiveBodyLiteralInfo>::ReferenceManagerType;
    friend class PivotPositiveBodyLiteralInfoPatternMain;
    friend class PivotPositiveBodyLiteralInfoPatternIncremental;

protected:

    virtual void setThreadCapacity(const size_t numberOfThreads);

    virtual void ensureThreadReady(const size_t threadIndex);

public:

    PivotPositiveBodyLiteralInfo(RuleIndex& ruleIndex, const ArgumentIndexSet& literalVariables, const std::vector<ArgumentIndex>& argumentIndexes, const Literal& literal, const LiteralPosition literalPosition, BodyLiteralInfo* const parent);

    virtual ~PivotPositiveBodyLiteralInfo();

    virtual BodyLiteralInfo* getParent() const;

    virtual LiteralPosition getLiteralPosition() const;

    template<bool isMain, ComponentLevelFilter componentLevelFilter, bool callMonitor, class ConsumerType>
    void applyTo(ThreadContext& threadContext, const size_t workerIndex, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const size_t componentLevel, MaterializationMonitor* const materializationMonitor, ConsumerType consumer);

};

// PivotNegationBodyLiteralInfo

class PivotNegationBodyLiteralInfo : public BodyLiteralInfo {

    friend SmartPointer<PivotNegationBodyLiteralInfo>::ReferenceManagerType;
    friend UnderlyingNegationLiteralInfo;
    friend class RuleInfo;
    friend class RuleIndex;

public:

    typedef PivotUnderlyingNegationLiteralInfoPatternMain PivotUnderlyingLiteralPatternMain;

protected:

    struct UnderlyingEvaluationPlan {
        typedef PivotUnderlyingNegationLiteralInfo PivotLiteralInfoType;

        PivotUnderlyingNegationLiteralInfo& m_pivotUnderlyingNegationLiteralInfo;
        SmartPointer<UnderlyingNegationLiteralInfo> m_lastUnderlyingNegationLiteralInfo;
        size_t m_cost;

        UnderlyingEvaluationPlan(PivotUnderlyingNegationLiteralInfo& pivotUnderlyingNegationLiteralInfo, SmartPointer<UnderlyingNegationLiteralInfo> lastUnderlyingNegationLiteralInfo, size_t cost);

    };

    const bool m_hasProjection;
    std::vector<UnderlyingEvaluationPlan> m_underlyingEvaluationPlans;
    unique_ptr_vector<TupleIterator> m_tupleIteratorsByThreadMain;

    static UnderlyingNegationLiteralInfoIndex& getUnderlyingLiteralInfoIndex(RuleIndex& ruleIndex);

    virtual void setThreadCapacity(const size_t numberOfThreads);

    virtual void ensureThreadReady(const size_t threadIndex);

    template<ComponentLevelFilter componentLevelFilter, bool callMonitor, class ConsumerType>
    void underlyingLiteralsMatched(ThreadContext& threadContext, const size_t workerIndex, const size_t componentLevel, MaterializationMonitor* const materializationMonitor, ConsumerType consumer);

public:

    PivotNegationBodyLiteralInfo(RuleIndex& ruleIndex, const ArgumentIndexSet& literalVariables, const std::vector<ArgumentIndex>& argumentIndexes, const Literal& literal, const LiteralPosition literalPosition, BodyLiteralInfo* const parent);

    virtual ~PivotNegationBodyLiteralInfo();

    virtual BodyLiteralInfo* getParent() const;

    virtual LiteralPosition getLiteralPosition() const;

    void normalizeConstantsMain(const EqualityManager& equalityManager);

};

// NonPivotBodyLiteralInfo

class NonPivotBodyLiteralInfo : public BodyLiteralInfo {

    friend SmartPointer<NonPivotBodyLiteralInfo>::ReferenceManagerType;
    friend class BodyLiteralInfo;

protected:

    LiteralPosition m_literalPosition;
    BodyLiteralInfoPtr m_parent;
    NonPivotBodyLiteralInfo* m_nextSibling;
    NonPivotBodyLiteralInfo* m_previousSibling;
    unique_ptr_vector<TupleIterator> m_tupleIteratorsByThreadMain;
    unique_ptr_vector<TupleIterator> m_tupleIteratorsByThreadIncremental;

    const TupleFilter* & getTupleFilterMain(const size_t threadIndex);

    const TupleFilter* & getTupleFilterIncremental(const size_t threadIndex);

    virtual void setThreadCapacity(const size_t numberOfThreads);

    virtual void ensureThreadReady(const size_t threadIndex);

    template<bool isMain, ComponentLevelFilter componentLevelFilter, bool callMonitor, class ConsumerType>
    void matchLiteral(ThreadContext& threadContext, const size_t workerIndex, const size_t componentLevel, MaterializationMonitor* const materializationMonitor, ConsumerType consumer);

public:

    NonPivotBodyLiteralInfo(RuleIndex& ruleIndex, const ArgumentIndexSet& literalVariables, const std::vector<ArgumentIndex>& argumentIndexes, const Literal& literal, const LiteralPosition literalPosition, BodyLiteralInfo* const parent);

    virtual ~NonPivotBodyLiteralInfo();

    virtual BodyLiteralInfo* getParent() const;

    virtual LiteralPosition getLiteralPosition() const;

};

// UnderlyingLiteralInfo

template<class PBT>
class UnderlyingLiteralInfo : public LiteralInfoBase<UnderlyingLiteralInfo<PBT>, NonPivotUnderlyingLiteralInfo<PBT> > {

public:

    typedef PBT ParentBodyType;
    typedef UnderlyingLiteralInfo<ParentBodyType> UnderlyingLiteralInfoType;
    typedef PivotUnderlyingLiteralInfo<ParentBodyType> PivotUnderlyingLiteralInfoType;
    typedef NonPivotUnderlyingLiteralInfo<ParentBodyType> NonPivotUnderlyingLiteralInfoType;

    friend typename SmartPointer<UnderlyingLiteralInfoType>::ReferenceManagerType;
    friend ParentBodyType;
    friend NonPivotUnderlyingLiteralInfoType;
    friend class RuleInfo;
    friend class DependencyGraph;
    friend class RuleIndex;

protected:

    std::vector<ParentBodyType*> m_lastLiteralForParentBodyLiterals;

    void addParentBodyLiteralInfo(ParentBodyType& parentBodyLiteralInfo);

    void removeParentBodyLiteralInfo(ParentBodyType& parentBodyLiteralInfo);

    template<ComponentLevelFilter componentLevelFilter, bool callMonitor, class ConsumerType>
    void literalMatched(ThreadContext& threadContext, const size_t workerIndex, const size_t componentLevel, MaterializationMonitor* const materializationMonitor, ConsumerType consumer);

public:

    UnderlyingLiteralInfo(RuleIndex& ruleIndex, const ArgumentIndexSet& literalVariables, const std::vector<ArgumentIndex>& argumentIndexes, const Literal& literal, const size_t hashCode);

    virtual ~UnderlyingLiteralInfo();

};

// PivotUnderlyingLiteralInfo

template<class PBT>
class PivotUnderlyingLiteralInfo : public ApplicationManager, public PBT::PivotUnderlyingLiteralPatternMain, public UnderlyingLiteralInfo<PBT> {

public:

    typedef PBT ParentBodyType;
    typedef UnderlyingLiteralInfo<ParentBodyType> UnderlyingLiteralInfoType;
    typedef PivotUnderlyingLiteralInfo<ParentBodyType> PivotUnderlyingLiteralInfoType;
    typedef NonPivotUnderlyingLiteralInfo<ParentBodyType> NonPivotUnderlyingLiteralInfoType;

    friend typename SmartPointer<PivotUnderlyingLiteralInfoType>::ReferenceManagerType;
    friend typename ParentBodyType::PivotUnderlyingLiteralPatternMain;

protected:

    virtual void setThreadCapacity(const size_t numberOfThreads);

    virtual void ensureThreadReady(const size_t threadIndex);

public:

    PivotUnderlyingLiteralInfo(RuleIndex& ruleIndex, const ArgumentIndexSet& literalVariables, const std::vector<ArgumentIndex>& argumentIndexes, const Literal& literal, const LiteralPosition literalPosition, UnderlyingLiteralInfoType* const parent);

    virtual ~PivotUnderlyingLiteralInfo();

    virtual UnderlyingLiteralInfoType* getParent() const;

    virtual LiteralPosition getLiteralPosition() const;

    template<ComponentLevelFilter componentLevelFilter, bool callMonitor, class ConsumerType>
    void applyTo(ThreadContext& threadContext, const size_t workerIndex, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const size_t componentLevel, MaterializationMonitor* const materializationMonitor, ConsumerType consumer);

};

// NonPivotUnderlyingLiteralInfo

template<class PBT>
class NonPivotUnderlyingLiteralInfo : public UnderlyingLiteralInfo<PBT> {

public:

    typedef PBT ParentBodyType;
    typedef UnderlyingLiteralInfo<ParentBodyType> UnderlyingLiteralInfoType;
    typedef PivotUnderlyingLiteralInfo<ParentBodyType> PivotUnderlyingLiteralInfoType;
    typedef NonPivotUnderlyingLiteralInfo<ParentBodyType> NonPivotUnderlyingLiteralInfoType;

    friend typename SmartPointer<NonPivotUnderlyingLiteralInfoType>::ReferenceManagerType;
    friend UnderlyingLiteralInfoType;

protected:

    LiteralPosition m_literalPosition;
    SmartPointer<UnderlyingLiteralInfoType> m_parent;
    NonPivotUnderlyingLiteralInfoType* m_nextSibling;
    NonPivotUnderlyingLiteralInfoType* m_previousSibling;
    unique_ptr_vector<TupleIterator> m_tupleIteratorsByThread;

    const TupleFilter* & getTupleFilter(const size_t threadIndex);

    virtual void setThreadCapacity(const size_t numberOfThreads);

    virtual void ensureThreadReady(const size_t threadIndex);

    template<ComponentLevelFilter componentLevelFilter, bool callMonitor, class ConsumerType>
    void matchLiteral(ThreadContext& threadContext, const size_t workerIndex, const size_t componentLevel, MaterializationMonitor* const materializationMonitor, ConsumerType consumer);

public:

    NonPivotUnderlyingLiteralInfo(RuleIndex& ruleIndex, const ArgumentIndexSet& literalVariables, const std::vector<ArgumentIndex>& argumentIndexes, const Literal& literal, const LiteralPosition literalPosition, UnderlyingLiteralInfoType* const parent);

    virtual ~NonPivotUnderlyingLiteralInfo();

    virtual UnderlyingLiteralInfoType* getParent() const;

    virtual LiteralPosition getLiteralPosition() const;

};

// LiteralInfoIndexBase

template<class InfoType>
class LiteralInfoIndexBase : private Unmovable {

protected:

    struct LiteralInfoIndexPolicy {

        static const size_t BUCKET_SIZE = sizeof(InfoType*);

        struct BucketContents {
            InfoType* m_literalInfo;
        };

        static void getBucketContents(const uint8_t* const bucket, BucketContents& bucketContents);

        static BucketStatus getBucketContentsStatus(const BucketContents& bucketContents, const size_t valuesHashCode, const InfoType* const parent, const Literal& literal, const LiteralPosition literalPosition);

        static BucketStatus getBucketContentsStatus(const BucketContents& bucketContents, const size_t valuesHashCode, const InfoType* const literalInfo);

        static bool setBucketContentsIfEmpty(uint8_t* const bucket, const BucketContents& bucketContents);

        static bool isBucketContentsEmpty(const BucketContents& bucketContents);

        static size_t getBucketContentsHashCode(const BucketContents& bucketContents);

        static size_t hashCodeFor(const InfoType* parent, const Literal& literal, const LiteralPosition literalPosition);

        static size_t hashCodeFor(const InfoType* literalInfo);

        static void makeBucketEmpty(uint8_t* const bucket);

        static const InfoType* getLiteralInfo(const uint8_t* const bucket);

        static void setLiteralInfo(uint8_t* const bucket, InfoType* const literalInfo);

    };

    friend InfoType;

    SequentialHashTable<LiteralInfoIndexPolicy> m_index;

    void removeLiteralInfo(const InfoType* literalInfo);

public:

    typedef InfoType LiteralInfoType;

    LiteralInfoIndexBase(MemoryManager& memoryManager);

    void initialize();

    void setThreadCapacity(const size_t numberOfThreads);

    void ensureThreadReady(const size_t threadIndex);

    InfoType* firstLiteralLocation(uint8_t* & location);

    InfoType* nextLiteralLocation(uint8_t* & location);

};

// BodyLiteralInfoIndex

class BodyLiteralInfoIndex : public LiteralInfoIndexBase<BodyLiteralInfo> {

protected:

    template<class AT>
    SmartPointer<AT> getBodyLiteralInfo(RuleIndex& ruleIndex, const ArgumentIndexSet& literalVariables, const std::vector<ArgumentIndex>& argumentIndexes, const Literal& literal, BodyLiteralInfo* const parent, const LiteralPosition literalPosition);

public:

    BodyLiteralInfoIndex(MemoryManager& memoryManager);

    PivotPositiveBodyLiteralInfoPtr getPivotPositiveBodyLiteralInfo(RuleIndex& ruleIndex, const ArgumentIndexSet& literalVariables, const std::vector<ArgumentIndex>& argumentIndexes, const Literal& literal);

    PivotNegationBodyLiteralInfoPtr getPivotNegationBodyLiteralInfo(RuleIndex& ruleIndex, const ArgumentIndexSet& literalVariables, const std::vector<ArgumentIndex>& argumentIndexes, const Literal& literal);

    NonPivotBodyLiteralInfoPtr getNonPivotLiteralInfo(RuleIndex& ruleIndex, const ArgumentIndexSet& literalVariables, const std::vector<ArgumentIndex>& argumentIndexes, const Literal& literal, BodyLiteralInfo* const parent, const LiteralPosition literalPosition);

};

// UnderlyingLiteralInfoIndex

template<class PBT>
class UnderlyingLiteralInfoIndex : public LiteralInfoIndexBase<UnderlyingLiteralInfo<PBT> > {

public:

    typedef PBT ParentBodyType;
    typedef UnderlyingLiteralInfo<ParentBodyType> UnderlyingLiteralInfoType;
    typedef PivotUnderlyingLiteralInfo<ParentBodyType> PivotUnderlyingLiteralInfoType;
    typedef NonPivotUnderlyingLiteralInfo<ParentBodyType> NonPivotUnderlyingLiteralInfoType;

protected:

    template<class AT>
    SmartPointer<AT> getUnderlyingLiteralInfo(RuleIndex& ruleIndex, const ArgumentIndexSet& literalVariables, const std::vector<ArgumentIndex>& argumentIndexes, const Literal& literal, UnderlyingLiteralInfoType* const parent, const LiteralPosition literalPosition);

public:

    UnderlyingLiteralInfoIndex(MemoryManager& memoryManager);

    SmartPointer<PivotUnderlyingLiteralInfoType> getPivotUnderlyingLiteralInfo(RuleIndex& ruleIndex, const ArgumentIndexSet& literalVariables, const std::vector<ArgumentIndex>& argumentIndexes, const Literal& literal);

    SmartPointer<NonPivotUnderlyingLiteralInfoType> getNonPivotLiteralInfo(RuleIndex& ruleIndex, const ArgumentIndexSet& literalVariables, const std::vector<ArgumentIndex>& argumentIndexes, const Literal& literal, UnderlyingLiteralInfoType* const parent, const LiteralPosition literalPosition);

};

// SupportingFactsEvaluator

class SupportingFactsEvaluator : private Unmovable {

    friend class HeadAtomInfo;

protected:

    ResourceValueCache& m_resourceValueCache;
    std::vector<ResourceID> m_argumentsBuffer;
    std::unique_ptr<TupleIterator> m_tupleIterator;

public:

    SupportingFactsEvaluator(ResourceValueCache& resourceValueCache, const ArgumentIndexSet& headVariables, DataStore& dataStore, const std::vector<ResourceID>& defaultArgumentsBuffer, const Rule& rule, const std::vector<ArgumentIndexSet>& literalVariables, const std::vector<std::vector<ArgumentIndex> >& argumentIndexes, const TermArray& termArray, const TupleFilter* & positiveTupleFilter, const TupleFilter* & negativeTupleFilter, size_t* compiledConjunctIndexes);

    SupportingFactsEvaluator(const SupportingFactsEvaluator& other);

    ~SupportingFactsEvaluator();

    always_inline const std::vector<ResourceID>& getArgumentsBuffer() const {
        return m_argumentsBuffer;
    }

    always_inline size_t getNumberOfBodyLiterals() const {
        return m_tupleIterator->getNumberOfChildIterators();
    }

    always_inline const TupleIterator& getBodyLiteral(const size_t literalIndex) const {
        return m_tupleIterator->getChildIterator(literalIndex);
    }

    size_t open(ThreadContext& threadContext);

    size_t advance();

};

// HeadAtomInfo

class HeadAtomInfo : public ApplicationManager, public HeadAtomInfoPatternMain {

    friend class DependencyGraph;
    friend class HeadAtomInfoPatternMain;
    friend class BodyLiteralInfo;
    friend class RuleInfo;

protected:

    RuleInfo& m_ruleInfo;
    const size_t m_headAtomIndex;
    size_t m_componentLevel;
    bool m_recursive;
    std::vector<ArgumentIndex> m_headArgumentIndexes;
    std::vector<ArgumentIndex> m_supportingFactsHeadArgumentIndexes;
    std::unique_ptr<BodyLiteralInfo*[]> m_supportingBodyLiteralInfosOrdered;
    std::vector<ArgumentToBuffer> m_supportingFactsCopyToBuffer;
    std::unique_ptr<SupportingFactsEvaluator> m_supportingFactsEvaluatorPrototype;
    unique_ptr_vector<SupportingFactsEvaluator> m_supportingFactsEvaluators;

    template<bool isMain, bool callMonitor, class ConsumerType>
    void headAtomMatched(ThreadContext& threadContext, const size_t workerIndex, const BodyLiteralInfo* const lastBodyLiteralInfo, MaterializationMonitor* const materializationMonitor, ConsumerType consumer);

public:

    HeadAtomInfo(RuleInfo& ruleInfo, const size_t headAtomIndex, TermArray& termArraySupporting, const std::vector<ArgumentIndexSet>& literalVariablesSupporting, const std::vector<std::vector<ArgumentIndex> >& argumentIndexesSupporting);

    always_inline RuleInfo& getRuleInfo() {
        return m_ruleInfo;
    }

    always_inline const RuleInfo& getRuleInfo() const {
        return m_ruleInfo;
    }

    const Atom& getAtom() const;

    always_inline size_t getHeadAtomIndex() const {
        return m_headAtomIndex;
    }

    always_inline size_t getComponentLevel() const {
        return m_componentLevel;
    }

    template<bool checkComponentLevel>
    always_inline bool isRecursive() const;

    always_inline const std::vector<ArgumentIndex>& getHeadArgumentIndexes() const {
        return m_headArgumentIndexes;
    }

    HeadAtomInfo* getNextMatchingHeadAtomInfo(const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes);

    void normalizeConstantsMain(const EqualityManager& equalityManager);

    always_inline const std::vector<ArgumentIndex>& getSupportingFactsHeadArgumentIndexes() const {
        return m_supportingFactsHeadArgumentIndexes;
    }

    template<bool checkComponentLevel>
    bool isSupportingBodyAtom(const size_t bodyLiteralIndex) const;

    SupportingFactsEvaluator& getSupportingFactsEvaluatorPrototype(const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes);

    std::unique_ptr<SupportingFactsEvaluator> getSupportingFactsEvaluator(const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes);

    void leaveSupportFactsEvaluator(std::unique_ptr<SupportingFactsEvaluator> supportingFactsEvaluator);

};

// RuleInfo

class RuleInfo : public Unmovable {

    friend class RuleIndex;
    friend class HeadAtomInfo;
    friend class HeadAtomInfoPatternMain;
    friend class DependencyGraph;
    friend class RuleConstantIndex;

protected:

    struct RuleConstant : private Unmovable {
        RuleInfo& m_forRuleInfo;
        ResourceID m_defaultID;
        ResourceID m_currentMainID;
        ResourceID m_currentIncrementalID;
        RuleConstant* m_previous;
        RuleConstant* m_next;

        RuleConstant(RuleInfo& forRule, const ResourceID resourceID, const EqualityManager& equalityManager, const EqualityManager& provingEqualityManager);
        
    };

    struct PivotPositiveEvaluationPlan {
        typedef PivotPositiveBodyLiteralInfo PivotLiteralInfoType;

        PivotPositiveBodyLiteralInfo& m_pivotPositiveBodyLiteralInfo;
        BodyLiteralInfoPtr m_lastBodyLiteralInfo;
        size_t m_cost;

        PivotPositiveEvaluationPlan(PivotPositiveBodyLiteralInfo& pivotPositiveBodyLiteralInfo, BodyLiteralInfoPtr lastBodyLiteralInfo, size_t cost);

    };

    struct PivotNegationEvaluationPlan {
        typedef PivotNegationBodyLiteralInfo PivotLiteralInfoType;

        PivotNegationBodyLiteralInfo& m_pivotNegationBodyLiteralInfo;
        BodyLiteralInfoPtr m_lastBodyLiteralInfo;
        size_t m_cost;

        PivotNegationEvaluationPlan(PivotNegationBodyLiteralInfo& pivotNegationBodyLiteralInfo, BodyLiteralInfoPtr lastBodyLiteralInfo, size_t cost);

    };

protected:

    RuleIndex& m_ruleIndex;
    const Rule m_rule;
    const bool m_internalRule;
    bool m_pivotless;
    bool m_hasNegation;
    bool m_hasAggregation;
    bool m_active;
    bool m_justAdded;
    bool m_justDeleted;
    std::unique_ptr<uint8_t[]> m_componentLevelFilter;
    RuleInfo* m_nextRuleInfo;
    RuleInfo* m_previousRuleInfo;
    unique_ptr_vector<RuleConstant> m_ruleConstants;
    std::vector<PivotPositiveEvaluationPlan> m_pivotPositiveEvaluationPlans;
    std::vector<PivotNegationEvaluationPlan> m_pivotNegationEvaluationPlans;
    std::unique_ptr<BodyLiteralInfo*[]> m_bodyAtomInfosInOrder;
    unique_ptr_vector<HeadAtomInfo> m_headAtomInfos;
    unique_ptr_vector<TupleIterator> m_ruleEvaluatorsByThreadMain;
    unique_ptr_vector<TupleIterator> m_ruleEvaluatorsByThreadIncremental;
    std::vector<std::pair<Variable, ArgumentIndex> > m_supportingFactsVariableIndexes;
    std::vector<ArgumentIndex> m_supportingFactsConstantsIndexes;
    std::vector<ResourceID> m_supportingFactsDefaultArgumentsBuffer;

    void ensureActive();

    void ensureInactive();

    void resizeComponentLevelFilter();

    void updateComponentLevel();

    void normalizeConstantsMain(const EqualityManager& equalityManager);

    void normalizeConstantsIncremental(const EqualityManager& equalityManager);

    always_inline HeadAtomInfo& getHeadAtomInfo(const size_t index) {
        return *m_headAtomInfos[index];
    }

    template<bool isMain, bool checkComponentLevel, bool callMonitor, class ConsumerType>
    void evaluateRule(ThreadContext& threadContext, const size_t workerIndex, const size_t componentLevel, MaterializationMonitor* const materializationMonitor, ConsumerType consumer) const;

    void indexAtomicFormulaConstants(const AtomicFormula& atomicFormula, std::unordered_set<ResourceID>& ruleConstants);

public:

    RuleInfo(RuleIndex& ruleIndex, const Rule& rule, const bool internalRule, const bool active, const bool justAdded, const bool justDeleted);

    ~RuleInfo();

    always_inline const Rule& getRule() const {
        return m_rule;
    }

    always_inline bool isInternalRule() const {
        return m_internalRule;
    }

    always_inline bool isPivotless() const {
        return m_pivotless;
    }

    always_inline bool hasNegation() const {
        return m_hasNegation;
    }

    always_inline bool hasAggregation() const {
        return m_hasAggregation;
    }
    
    always_inline bool isActive() const {
        return m_active;
    }

    always_inline bool isJustAdded() const {
        return m_justAdded;
    }

    always_inline bool isJustDeleted() const {
        return m_justDeleted;
    }
    
    always_inline size_t getNumberOfHeadAtoms() const {
        return m_headAtomInfos.size();
    }

    always_inline const HeadAtomInfo& getHeadAtomInfo(const size_t headAtomIndex) const {
        return *m_headAtomInfos[headAtomIndex];
    }

    always_inline bool isInComponentLevelFilter(const size_t componentLevel) const {
        return (m_componentLevelFilter[componentLevel >> 3] & (static_cast<uint8_t>(1) << (componentLevel & 0x7))) != 0;
    }

    always_inline size_t getBodyLiteralComponentIndex(const size_t bodyLiteralIndex) const {
        const BodyLiteralInfo* const bodyLiteralInfo = m_bodyAtomInfosInOrder[bodyLiteralIndex];
        return bodyLiteralInfo == nullptr ? static_cast<size_t>(-1) : bodyLiteralInfo->getComponentLevel();
    }

    const std::vector<ResourceID>& getDefaultArgumentsBuffer() const;

    void setThreadCapacity(const size_t numberOfThreads);

    void ensureThreadReady(const size_t threadIndex);

    template<bool checkComponentLevel, bool callMonitor, class ConsumerType>
    void evaluateRuleMain(ThreadContext& threadContext, const size_t workerIndex, const size_t componentLevel, MaterializationMonitor* const materializationMonitor, ConsumerType consumer) const;

    template<bool checkComponentLevel, bool callMonitor, class ConsumerType>
    void evaluateRuleIncremental(ThreadContext& threadContext, const size_t workerIndex, const size_t componentLevel, MaterializationMonitor* const materializationMonitor, ConsumerType consumer) const;

    always_inline const std::vector<std::pair<Variable, ArgumentIndex> >& getSupportingFactsVariableIndexes() const {
        return m_supportingFactsVariableIndexes;
    }

    always_inline size_t getNumberOfPivotPositiveEvaluationPlans() const {
        return m_pivotPositiveEvaluationPlans.size();
    }

    void loadPivotPositiveEvaluationPlan(const size_t positivePlanIndex, std::vector<BodyLiteralInfo*>& rulePlan) const;

    always_inline size_t getNumberOfPivotNegationEvaluationPlans() const {
        return m_pivotNegationEvaluationPlans.size();
    }

    void loadPivotNegationEvaluationPlan(const size_t negationPlanIndex, std::vector<BodyLiteralInfo*>& rulePlan) const;

    always_inline size_t getNumberOfPivotNegationUnderlyingEvaluationPlans(const size_t negationPlanIndex) const {
        return m_pivotNegationEvaluationPlans[negationPlanIndex].m_pivotNegationBodyLiteralInfo.m_underlyingEvaluationPlans.size();
    }

    void loadPivotNegationUnderlyingEvaluationPlan(const size_t negationPlanIndex, const size_t underlyingPlanIndex, std::vector<UnderlyingNegationLiteralInfo*>& underlyingNegationPlan) const;

};

// LiteralPatternIndex

template<class ObjectType, class PatternType>
class LiteralPatternIndex : private Unmovable {

protected:

    struct LiteralPatternIndexPolicy {

        static const size_t BUCKET_SIZE = sizeof(ObjectType*);

        struct BucketContents {
            ObjectType* m_object;
        };

        static void getBucketContents(const uint8_t* const bucket, BucketContents& bucketContents);

        static BucketStatus getBucketContentsStatus(const BucketContents& bucketContents, const size_t valuesHashCode, const ResourceID subjectID, const ResourceID predicateID, const ResourceID objectID);

        static BucketStatus getBucketContentsStatus(const BucketContents& bucketContents, const size_t valuesHashCode, const ObjectType* const object);

        static bool setBucketContentsIfEmpty(uint8_t* const bucket, const BucketContents& bucketContents);

        static bool isBucketContentsEmpty(const BucketContents& bucketContents);

        static size_t getBucketContentsHashCode(const BucketContents& bucketContents);

        static size_t hashCodeFor(const ResourceID subjectID, const ResourceID predicateID, const ResourceID objectID);

        static size_t hashCodeFor(const ObjectType* const object);

        static void makeBucketEmpty(uint8_t* const bucket);

        static const ObjectType* getObject(const uint8_t* const bucket);

        static void setObject(uint8_t* const bucket, ObjectType* const object);

    };

    friend ObjectType;

    SequentialHashTable<LiteralPatternIndexPolicy> m_index;
    size_t m_patternCounts[8];

public:

    LiteralPatternIndex(MemoryManager& memoryManager);

    void initialize();

    void addObject(ObjectType* object);

    void removeObject(ObjectType* object);

    ObjectType* getFirstObject(ThreadContext& threadContext, const size_t indexingPatternNumber, const ResourceID subjectID, const ResourceID predicateID, const ResourceID objectID);

};

// HeadAtomInfoByPatternIndex

template<class PatternType>
class HeadAtomInfoByPatternIndex : public LiteralPatternIndex<HeadAtomInfo, PatternType> {

public:

    HeadAtomInfoByPatternIndex(MemoryManager& memoryManager);

    HeadAtomInfo* getMatchingHeadAtomInfos(ThreadContext& threadContext, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const size_t indexingPatternNumber);
    
};

// PivotPositiveBodyLiteralInfoByPatternIndex

template<class PatternType>
class PivotPositiveBodyLiteralInfoByPatternIndex : public LiteralPatternIndex<PivotPositiveBodyLiteralInfo, PatternType> {

public:

    PivotPositiveBodyLiteralInfoByPatternIndex(MemoryManager& memoryManager);

    template<bool isMain, ComponentLevelFilter componentLevelFilter, bool callMonitor, class ConsumerType>
    void applyRulesTo(ThreadContext& threadContext, const size_t workerIndex, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const size_t componentLevel, MaterializationMonitor* const materializationMonitor, ConsumerType consumer);

};

// PivotUnderlyingLiteralInfoByPatternIndex

template<class PatternType>
class PivotUnderlyingLiteralInfoByPatternIndex : public LiteralPatternIndex<typename PatternType::PivotUnderlyingLiteralInfoType, PatternType> {

public:

    PivotUnderlyingLiteralInfoByPatternIndex(MemoryManager& memoryManager);

    template<ComponentLevelFilter componentLevelFilter, bool callMonitor, class ConsumerType>
    void applyRulesTo(ThreadContext& threadContext, const size_t workerIndex, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const size_t componentLevel, MaterializationMonitor* const materializationMonitor, ConsumerType consumer);

};

// RuleConstantIndex

class RuleConstantIndex : private Unmovable {

protected:

    struct Entry {
        ArgumentIndex m_argumentsBufferIndex;
        RuleInfo::RuleConstant* m_firstRuleConstant;

        Entry();
    };

    std::unordered_map<ResourceID, Entry> m_index;

public:

    RuleConstantIndex();

    void initialize();

    void newConstant(const ResourceID resourceID, const ArgumentIndex argumentsBufferIndex);

    bool getConstant(const ResourceID resourceID, ArgumentIndex& argumentsBufferIndex, RuleInfo::RuleConstant* & firstRuleConstant) const;

    void addRuleConstant(RuleInfo::RuleConstant& ruleConstant);

    void removeRuleConstant(RuleInfo::RuleConstant& ruleConstant);

};

// RuleIndex

class RuleIndex : private Unmovable {

protected:

    struct ByThreadInfo : private ::Unmovable {
        ResourceValueCache m_resourceValueCache;
        std::vector<ResourceID> m_argumentBufferMain;
        std::vector<ResourceID> m_argumentBufferIncremental;
        // *Main body literal filters
        const TupleFilter* m_mainPositiveBodyLiteralBeforePivotFilter;
        const TupleFilter* m_mainPositiveBodyLiteralAfterPivotFilter;
        const TupleFilter* m_mainNegativeBodyLiteralSingleAtomBeforePivotFilter;
        const TupleFilter* m_mainNegativeBodyLiteralMultipleAtomsBeforePivotFilter;
        const TupleFilter* m_mainNegativeBodyLiteralPivotOrAfterPivotFilter;
        // *Main rule evaluation filters
        const TupleFilter* m_mainRuleEvaluatorPositiveFilter;
        const TupleFilter* m_mainRuleEvaluatorNegativeFilter;
        // *Incremental body literal filters
        const TupleFilter* m_incrementalPositiveBodyLiteralBeforePivotFilter;
        const TupleFilter* m_incrementalPositiveBodyLiteralAfterPivotFilter;
        const TupleFilter* m_incrementalNegativeBodyLiteralFilterIncremental;
        // *Incremental rule evalution filters
        const TupleFilter* m_incrementalRuleEvaluatorPositiveFilter;
        const TupleFilter* m_incrementalRuleEvaluatorNegativeFilter;
        // Supporting facts filters
        const TupleFilter* m_supportingFactsPositiveFilter;
        const TupleFilter* m_supportingFactsNegativeFilter;
        // Underlying literal filters
        const TupleFilter* m_underlyingLiteralBeforePivotFilter;
        const TupleFilter* m_underlyingLiteralAfterPivotFilter;

        ByThreadInfo(const Dictionary& dictionary, MemoryManager& memoryManager, const std::vector<ResourceID>& defaultArgumentsBuffer);

    };

    struct ComponentLevelInfo {
        bool m_hasRulesWithNegation;
        bool m_hasRulesWithAggregation;
        bool m_hasPivotlessRules;
        bool m_hasNonrecursiveRules;
        bool m_hasRecursiveRules;

        ComponentLevelInfo();

        void reset();

    };

    template<TupleIteratorFilterType FilterType, typename TargetType, typename PositiveCheckType, typename NegativeCheckType>
    friend class TupleIteratorFilters;
    template<BodyLiteralInfoFilterType FilterType, typename TargetType, typename CheckType>
    friend class BodyLiteralInfoFilter;
    template<UnderlyingLiteralInfoFilterType FilterType, typename TargetType, typename CheckType>
    friend class UnderlyingLiteralInfoFilter;
    template<class InfoType, class NonPivotInfoType>
    friend class LiteralInfoBase;
    friend class BodyLiteralInfo;
    friend class PivotPositiveBodyLiteralInfo;
    friend class PivotCompositeBodyLiteralInfo;
    friend class PivotNegationBodyLiteralInfo;
    friend class NonPivotBodyLiteralInfo;
    friend UnderlyingNegationLiteralInfo;
    friend PivotUnderlyingNegationLiteralInfo;
    friend NonPivotUnderlyingNegationLiteralInfo;
    friend class HeadAtomInfo;
    friend class HeadAtomInfoPatternMain;
    friend class PivotPositiveBodyLiteralInfoPatternMain;
    friend class PivotPositiveBodyLiteralInfoPatternIncremental;
    friend class PivotUnderlyingNegationLiteralInfoPatternMain;
    friend class RuleInfo;

    DataStore& m_dataStore;
    const EqualityManager& m_equalityManager;
    EqualityManager m_provingEqualityManager;
    bool m_fastDestructors;
    TermArray m_termArray;
    LogicFactory m_logicFactory;
    std::vector<ResourceID> m_defaultArgumentsBuffer;
    unique_ptr_vector<ByThreadInfo> m_byThreadInfos;
    // Dependency graph
    DependencyGraph m_dependencyGraph;
    size_t m_componentLevelFilterLength;
    std::vector<ComponentLevelInfo> m_componentLevelInfos;
    // Index of BodyLiteralInfo objects
    BodyLiteralInfoIndex m_bodyLiteralInfoIndex;
    // Index of UnderlyingNegationLiteralInfo objects
    UnderlyingNegationLiteralInfoIndex m_underlyingNegationLiteralInfoIndex;
    // Index for materialization
    HeadAtomInfoByPatternIndexMain m_headAtomInfoByPatternIndexMain;
    PivotPositiveBodyLiteralInfoByPatternIndexMain m_pivotPositiveBodyLiteralInfoByPatternIndexMain;
    PivotUnderlyingNegationLiteralInfoByPatternIndexMain m_pivotUnderlyingNegationLiteralInfoByPatternIndexMain;
    // Index for incremental reasoning
    PivotPositiveBodyLiteralInfoByPatternIndexIncremental m_pivotPositiveBodyLiteralInfoByPatternIndexIncremental;
    // Index of rules by constants they contain
    RuleConstantIndex m_ruleConstantIndex;
    // m_ruleInfosByRule must die before other indexes, or the destructor of BodyLiteralInfo crashes.
    // Therefore, this member *must* be declared after the ones before.
    std::unordered_map<Rule, std::unique_ptr<RuleInfo> > m_ruleInfosByRule;
    RuleInfo* m_firstRuleInfo;
    RuleInfo* m_lastRuleInfo;
    size_t m_numberOfPivotlessRules;
    size_t m_numberOfRulesWithNegation;
    size_t m_numberOfRulesWithAggregation;
    size_t m_numberOfJustAddedRuleInfos;
    size_t m_numberOfJustDeletedRuleInfos;

    void addRuleInfo(const Rule& rule, const bool isInternalRule, const bool inMain, const bool justAdded, const bool justDeleted);

    size_t getCurrentNumberOfThreads() const;

    void updateDependencyGraph();

public:

    RuleIndex(DataStore& dataStore);

    ~RuleIndex();

    always_inline EqualityManager& getProvingEqualityManager() {
        return m_provingEqualityManager;
    }

    always_inline LogicFactory& getLogicFactory() {
        return m_logicFactory;
    }

    void initialize();

    always_inline const RuleInfo* getFirstRuleInfo() const {
        return m_firstRuleInfo;
    }

    always_inline const RuleInfo* getLastRuleInfo() const {
        return m_lastRuleInfo;
    }

    always_inline static const RuleInfo* getNextRuleInfo(const RuleInfo* const ruleInfo) {
        return ruleInfo->m_nextRuleInfo;
    }

    const RuleInfo* getRuleInfoFor(const Rule& rule) const;

    always_inline bool isStratified() const {
        return m_dependencyGraph.isStratified();
    }

    always_inline const std::vector<std::vector<const DependencyGraphNode*> >& getUnstratifiedComponents() const {
        return m_dependencyGraph.getUnstratifiedComponents();
    }

    always_inline size_t getFirstRuleComponentLevel() const {
        return m_dependencyGraph.getFirstRuleComponentLevel();
    }

    always_inline size_t getMaxComponentLevel() const {
        return m_dependencyGraph.getMaxComponentLevel();
    }

    always_inline bool hasRules(const size_t componentLevel) const {
        return hasNonrecursiveRules(componentLevel) || hasRecursiveRules(componentLevel);
    }

    always_inline bool hasRulesWithNegation(const size_t componentLevel) const {
        if (componentLevel == static_cast<size_t>(-1))
            return m_numberOfRulesWithNegation > 0;
        else
            return m_componentLevelInfos[componentLevel].m_hasRulesWithNegation;
    }

    always_inline bool hasRulesWithAggregation(const size_t componentLevel) const {
        if (componentLevel == static_cast<size_t>(-1))
            return m_numberOfRulesWithAggregation > 0;
        else
            return m_componentLevelInfos[componentLevel].m_hasRulesWithAggregation;
    }

    always_inline bool hasPivotlessRules(const size_t componentLevel) const {
        if (componentLevel == static_cast<size_t>(-1))
            return m_numberOfPivotlessRules > 0;
        else
            return m_componentLevelInfos[componentLevel].m_hasPivotlessRules;
    }
    
    always_inline bool hasNonrecursiveRules(const size_t componentLevel) const {
        // In the 'no levels' more, only pivotless rules are nonrecursive.
        if (componentLevel == static_cast<size_t>(-1))
            return m_numberOfPivotlessRules > 0;
        else
            return m_componentLevelInfos[componentLevel].m_hasNonrecursiveRules;
    }

    always_inline bool hasRecursiveRules(const size_t componentLevel) const {
        if (componentLevel == static_cast<size_t>(-1))
            return m_firstRuleInfo != nullptr;
        else
            return m_componentLevelInfos[componentLevel].m_hasRecursiveRules;
    }

    always_inline bool hasJustAddedRules() const {
        return m_numberOfJustAddedRuleInfos > 0;
    }

    always_inline bool hasJustDeletedRules() const {
        return m_numberOfJustDeletedRuleInfos > 0;
    }

    always_inline size_t getComponentLevel(const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes) const {
        return m_dependencyGraph.getComponentLevel(argumentsBuffer, argumentIndexes);
    }

    void forgetTemporaryConstants();

    void resetProving();

    void setThreadCapacity(const size_t numberOfThreads);

    void ensureThreadReady(const size_t threadIndex);

    bool addRule(const size_t currentNumberOfThreads, const Rule& rule, const bool isInternalRule);

    bool removeRule(const Rule& rule);

    void propagateDeletions();

    void propagateInsertions();

    void enqueueDeletedRules(LockFreeQueue<RuleInfo*>& ruleQueue);

    void enqueueInsertedRules(LockFreeQueue<RuleInfo*>& ruleQueue);

    template<bool checkComponentLevel>
    void enqueueRulesWithoutPositivePivot(const size_t componentLevel, LockFreeQueue<RuleInfo*>& ruleQueue);

    bool ensureConstantsAreNormalizedMain();

    template<bool checkComponentLevel>
    void enqueueRulesToReevaluateMain(const ResourceID mergedID, const size_t componentLevel, LockFreeQueue<RuleInfo*>& ruleQueue);

    bool ensureConstantsAreNormalizedIncremental();

    template<bool checkComponentLevel>
    void enqueueRulesToReevaluateIncremental(const ResourceID mergedID, const size_t componentLevel, LockFreeQueue<RuleInfo*>& ruleQueue);

    template<ComponentLevelFilter componentLevelFilter, bool callMonitor, class ConsumerType>
    void applyRulesToPositiveLiteralMain(ThreadContext& threadContext, const size_t workerIndex, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const size_t componentLevel, MaterializationMonitor* const materializationMonitor, ConsumerType consumer);

    template<ComponentLevelFilter componentLevelFilter, bool callMonitor, class ConsumerType>
    void applyRulesToPositiveLiteralIncremental(ThreadContext& threadContext, const size_t workerIndex, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const size_t componentLevel, MaterializationMonitor* const materializationMonitor, ConsumerType consumer);

    template<ComponentLevelFilter componentLevelFilter, bool callMonitor, class ConsumerType>
    void applyRulesToUnderlyingNegationLiteralMain(ThreadContext& threadContext, const size_t workerIndex, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const size_t componentLevel, MaterializationMonitor* const materializationMonitor, ConsumerType consumer);

    HeadAtomInfo* getMatchingHeadAtomInfos(ThreadContext& threadContext, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const size_t indexingPatternNumber);

    template<TupleIteratorFilterType FilterType, typename TargetType, typename PositiveCheckType, typename NegativeCheckType>
    TupleIteratorFilters<FilterType, TargetType, PositiveCheckType, NegativeCheckType> setTupleIteratorFilters(TargetType& target, PositiveCheckType positiveCheck, NegativeCheckType negativeCheck);

    template<TupleIteratorFilterType FilterType, typename TargetType, typename CheckType>
    TupleIteratorFilters<FilterType, TargetType, CheckType, CheckType> setTupleIteratorFilters(TargetType& target, CheckType check);

    template<BodyLiteralInfoFilterType FilterType, typename TargetType, typename CheckType>
    BodyLiteralInfoFilter<FilterType, TargetType, CheckType> setBodyLiteralInfoFilter(TargetType& target, CheckType check);

    template<UnderlyingLiteralInfoFilterType FilterType, typename TargetType, typename CheckType>
    UnderlyingLiteralInfoFilter<FilterType, TargetType, CheckType> setUnderlyingLiteralInfoFilter(TargetType& target, CheckType check);

    void recompileRules();

    void save(OutputStream& outputStream) const;

    void load(InputStream& inputStream);

};

// Some members that cannot be defined within class declaration due to the declaration order

template<class InfoType, class NonPivotInfoType>
template<uint8_t ruleType>
always_inline const std::vector<ResourceID>& LiteralInfoBase<InfoType, NonPivotInfoType>::getArgumentsBuffer(const size_t threadIndex) const {
    switch (ruleType) {
    case 0:
    case 1:
    case 2:
        return m_ruleIndex.m_byThreadInfos[threadIndex]->m_argumentBufferMain;
    case 3:
        return m_ruleIndex.m_byThreadInfos[threadIndex]->m_argumentBufferIncremental;
    default:
        UNREACHABLE;
    }
}

always_inline const Atom& HeadAtomInfo::getAtom() const {
    return m_ruleInfo.getRule()->getHead(m_headAtomIndex);
}

template<bool checkComponentLevel>
always_inline bool HeadAtomInfo::isRecursive() const {
    return checkComponentLevel ? m_recursive : !m_ruleInfo.m_pivotless;
}

always_inline const std::vector<ResourceID>& RuleInfo::getDefaultArgumentsBuffer() const {
    return m_ruleIndex.m_defaultArgumentsBuffer;
}

#endif /* RULEINDEX_H_ */
