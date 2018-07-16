// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef QUERYPATTERNS_H_
#define QUERYPATTERNS_H_

#include "TripleTable.h"
#include "TripleTableIterator.h"

template<class Configuration>
struct Query1On1 {
    static const TripleTableIteratorType ITERATOR_TYPE = ITERATE_NEXT;
    static const ResourceComponent ITERATE_NEXT_ON = Configuration::TwoKeysManager1Type::COMPONENT1;
    static const ResourceComponent ITERATE_COMPARE_WITH = static_cast<ResourceComponent>(-1);

    static always_inline TupleIndex getFirstTripleIndex(ThreadContext& threadContext, const TripleTable<Configuration>& tripleTable, const ResourceID s, const ResourceID p, const ResourceID o, ResourceID& compareResourceID, TupleIndex& compareGroupedMask) {
        return tripleTable.m_twoKeysManager1.getFirstTripleIndex1(threadContext, s, p, o);
    }

    static always_inline size_t getCountEstimate(ThreadContext& threadContext, const TripleTable<Configuration>& tripleTable, ResourceID s, ResourceID p, ResourceID o) {
        return tripleTable.m_twoKeysManager1.getCountEstimate1(threadContext, s, p, o);
    }
};

template<class Configuration>
struct Query1On12 {
    static const TripleTableIteratorType ITERATOR_TYPE = ITERATE_COMPARE;
    static const ResourceComponent ITERATE_NEXT_ON = Configuration::TwoKeysManager1Type::COMPONENT1;
    static const ResourceComponent ITERATE_COMPARE_WITH = Configuration::TwoKeysManager1Type::COMPONENT2;

    static always_inline TupleIndex getFirstTripleIndex(ThreadContext& threadContext, const TripleTable<Configuration>& tripleTable, const ResourceID s, const ResourceID p, const ResourceID o, ResourceID& compareResourceID, TupleIndex& compareGroupedMask) {
        return tripleTable.m_twoKeysManager1.getFirstTripleIndex12(threadContext, s, p, o, compareResourceID, compareGroupedMask);
    }

    static always_inline size_t getCountEstimate(ThreadContext& threadContext, const TripleTable<Configuration>& tripleTable, ResourceID s, ResourceID p, ResourceID o) {
        return tripleTable.m_twoKeysManager1.getCountEstimate12(threadContext, s, p, o);
    }
};

template<class Configuration>
struct Query1On13 {
    static const TripleTableIteratorType ITERATOR_TYPE = ITERATE_COMPARE;
    static const ResourceComponent ITERATE_NEXT_ON = Configuration::TwoKeysManager1Type::COMPONENT1;
    static const ResourceComponent ITERATE_COMPARE_WITH = Configuration::TwoKeysManager1Type::COMPONENT3;

    static always_inline TupleIndex getFirstTripleIndex(ThreadContext& threadContext, const TripleTable<Configuration>& tripleTable, const ResourceID s, const ResourceID p, const ResourceID o, ResourceID& compareResourceID, TupleIndex& compareGroupedMask) {
        return tripleTable.m_twoKeysManager1.getFirstTripleIndex13(threadContext, s, p, o, compareResourceID, compareGroupedMask);
    }

    static always_inline size_t getCountEstimate(ThreadContext& threadContext, const TripleTable<Configuration>& tripleTable, ResourceID s, ResourceID p, ResourceID o) {
        return tripleTable.m_twoKeysManager1.getCountEstimate13(threadContext, s, p, o);
    }
};

template<class Configuration>
struct Query2On1 {
    static const TripleTableIteratorType ITERATOR_TYPE = ITERATE_NEXT;
    static const ResourceComponent ITERATE_NEXT_ON = Configuration::TwoKeysManager2Type::COMPONENT1;
    static const ResourceComponent ITERATE_COMPARE_WITH = static_cast<ResourceComponent>(-1);

    static always_inline TupleIndex getFirstTripleIndex(ThreadContext& threadContext, const TripleTable<Configuration>& tripleTable, const ResourceID s, const ResourceID p, const ResourceID o, ResourceID& compareResourceID, TupleIndex& compareGroupedMask) {
        return tripleTable.m_twoKeysManager2.getFirstTripleIndex1(threadContext, s, p, o);
    }

    static always_inline size_t getCountEstimate(ThreadContext& threadContext, const TripleTable<Configuration>& tripleTable, ResourceID s, ResourceID p, ResourceID o) {
        return tripleTable.m_twoKeysManager2.getCountEstimate1(threadContext, s, p, o);
    }
};

template<class Configuration>
struct Query2On12 {
    static const TripleTableIteratorType ITERATOR_TYPE = ITERATE_COMPARE;
    static const ResourceComponent ITERATE_NEXT_ON = Configuration::TwoKeysManager2Type::COMPONENT1;
    static const ResourceComponent ITERATE_COMPARE_WITH = Configuration::TwoKeysManager2Type::COMPONENT2;

    static always_inline TupleIndex getFirstTripleIndex(ThreadContext& threadContext, const TripleTable<Configuration>& tripleTable, const ResourceID s, const ResourceID p, const ResourceID o, ResourceID& compareResourceID, TupleIndex& compareGroupedMask) {
        return tripleTable.m_twoKeysManager2.getFirstTripleIndex12(threadContext, s, p, o, compareResourceID, compareGroupedMask);
    }

    static always_inline size_t getCountEstimate(ThreadContext& threadContext, const TripleTable<Configuration>& tripleTable, ResourceID s, ResourceID p, ResourceID o) {
        return tripleTable.m_twoKeysManager2.getCountEstimate12(threadContext, s, p, o);
    }
};

template<class Configuration>
struct Query2On13 {
    static const TripleTableIteratorType ITERATOR_TYPE = ITERATE_COMPARE;
    static const ResourceComponent ITERATE_NEXT_ON = Configuration::TwoKeysManager2Type::COMPONENT1;
    static const ResourceComponent ITERATE_COMPARE_WITH = Configuration::TwoKeysManager2Type::COMPONENT3;

    static always_inline TupleIndex getFirstTripleIndex(ThreadContext& threadContext, const TripleTable<Configuration>& tripleTable, const ResourceID s, const ResourceID p, const ResourceID o, ResourceID& compareResourceID, TupleIndex& compareGroupedMask) {
        return tripleTable.m_twoKeysManager2.getFirstTripleIndex13(threadContext, s, p, o, compareResourceID, compareGroupedMask);
    }

    static always_inline size_t getCountEstimate(ThreadContext& threadContext, const TripleTable<Configuration>& tripleTable, ResourceID s, ResourceID p, ResourceID o) {
        return tripleTable.m_twoKeysManager2.getCountEstimate13(threadContext, s, p, o);
    }
};

template<class Configuration>
struct Query3On1 {
    static const TripleTableIteratorType ITERATOR_TYPE = ITERATE_NEXT;
    static const ResourceComponent ITERATE_NEXT_ON = Configuration::TwoKeysManager3Type::COMPONENT1;
    static const ResourceComponent ITERATE_COMPARE_WITH = static_cast<ResourceComponent>(-1);

    static always_inline TupleIndex getFirstTripleIndex(ThreadContext& threadContext, const TripleTable<Configuration>& tripleTable, const ResourceID s, const ResourceID p, const ResourceID o, ResourceID& compareResourceID, TupleIndex& compareGroupedMask) {
        return tripleTable.m_twoKeysManager3.getFirstTripleIndex1(threadContext, s, p, o);
    }

    static always_inline size_t getCountEstimate(ThreadContext& threadContext, const TripleTable<Configuration>& tripleTable, ResourceID s, ResourceID p, ResourceID o) {
        return tripleTable.m_twoKeysManager3.getCountEstimate1(threadContext, s, p, o);
    }
};

template<class Configuration>
struct Query3On12 {
    static const TripleTableIteratorType ITERATOR_TYPE = ITERATE_COMPARE;
    static const ResourceComponent ITERATE_NEXT_ON = Configuration::TwoKeysManager3Type::COMPONENT1;
    static const ResourceComponent ITERATE_COMPARE_WITH = Configuration::TwoKeysManager3Type::COMPONENT2;

    static always_inline TupleIndex getFirstTripleIndex(ThreadContext& threadContext, const TripleTable<Configuration>& tripleTable, const ResourceID s, const ResourceID p, const ResourceID o, ResourceID& compareResourceID, TupleIndex& compareGroupedMask) {
        return tripleTable.m_twoKeysManager3.getFirstTripleIndex12(threadContext, s, p, o, compareResourceID, compareGroupedMask);
    }

    static always_inline size_t getCountEstimate(ThreadContext& threadContext, const TripleTable<Configuration>& tripleTable, ResourceID s, ResourceID p, ResourceID o) {
        return tripleTable.m_twoKeysManager3.getCountEstimate13(threadContext, s, p, o);
    }
};

template<class Configuration>
struct Query3On13 {
    static const TripleTableIteratorType ITERATOR_TYPE = ITERATE_COMPARE;
    static const ResourceComponent ITERATE_NEXT_ON = Configuration::TwoKeysManager3Type::COMPONENT1;
    static const ResourceComponent ITERATE_COMPARE_WITH = Configuration::TwoKeysManager3Type::COMPONENT3;

    static always_inline TupleIndex getFirstTripleIndex(ThreadContext& threadContext, const TripleTable<Configuration>& tripleTable, const ResourceID s, const ResourceID p, const ResourceID o, ResourceID& compareResourceID, TupleIndex& compareGroupedMask) {
        return tripleTable.m_twoKeysManager3.getFirstTripleIndex13(threadContext, s, p, o, compareResourceID, compareGroupedMask);
    }

    static always_inline size_t getCountEstimate(ThreadContext& threadContext, const TripleTable<Configuration>& tripleTable, ResourceID s, ResourceID p, ResourceID o) {
        return tripleTable.m_twoKeysManager3.getCountEstimate(threadContext, s, p, o);
    }
};

#endif /* QUERYPATTERNS_H_ */
