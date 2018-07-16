// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef TRIPLETABLEPROXY_H_
#define TRIPLETABLEPROXY_H_

#include "../Common.h"
#include "../storage/TupleTableProxy.h"
#include "TripleTable.h"

class Parameters;

template<class TT>
class TripleTableProxyGroup;

template<class TT>
class TripleTableProxy : public TupleTableProxy {

public:

    typedef TT TripleTableType;
    typedef TripleTableProxyGroup<TT> TripleTableProxyGroupType;
    typedef typename TripleTableType::TripleListType TripleListType;
    typedef typename TripleTableType::TwoKeysManager1Type TwoKeysManager1Type;
    typedef typename TripleTableType::TwoKeysManager2Type TwoKeysManager2Type;
    typedef typename TripleTableType::TwoKeysManager3Type TwoKeysManager3Type;
    typedef typename TripleTableType::ThreeKeysManagerType ThreeKeysManagerType;
    typedef typename TwoKeysManager1Type::TwoKeysManagerProxyType TwoKeysManagerProxy1Type;
    typedef typename TwoKeysManager2Type::TwoKeysManagerProxyType TwoKeysManagerProxy2Type;
    typedef typename TwoKeysManager3Type::TwoKeysManagerProxyType TwoKeysManagerProxy3Type;

    friend TripleTableProxyGroupType;

protected:

    TripleTableType& m_tripleTable;
    TwoKeysManagerProxy1Type m_twoKeysManagerProxy1;
    TwoKeysManagerProxy2Type m_twoKeysManagerProxy2;
    TwoKeysManagerProxy3Type m_twoKeysManagerProxy3;
    const size_t m_windowSize;
    TupleIndex m_windowStartTripleIndex;
    TupleIndex m_windowEndTripleIndex;
    TupleIndex m_windowNextTripleIndex;

    void ensureReservationNotEmptyInternal();

public:

    TripleTableProxy(TripleTableType& tripleTable, const Parameters& dataStoreParameters, const size_t windowSize);

    virtual void initialize();

    virtual std::pair<bool, TupleIndex> addTuple(ThreadContext& threadContext, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const TupleStatus deleteTupleStatus, const TupleStatus addTupleStatus);

    virtual std::pair<bool, TupleIndex> addTuple(const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const TupleStatus deleteTupleStatus, const TupleStatus addTupleStatus);

    virtual TupleIndex getFirstReservedTupleIndex() const;

    virtual void invalidateRemainingBuffer(ThreadContext& threadContext);

    virtual TupleIndex getLowerWriteTupleIndex(const TupleIndex otherWriteTupleIndex) const;

    virtual std::unique_ptr<ComponentStatistics> getComponentStatistics() const;

};

#endif /* TRIPLETABLEPROXY_H_ */
