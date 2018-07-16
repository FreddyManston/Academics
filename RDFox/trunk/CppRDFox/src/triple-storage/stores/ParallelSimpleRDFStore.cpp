// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "../../Common.h"
#include "../../util/ParallelHashTableImpl.h"
#include "../QueryPatternHandlers.h"
#include "../TripleTableImpl.h"
#include "../TripleTableProxyImpl.h"
#include "../concurrent/ConcurrentTripleListImpl.h"
#include "../concurrent/ConcurrentOneKeyIndexImpl.h"
#include "../concurrent/ConcurrentThreeKeysIndexPolicyImpl.h"
#include "../managers/TwoKeysManagerGroupByOneImpl.h"
#include "../managers/ThreeKeysManagerImpl.h"
#include "../RDFStoreImpl.h"

template<typename RI, typename TI>
struct ParallelSimpleTripleTableConfiguration {
    typedef ConcurrentTripleList<RI, TI> TripleListType;
    typedef ConcurrentOneKeyIndex<TupleIndex> OneKeyIndexType;

    struct TwoKeysManager1Configuration {
        static const ResourceComponent COMPONENT1 = RC_S;
        static const ResourceComponent COMPONENT2 = RC_P;
        static const ResourceComponent COMPONENT3 = RC_O;
        typedef typename ParallelSimpleTripleTableConfiguration<RI, TI>::TripleListType TripleListType;
        typedef typename ParallelSimpleTripleTableConfiguration<RI, TI>::OneKeyIndexType OneKeyIndexType;
    };

    struct TwoKeysManager2Configuration {
        static const ResourceComponent COMPONENT1 = RC_P;
        static const ResourceComponent COMPONENT2 = RC_O;
        static const ResourceComponent COMPONENT3 = RC_S;
        typedef typename ParallelSimpleTripleTableConfiguration<RI, TI>::TripleListType TripleListType;
        typedef typename ParallelSimpleTripleTableConfiguration<RI, TI>::OneKeyIndexType OneKeyIndexType;
    };

    struct TwoKeysManager3Configuration {
        static const ResourceComponent COMPONENT1 = RC_O;
        static const ResourceComponent COMPONENT2 = RC_P;
        static const ResourceComponent COMPONENT3 = RC_S;
        typedef typename ParallelSimpleTripleTableConfiguration<RI, TI>::TripleListType TripleListType;
        typedef typename ParallelSimpleTripleTableConfiguration<RI, TI>::OneKeyIndexType OneKeyIndexType;
    };

    typedef TwoKeysManagerGroupByOne<TwoKeysManager1Configuration> TwoKeysManager1Type;
    typedef TwoKeysManagerGroupByOne<TwoKeysManager2Configuration> TwoKeysManager2Type;
    typedef TwoKeysManagerGroupByOne<TwoKeysManager3Configuration> TwoKeysManager3Type;

    struct ThreeKeysManagerConfiguration {
        typedef typename ParallelSimpleTripleTableConfiguration<RI, TI>::TripleListType TripleListType;
        typedef ConcurrentThreeKeysIndexPolicy<TripleListType> ThreeKeysIndexPolicyType;
        typedef ParallelHashTable<ThreeKeysIndexPolicyType> ThreeKeysIndexType;
    };

    typedef ThreeKeysManager<ThreeKeysManagerConfiguration> ThreeKeysManagerType;

    typedef Query1On1<ParallelSimpleTripleTableConfiguration>  QP1HandlerType;  // Query: s * *
    typedef Query2On1<ParallelSimpleTripleTableConfiguration>  QP2HandlerType;  // Query: * p *
    typedef Query1On12<ParallelSimpleTripleTableConfiguration> QP3HandlerType;  // Query: s p *
    typedef Query3On1<ParallelSimpleTripleTableConfiguration>  QP4HandlerType;  // Query: * * o
    typedef Query1On13<ParallelSimpleTripleTableConfiguration> QP5HandlerType;  // Query: s * o
    typedef Query3On12<ParallelSimpleTripleTableConfiguration> QP6HandlerType;  // Query: * p o

};

// Narrow-narrow version

template<>
const char* const TripleTableTraits<TripleTable<ParallelSimpleTripleTableConfiguration<uint32_t, uint32_t> > >::TYPE_NAME = "par-simple-nn";

template class TripleTable<ParallelSimpleTripleTableConfiguration<uint32_t, uint32_t> >;

template class RDFStore<TripleTable<ParallelSimpleTripleTableConfiguration<uint32_t, uint32_t> > >;

// Narrow-wide version

template<>
const char* const TripleTableTraits<TripleTable<ParallelSimpleTripleTableConfiguration<uint32_t, uint64_t> > >::TYPE_NAME = "par-simple-nw";

template class TripleTable<ParallelSimpleTripleTableConfiguration<uint32_t, uint64_t> >;

template class RDFStore<TripleTable<ParallelSimpleTripleTableConfiguration<uint32_t, uint64_t> > >;

// Wide-wide version

template<>
const char* const TripleTableTraits<TripleTable<ParallelSimpleTripleTableConfiguration<uint64_t, uint64_t> > >::TYPE_NAME = "par-simple-ww";

template class TripleTable<ParallelSimpleTripleTableConfiguration<uint64_t, uint64_t> >;

template class RDFStore<TripleTable<ParallelSimpleTripleTableConfiguration<uint64_t, uint64_t> > >;
