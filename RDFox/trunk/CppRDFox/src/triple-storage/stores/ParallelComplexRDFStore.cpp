// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "../../Common.h"
#include "../../util/ParallelHashTableImpl.h"
#include "../QueryPatternHandlers.h"
#include "../TripleTableImpl.h"
#include "../TripleTableProxyImpl.h"
#include "../concurrent/ConcurrentTripleListImpl.h"
#include "../concurrent/ConcurrentOneKeyIndexImpl.h"
#include "../concurrent/ConcurrentTwoKeysIndexPolicyImpl.h"
#include "../concurrent/ConcurrentThreeKeysIndexPolicyImpl.h"
#include "../managers/TwoKeysManagerGroupByOneImpl.h"
#include "../managers/TwoKeysManagerGroupByTwoImpl.h"
#include "../managers/ThreeKeysManagerImpl.h"
#include "../RDFStoreImpl.h"

template<typename RI, typename TI>
struct ParallelTripleTableConfiguration {
    typedef ConcurrentTripleList<RI, TI> TripleListType;
    typedef ConcurrentOneKeyIndex<TupleIndex> OneKeyIndexType;

    struct TwoKeysManager1Configuration {
        static const ResourceComponent COMPONENT1 = RC_S;
        static const ResourceComponent COMPONENT2 = RC_P;
        static const ResourceComponent COMPONENT3 = RC_O;
        typedef typename ParallelTripleTableConfiguration<RI, TI>::TripleListType TripleListType;
        typedef typename ParallelTripleTableConfiguration<RI, TI>::OneKeyIndexType OneKeyIndexType;
        typedef ConcurrentTwoKeysIndexPolicy<TripleListType, COMPONENT1, COMPONENT2> TwoKeysIndexPolicyType;
        typedef ParallelHashTable<TwoKeysIndexPolicyType> TwoKeysIndexType;
    };

    struct TwoKeysManager2Configuration {
        static const ResourceComponent COMPONENT1 = RC_P;
        static const ResourceComponent COMPONENT2 = RC_O;
        static const ResourceComponent COMPONENT3 = RC_S;
        typedef typename ParallelTripleTableConfiguration<RI, TI>::TripleListType TripleListType;
        typedef typename ParallelTripleTableConfiguration<RI, TI>::OneKeyIndexType OneKeyIndexType;
    };

    struct TwoKeysManager3Configuration {
        static const ResourceComponent COMPONENT1 = RC_O;
        static const ResourceComponent COMPONENT2 = RC_P;
        static const ResourceComponent COMPONENT3 = RC_S;
        typedef typename ParallelTripleTableConfiguration<RI, TI>::TripleListType TripleListType;
        typedef typename ParallelTripleTableConfiguration<RI, TI>::OneKeyIndexType OneKeyIndexType;
        typedef ConcurrentTwoKeysIndexPolicy<TripleListType, COMPONENT1, COMPONENT2> TwoKeysIndexPolicyType;
        typedef ParallelHashTable<TwoKeysIndexPolicyType> TwoKeysIndexType;
    };

    typedef TwoKeysManagerGroupByTwo<TwoKeysManager1Configuration> TwoKeysManager1Type;
    typedef TwoKeysManagerGroupByOne<TwoKeysManager2Configuration> TwoKeysManager2Type;
    typedef TwoKeysManagerGroupByTwo<TwoKeysManager3Configuration> TwoKeysManager3Type;

    struct ThreeKeysManagerConfiguration {
        typedef typename ParallelTripleTableConfiguration<RI, TI>::TripleListType TripleListType;
        typedef ConcurrentThreeKeysIndexPolicy<TripleListType> ThreeKeysIndexPolicyType;
        typedef ParallelHashTable<ThreeKeysIndexPolicyType> ThreeKeysIndexType;
    };

    typedef ThreeKeysManager<ThreeKeysManagerConfiguration> ThreeKeysManagerType;

    typedef Query1On1<ParallelTripleTableConfiguration>  QP1HandlerType;  // Query: s * *
    typedef Query2On1<ParallelTripleTableConfiguration>  QP2HandlerType;  // Query: * p *
    typedef Query1On12<ParallelTripleTableConfiguration> QP3HandlerType;  // Query: s p *
    typedef Query3On1<ParallelTripleTableConfiguration>  QP4HandlerType;  // Query: * * o
    typedef Query1On13<ParallelTripleTableConfiguration> QP5HandlerType;  // Query: s * o
    typedef Query3On12<ParallelTripleTableConfiguration> QP6HandlerType;  // Query: * p o

};

// Narrow-narrow version

template<>
const char* const TripleTableTraits<TripleTable<ParallelTripleTableConfiguration<uint32_t, uint32_t> > >::TYPE_NAME = "par-complex-nn";

template class TripleTable<ParallelTripleTableConfiguration<uint32_t, uint32_t> >;

template class RDFStore<TripleTable<ParallelTripleTableConfiguration<uint32_t, uint32_t> > >;

// Narrow-wide version

template<>
const char* const TripleTableTraits<TripleTable<ParallelTripleTableConfiguration<uint32_t, uint64_t> > >::TYPE_NAME = "par-complex-nw";

template class TripleTable<ParallelTripleTableConfiguration<uint32_t, uint64_t> >;

template class RDFStore<TripleTable<ParallelTripleTableConfiguration<uint32_t, uint64_t> > >;

// Wide-wide version

template<>
const char* const TripleTableTraits<TripleTable<ParallelTripleTableConfiguration<uint64_t, uint64_t> > >::TYPE_NAME = "par-complex-ww";

template class TripleTable<ParallelTripleTableConfiguration<uint64_t, uint64_t> >;

template class RDFStore<TripleTable<ParallelTripleTableConfiguration<uint64_t, uint64_t> > >;
