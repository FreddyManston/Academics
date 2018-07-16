// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "../../Common.h"
#include "../../util/SequentialHashTableImpl.h"
#include "../QueryPatternHandlers.h"
#include "../TripleTableImpl.h"
#include "../TripleTableProxyImpl.h"
#include "../sequential/SequentialTripleListImpl.h"
#include "../sequential/SequentialOneKeyIndexImpl.h"
#include "../sequential/SequentialTwoKeysIndexPolicyImpl.h"
#include "../sequential/SequentialThreeKeysIndexPolicyImpl.h"
#include "../managers/TwoKeysManagerGroupByOneImpl.h"
#include "../managers/TwoKeysManagerGroupByTwoImpl.h"
#include "../managers/ThreeKeysManagerImpl.h"
#include "../RDFStoreImpl.h"

struct SequentialTripleTableConfiguration {
    typedef SequentialTripleList TripleListType;
    typedef SequentialOneKeyIndex OneKeyIndexType;

    struct TwoKeysManager1Configuration {
        static const ResourceComponent COMPONENT1 = RC_S;
        static const ResourceComponent COMPONENT2 = RC_P;
        static const ResourceComponent COMPONENT3 = RC_O;
        typedef SequentialTripleTableConfiguration::TripleListType TripleListType;
        typedef SequentialTripleTableConfiguration::OneKeyIndexType OneKeyIndexType;
        typedef SequentialTwoKeysIndexPolicy<TripleListType, COMPONENT1, COMPONENT2> TwoKeysIndexPolicyType;
        typedef SequentialHashTable<TwoKeysIndexPolicyType> TwoKeysIndexType;
    };

    struct TwoKeysManager2Configuration {
        static const ResourceComponent COMPONENT1 = RC_P;
        static const ResourceComponent COMPONENT2 = RC_O;
        static const ResourceComponent COMPONENT3 = RC_S;
        typedef SequentialTripleTableConfiguration::TripleListType TripleListType;
        typedef SequentialTripleTableConfiguration::OneKeyIndexType OneKeyIndexType;
    };

    struct TwoKeysManager3Configuration {
        static const ResourceComponent COMPONENT1 = RC_O;
        static const ResourceComponent COMPONENT2 = RC_P;
        static const ResourceComponent COMPONENT3 = RC_S;
        typedef SequentialTripleTableConfiguration::TripleListType TripleListType;
        typedef SequentialTripleTableConfiguration::OneKeyIndexType OneKeyIndexType;
        typedef SequentialTwoKeysIndexPolicy<TripleListType, COMPONENT1, COMPONENT2> TwoKeysIndexPolicyType;
        typedef SequentialHashTable<TwoKeysIndexPolicyType> TwoKeysIndexType;
    };

    typedef TwoKeysManagerGroupByTwo<TwoKeysManager1Configuration> TwoKeysManager1Type;
    typedef TwoKeysManagerGroupByOne<TwoKeysManager2Configuration> TwoKeysManager2Type;
    typedef TwoKeysManagerGroupByTwo<TwoKeysManager3Configuration> TwoKeysManager3Type;

    struct ThreeKeysManagerConfiguration {
        typedef SequentialTripleTableConfiguration::TripleListType TripleListType;
        typedef SequentialThreeKeysIndexPolicy<TripleListType> ThreeKeysIndexPolicyType;
        typedef SequentialHashTable<ThreeKeysIndexPolicyType> ThreeKeysIndexType;
    };

    typedef ThreeKeysManager<ThreeKeysManagerConfiguration> ThreeKeysManagerType;

    typedef Query1On1<SequentialTripleTableConfiguration> QP1HandlerType;    // Query: s * *
    typedef Query2On1<SequentialTripleTableConfiguration> QP2HandlerType;    // Query: * p *
    typedef Query1On12<SequentialTripleTableConfiguration> QP3HandlerType;   // Query: s p *
    typedef Query3On1<SequentialTripleTableConfiguration> QP4HandlerType;    // Query: * * o
    typedef Query1On13<SequentialTripleTableConfiguration> QP5HandlerType;   // Query: s * o
    typedef Query3On12 <SequentialTripleTableConfiguration> QP6HandlerType;  // Query: * p o

};

template<>
const char* const TripleTableTraits<TripleTable<SequentialTripleTableConfiguration> >::TYPE_NAME = "seq";

template class TripleTable<SequentialTripleTableConfiguration>;

template class RDFStore<TripleTable<SequentialTripleTableConfiguration> >;
