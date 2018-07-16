// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef DICTIONARYUSAGECONTEXT_H_
#define DICTIONARYUSAGECONTEXT_H_

#include "../Common.h"

struct DictionaryUsageContext {
    ResourceID m_nextFreeResourceID;
    ResourceID m_afterLastFreeResourceID;
    uint32_t m_nextFreePrefixID;
    uint32_t m_afterLastFreePrefixID;
    uint64_t m_nextFreeDataPoolSegment;
    uint64_t m_afterLastFreeDataPoolSegment;

    DictionaryUsageContext() :
        m_nextFreeResourceID(0),
        m_afterLastFreeResourceID(0),
        m_nextFreePrefixID(0),
        m_afterLastFreePrefixID(0),
        m_nextFreeDataPoolSegment(0),
        m_afterLastFreeDataPoolSegment(0)
    {
    }
};

#endif /* DICTIONARYUSAGECONTEXT_H_ */
