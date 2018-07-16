// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef NODESERVERQUERYLISTENER_H_
#define NODESERVERQUERYLISTENER_H_

#include "../Common.h"

class ResourceValueCache;
class ResourceIDMapper;
class ThreadContext;

class NodeServerQueryListener {

public:

    virtual ~NodeServerQueryListener() {
    }

    virtual void queryAnswer(const ResourceValueCache& resourceValueCache, const ResourceIDMapper& resourceIDMapper, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const size_t multiplicity) = 0;

    virtual void queryAnswersComplete() = 0;

    virtual void queryInterrupted() = 0;

    always_inline static bool isGlobalResourceID(const ResourceID resourceID) {
        return (resourceID & 0xC000000000000000ULL) == 0x4000000000000000ULL;
    }

    always_inline static ResourceID unmaskGlobalResourceID(const ResourceID resourceID) {
        return resourceID & 0x3fffffffffffffffULL;
    }

};

#endif // NODESERVERQUERYLISTENER_H_
