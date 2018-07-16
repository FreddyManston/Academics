// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef RESOURCEVALUERESOLVER_H_
#define RESOURCEVALUERESOLVER_H_

#include "../Common.h"

class ResourceValueResolver {

public:

    bool getResource(const ResourceID resourceID, ResourceValue& resourceValue) const;

    ResourceID tryResolveResource(ThreadContext& threadContext, const ResourceValue& resourceValue) const;

    ResourceID resolveResource(ThreadContext& threadContext, DictionaryUsageContext* dictionaryUsageContext, const ResourceValue& resourceValue);

};


#endif /* RESOURCEVALUERESOLVER_H_ */
