// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef TUPLTABLEPROXY_H_
#define TUPLTABLEPROXY_H_

#include "TupleReceiver.h"
#include "../util/ComponentStatistics.h"

class TupleTable;

class TupleTableProxy : public TupleReceiver {

public:

    virtual ~TupleTableProxy() {
    }

    virtual void initialize() = 0;

    virtual TupleIndex getFirstReservedTupleIndex() const = 0;

    virtual void invalidateRemainingBuffer(ThreadContext& threadContext) = 0;

    virtual TupleIndex getLowerWriteTupleIndex(const TupleIndex otherWriteTupleIndex) const = 0;

    virtual std::unique_ptr<ComponentStatistics> getComponentStatistics() const = 0;

};

#endif /* TUPLTABLEPROXY_H_ */
