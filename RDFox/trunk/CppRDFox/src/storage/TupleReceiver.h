// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef TUPLERECEIVER_H_
#define TUPLERECEIVER_H_

#include "../Common.h"

class TupleReceiver : private Unmovable {

public:

    virtual ~TupleReceiver() {
    }

    virtual std::pair<bool, TupleIndex> addTuple(ThreadContext& threadContext, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const TupleStatus deleteTupleStatus, const TupleStatus addTupleStatus) = 0;

    virtual std::pair<bool, TupleIndex> addTuple(const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const TupleStatus deleteTupleStatus, const TupleStatus addTupleStatus) = 0;

};

#endif /* TUPLERECEIVER_H_ */
