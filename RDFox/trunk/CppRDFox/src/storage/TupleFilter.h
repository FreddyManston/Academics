// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef TUPLEFILTER_H_
#define TUPLEFILTER_H_

#include "../Common.h"

class TupleFilter {

public:

    virtual ~TupleFilter() {
    }

    virtual bool processTuple(const void* const tupleFilterContext, const TupleIndex tupleIndex, const TupleStatus tupleStatus) const = 0;

};

#endif /* TUPLEFILTER_H_ */
