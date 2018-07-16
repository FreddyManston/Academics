// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef TUPLEITERATORMONITOR_H_
#define TUPLEITERATORMONITOR_H_

#include "../Common.h"

class TupleIterator;

class TupleIteratorMonitor {

public:

    virtual ~TupleIteratorMonitor() {
    }

    virtual void iteratorOpened(const TupleIterator& tupleIterator, const size_t multiplicity) = 0;

    virtual void iteratorAdvanced(const TupleIterator& tupleIterator, const size_t multiplicity) = 0;

};

#endif /* TUPLEITERATORMONITOR_H_ */
