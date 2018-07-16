// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef MATERIALIZATIONSTATISTICS_H_
#define MATERIALIZATIONSTATISTICS_H_

#include "../reasoning/MaterializationMonitor.h"
#include "AbstractStatistics.h"

class MaterializationStatistics : public AbstractStatisticsImpl<MaterializationMonitor> {

protected:

    virtual const char* const* describeStatistics(size_t& numberOfCountersPerLevel);

public:

    static const size_t SUMMARY_EDB_BEFORE                  = 0;
    static const size_t SUMMARY_IDB_BEFORE                  = SUMMARY_EDB_BEFORE + 1;
    static const size_t SUMMARY_IDB_AFTER                   = SUMMARY_IDB_BEFORE + 1;
    static const size_t SUMMARY_TITLE                       = SUMMARY_IDB_AFTER + 1;
    static const size_t NUMBER_OF_MATERIALIZATION_COUNTERS  = SUMMARY_TITLE + 1 + NUMBER_OF_DERIVATION_COUNTERS;

    MaterializationStatistics();

    virtual void taskStarted(const DataStore& dataStore, const size_t maxComponentLevel);

    virtual void taskFinished(const DataStore& dataStore);

    virtual void materializationStarted(const size_t workerIndex);

};

#endif /* MATERIALIZATIONSTATISTICS_H_ */
