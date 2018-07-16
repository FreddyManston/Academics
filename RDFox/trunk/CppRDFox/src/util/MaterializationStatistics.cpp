// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "AbstractStatisticsImpl.h"
#include "MaterializationStatistics.h"

DECLARE_REDIRECTION_TABLE(MATERIALIZATION, MaterializationStatistics::SUMMARY_TITLE + 1);

const char* const* MaterializationStatistics::describeStatistics(size_t& numberOfCountersPerLevel) {
    static const char* const s_counterDescriptions[NUMBER_OF_MATERIALIZATION_COUNTERS] = {
        "+Number of EDB tuples before materialization",
        "+Number of IDB tuples before materialization",
        "+Number of IDB tuples after materialization",
        "-",
        "Non-merged IDB tuples extracted from the store",
        "    Tuples that were not normal after extraction",
        "    Normal tuples matching at least one body atom",
        "        Producing no derivation",
        "Total number of matched body atoms",
        "    The number of matches of the first atom in a rule",
        "        Producing no derivation",
        "Matched rule instances",
        "    Matched rule instances during tuple extraction",
        "    Matched rule instances during rule reevaluation",
        "    Matched rule instances during pivotless rule evaluation",
        "Number of attempts to reevaluate a rule",
        "    Producing no derivation",
        "Derived equalities (via rules or via data)",
        "    Successful",
        "Derivations",
        "    Via replacement rules (at extraction or due to merging)",
        "        Successful",
        "    Via reflexivity rules",
        "        Successful",
        "    Via used-defined rules",
        "        Not normal at derivation time",
        "        Successful",
    };
    numberOfCountersPerLevel = NUMBER_OF_MATERIALIZATION_COUNTERS;
    return s_counterDescriptions;
}

MaterializationStatistics::MaterializationStatistics() : AbstractStatisticsImpl<MaterializationMonitor>() {
}

void MaterializationStatistics::taskStarted(const DataStore& dataStore, const size_t maxComponentLevel) {
    AbstractStatisticsImpl<MaterializationMonitor>::taskStarted(dataStore, maxComponentLevel);
    TupleTable& tupleTable = dataStore.getTupleTable("internal$rdf");
    ThreadState& threadState = *m_statesByThread[0];
    threadState.set(SUMMARY_EDB_BEFORE, tupleTable.getTupleCount(TUPLE_STATUS_COMPLETE | TUPLE_STATUS_EDB, TUPLE_STATUS_COMPLETE | TUPLE_STATUS_EDB));
    threadState.set(SUMMARY_IDB_BEFORE, tupleTable.getTupleCount(TUPLE_STATUS_COMPLETE | TUPLE_STATUS_IDB_MERGED | TUPLE_STATUS_IDB, TUPLE_STATUS_COMPLETE | TUPLE_STATUS_IDB));
}

void MaterializationStatistics::taskFinished(const DataStore& dataStore) {
    AbstractStatisticsImpl<MaterializationMonitor>::taskFinished(dataStore);
    TupleTable& tupleTable = dataStore.getTupleTable("internal$rdf");
    ThreadState& threadState = *m_statesByThread[0];
    threadState.set(SUMMARY_IDB_AFTER, tupleTable.getTupleCount(TUPLE_STATUS_COMPLETE | TUPLE_STATUS_IDB_MERGED | TUPLE_STATUS_IDB, TUPLE_STATUS_COMPLETE | TUPLE_STATUS_IDB));
}

void MaterializationStatistics::materializationStarted(const size_t workerIndex) {
    setRedirectionTable(workerIndex, MATERIALIZATION_REDIRECTION);
}
