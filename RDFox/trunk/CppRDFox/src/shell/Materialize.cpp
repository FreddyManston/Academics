// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "../RDFStoreException.h"
#include "../storage/DataStore.h"
#include "../storage/TupleTable.h"
#include "../util/MaterializationStatistics.h"
#include "../util/IncrementalStatistics.h"
#include "../util/MaterializationTracer.h"
#include "../util/IncrementalTracer.h"
#include "ShellCommand.h"

class Materialize : public ShellCommand {

public:

    Materialize() : ShellCommand("mat") {
    }

    virtual std::string getOneLineHelp() const {
        return "materializes the data in the store with respect to the current rule set";
    }

    virtual void printHelpPage(std::ostream& output) const {
        output
            << "mat [inc]" << std::endl
            << "    Evaluates (incrementally) the current datalog program on the current data store with the current number of threads." << std::endl;
    }

    always_inline void printStartMessage(Shell& shell, const bool incremental, const size_t numberOfThreads, const bool processComponentsByLevels, const bool useDRedAlgorithm) const {
        OutputProtector protector(shell);
        shell.getOutput() << "Materializing rules " << (incremental ? "incrementally " : "") << "on " << numberOfThreads << " thread" << (numberOfThreads > 1 ? "s" : "");
        if (shell.getDataStore().getEqualityAxiomatizationType() == EQUALITY_AXIOMATIZATION_NO_UNA)
            shell.getOutput() << " with equality but no UNA";
        else if (shell.getDataStore().getEqualityAxiomatizationType() == EQUALITY_AXIOMATIZATION_UNA)
            shell.getOutput() << " with equality and UNA";
        shell.getOutput() << '.' << std::endl;
        if (processComponentsByLevels)
            shell.getOutput() << "Rules will be processed by the levels of their strongly connected components." << std::endl;
        if (incremental)
            shell.getOutput() << "Using the " << (useDRedAlgorithm ? "DRed" : "FBF") <<" algorithm." << std::endl;
    }

    virtual void execute(Shell& shell, Shell::ArgumentsTokenizer& arguments) const {
        if (isDataStoreActive(shell)) {
            bool incremental = false;
            if (arguments.symbolLowerCaseTokenEquals("inc")) {
                incremental = true;
                arguments.nextToken();
            }
            const bool processComponentsByLevels = shell.getBooleanVariable("reason.by-levels");
            const bool useDRedAlgorithm = shell.getBooleanVariable("reason.use-DRed");
            const size_t numberOfThreads = shell.getDataStore().getNumberOfThreads();
            printStartMessage(shell, incremental, numberOfThreads, processComponentsByLevels, useDRedAlgorithm);
            TupleTable& tupleTable = shell.getDataStore().getTupleTable("internal$rdf");
            const size_t numberOfTriplesBeforeReasoning = tupleTable.getTupleCount(TUPLE_STATUS_COMPLETE, TUPLE_STATUS_COMPLETE);
            const size_t numberOfIDBTriplesBeforeReasoning = tupleTable.getTupleCount(TUPLE_STATUS_IDB | TUPLE_STATUS_IDB_MERGED, TUPLE_STATUS_IDB);
            std::unique_ptr<MaterializationMonitor> monitor;
            std::string monitorType = shell.getStringVariable("reason.mat-monitor");
            if (monitorType == "stats") {
                if (incremental)
                    monitor.reset(new IncrementalStatistics());
                else
                    monitor.reset(new MaterializationStatistics());
            }
            else if (monitorType == "trace") {
                if (incremental)
                    monitor.reset(new IncrementalTracer(shell.getPrefixes(), shell.getDataStore().getDictionary(), shell.getOutput()));
                else
                    monitor.reset(new MaterializationTracer(shell.getPrefixes(), shell.getDataStore().getDictionary(), shell.getOutput()));
            }
            else if (monitorType != "off") {
                shell.printLine("Invalid monitor '", monitorType, "' specified in the 'reason.mat-monitor' variable.");
                return;
            }
            const TimePoint materializationStartTime = ::getTimePoint();
            try {
                if (incremental)
                    shell.getDataStore().applyRulesIncrementally(processComponentsByLevels, useDRedAlgorithm, static_cast<IncrementalMonitor*>(monitor.get()));
                else
                    shell.getDataStore().applyRules(processComponentsByLevels, monitor.get());
                const Duration duration = ::getTimePoint() - materializationStartTime;
                OutputProtector protector(shell);
                shell.getOutput() << "Materialization time:      " << duration / 1000.0 << " s." << std::endl;
                const size_t numberOfTriplesAfterReasoning = tupleTable.getTupleCount(TUPLE_STATUS_COMPLETE, TUPLE_STATUS_COMPLETE);
                const size_t numberOfIDBTriplesAfterReasoning = tupleTable.getTupleCount(TUPLE_STATUS_IDB | TUPLE_STATUS_IDB_MERGED, TUPLE_STATUS_IDB);
                shell.getOutput() <<
                    "Total number of stored triples: " << numberOfTriplesBeforeReasoning << " -> " << numberOfTriplesAfterReasoning << "." << std::endl <<
                    "The number of IDB triples:      " << numberOfIDBTriplesBeforeReasoning << " -> " << numberOfIDBTriplesAfterReasoning << "." << std::endl;
                AbstractStatistics* abstractStatistics = dynamic_cast<AbstractStatistics*>(monitor.get());
                if (abstractStatistics != nullptr)
                    shell.getOutput() << abstractStatistics->getStatisticsCounters();
            }
            catch (const RDFStoreException& e) {
                shell.printError(e, "Materialization has been aborted due to errors.");
            }
        }
    }
};

static Materialize s_materialize;
