// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "../dictionary/Dictionary.h"
#include "../storage/DataStore.h"
#include "MaterializationMonitor.h"
#include "AbstractMaterializationTask.h"

void AbstractMaterializationTask::doInitialize() {
    if (m_datalogEngine.getDataStore().getEqualityAxiomatizationType() != EQUALITY_AXIOMATIZATION_OFF) {
        if (!m_mergedConstants.initializeLarge())
            throw RDF_STORE_EXCEPTION("Cannot initialize the buffer for merged constants.");
        if (!m_hasReflexiveSameAs.initializeLarge() || !m_hasReflexiveSameAs.ensureEndAtLeast(m_datalogEngine.getDataStore().getDictionary().getMaxResourceID()))
            throw RDF_STORE_EXCEPTION("Cannot initialize the bit-set for tracking reflexive owl:sameAs triples.");
    }
    m_numberOfWaitingWorkers = 0;
    m_taskRunning = 1;
}

AbstractMaterializationTask::AbstractMaterializationTask(DatalogEngine& datalogEngine, MaterializationMonitor* const materializationMonitor, const size_t componentLevel) :
    ReasoningTask(datalogEngine),
    m_materializationMonitor(materializationMonitor),
    m_componentLevel(componentLevel),
    m_mutex(),
    m_tripleTable(m_datalogEngine.getDataStore().getTupleTable("internal$rdf")),
    m_proxies(),
    m_mergedConstants(m_datalogEngine.getDataStore().getMemoryManager()),
    m_ruleQueue(m_datalogEngine.getDataStore().getMemoryManager()),
    m_hasReflexiveSameAs(m_datalogEngine.getDataStore().getMemoryManager()),
    m_taskRunning(0),
    m_numberOfWaitingWorkers(0),
    m_numberOfActiveProxies(0),
    m_lowestWriteTupleIndex(INVALID_TUPLE_INDEX)
{
}
