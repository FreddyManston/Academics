// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "../formats/InputOutput.h"
#include "../formats/sources/InputStreamSource.h"
#include "../formats/sources/MemoryMappedFileSource.h"
#include "../formats/sources/MemorySource.h"
#include "../logic/Logic.h"
#include "../dictionary/Dictionary.h"
#include "../storage/DataStore.h"
#include "../storage/RuleIterator.h"
#include "../storage/RulePlanIterator.h"
#include "../storage/TupleTable.h"
#include "../storage/TupleIterator.h"
#include "../storage/TupleReceiver.h"
#include "../storage/TupleTableProxy.h"
#include "../equality/EqualityManager.h"
#include "../logic/Logic.h"
#include "../util/ComponentStatistics.h"
#include "../util/File.h"
#include "../util/Mutex.h"
#include "../util/Prefixes.h"
#include "../util/InputImporter.h"
#include "../util/Thread.h"
#include "../util/ThreadContext.h"
#include "Tasks.h"

// Utility functions for loading data

std::unique_ptr<InputSource> createInputSource(File& file) {
    if (file.isRegularFile())
        return std::unique_ptr<InputSource>(new MemoryMappedFileSource(file, 100 * 1024 * 1024));
    else
        return std::unique_ptr<InputSource>(new InputStreamSource<FileDescriptorInputStream>(file, 100 * 1024 * 1024));
}

void parseFile(File& file, Prefixes& prefixes, InputConsumer& inputConsumer) {
    LogicFactory logicFactory(::newLogicFactory());
    std::unique_ptr<InputSource> inputSource = createInputSource(file);
    std::string formatName;
    ::load(*inputSource, prefixes, logicFactory, inputConsumer, formatName);
}

TupleReceiver& chooseTupleReceiver(DataStore& dataStore, TupleTable& tupleTable, UpdateType updateType, std::unique_ptr<TupleTableProxy>& proxy) {
    if (updateType == ADD && dataStore.getNumberOfThreads() > 1 && tupleTable.supportsProxy()) {
        proxy = tupleTable.createTupleTableProxy(PROXY_WINDOW_SIZE);
        proxy->initialize();
        return *proxy;
    }
    else
        return tupleTable;
}

void chooseInputImporter(DataStore& dataStore, TupleTable& tupleTable, UpdateType updateType, TupleReceiver& tupleReceiver, std::ostream& errors, std::unique_ptr<AbstractInputImporter>& inputImporter) {
    if (updateType == ADD)
        inputImporter.reset(new InputImporter(dataStore, &errors, tupleReceiver));
    else if (updateType == SCHEDULE_FOR_ADDITION)
        inputImporter.reset(new AdditionInputImporter(dataStore, &errors));
    else if (updateType == SCHEDULE_FOR_DELETION)
        inputImporter.reset(new DeletionInputImporter(dataStore, &errors));
}

// ImportProgressLogger

class ImportProgressLogger : public InputConsumer {

protected:

    const size_t m_jobIndex;
    TupleTableProxy* m_proxy;
    AbstractInputImporter& m_abstractInputImporter;
    std::ostream& m_output;
    Mutex& m_outputMutex;
    const Duration m_logFrequency;
    TimePoint m_startTimePoint;
    TimePoint m_thisBatchStartTimePoint;
    TimePoint m_thisFileStartTimePoint;
    size_t m_thisFileInitialNumberOfUpdates;
    size_t m_thisBatchInitialNumberOfUpdates;
    std::ostringstream m_message;

public:

    ImportProgressLogger(const size_t jobIndex, TupleTableProxy* proxy, AbstractInputImporter& abstractInputImporter, std::ostream& output, Mutex& outputMutex, const Duration logFrequency) :
        m_jobIndex(jobIndex),
        m_proxy(proxy),
        m_abstractInputImporter(abstractInputImporter),
        m_output(output),
        m_outputMutex(outputMutex),
        m_logFrequency(logFrequency),
        m_startTimePoint(0),
        m_thisBatchStartTimePoint(0),
        m_thisFileStartTimePoint(0),
        m_thisFileInitialNumberOfUpdates(0),
        m_thisBatchInitialNumberOfUpdates(0),
        m_message()
    {
    }

    virtual void start();

    virtual void consumeTriple(const size_t line, const size_t column, const ResourceText& subject, const ResourceText& predicate, const ResourceText& object);

    virtual void finish();

    void outputLogEvent();

    void outputLogEvent(const TimePoint now);

};

void ImportProgressLogger::start() {
    m_thisFileStartTimePoint = m_thisBatchStartTimePoint = ::getTimePoint();
    if (m_startTimePoint == 0)
        m_startTimePoint = m_thisBatchStartTimePoint;
    m_thisFileInitialNumberOfUpdates = m_abstractInputImporter.getNumberOfUpdates();
    m_abstractInputImporter.start();
}

void ImportProgressLogger::consumeTriple(const size_t line, const size_t column, const ResourceText& subject, const ResourceText& predicate, const ResourceText& object) {
    m_abstractInputImporter.consumeTriple(line, column, subject, predicate, object);
    // The following is just so that we don't get the time point every time we read a triple, as that might be very costly.
    if ((m_abstractInputImporter.getNumberOfUpdates() % 1000) == 0) {
        const TimePoint now = ::getTimePoint();
        if (now - m_thisBatchStartTimePoint > m_logFrequency)
            outputLogEvent(now);
    }
}

void ImportProgressLogger::finish() {
    outputLogEvent();
}

void ImportProgressLogger::outputLogEvent() {
    outputLogEvent(::getTimePoint());
}

void ImportProgressLogger::outputLogEvent(const TimePoint now) {
    const size_t numberOfUpdates = m_abstractInputImporter.getNumberOfUpdates();
    m_message << "[" << m_jobIndex << "] TRIPLES total: " << numberOfUpdates << "; this file: " << (numberOfUpdates - m_thisFileInitialNumberOfUpdates) << "; this batch: " << (numberOfUpdates - m_thisBatchInitialNumberOfUpdates) << ". TIME since start: " << (now - m_startTimePoint) / 1000 << " s; this file: " << (now - m_thisFileStartTimePoint) / 1000 << " s" << std::endl << std::ends;
    if (m_proxy != 0) {
        std::unique_ptr<ComponentStatistics> componentStatistics = m_proxy->getComponentStatistics();
        componentStatistics->print(m_message);
        m_message << std::endl << std::endl << std::endl;
    }
    std::string message(m_message.str());
    m_message.clear();
    m_message.seekp(0);
    m_thisBatchStartTimePoint = now;
    m_thisBatchInitialNumberOfUpdates = numberOfUpdates;
    MutexHolder holder(m_outputMutex);
    m_output << message;
}

// class ImportJobs

class ImportJobs {

protected:

    std::vector<std::string> m_jobs;
    Mutex m_mutex;

public:

    ImportJobs() : m_jobs(), m_mutex() {
    }

    always_inline void addJob(const std::string job) {
        m_jobs.push_back(job);
    }

    always_inline bool getJob(std::string& job) {
        MutexHolder holder(m_mutex);
        if (m_jobs.empty())
            return false;
        else {
            job = m_jobs.front();
            m_jobs.erase(m_jobs.begin());
            return true;
        }
    }

};

// ImpoterThread

class ImporterThread : public Thread {

protected:

    const size_t m_jobIndex;
    ImportJobs& m_importJobs;
    DataStore& m_dataStore;
    const Duration m_logFrequency;
    const bool m_logProxy;
    const UpdateType m_updateType;
    const bool m_decomposeRules;
    const bool m_renameBlankNodes;
    Prefixes& m_prefixes;
    std::ostream& m_output;
    std::ostream& m_errors;
    Mutex& m_outputMutex;
    size_t m_numberOfUpdates;
    size_t m_numberOfUniqueUpdates;
    bool m_hasErrors;

public:

    ImporterThread(const size_t jobIndex, ImportJobs& importJobs, DataStore& dataStore, const Duration logFrequency, const bool logProxy, const UpdateType updateType, const bool decomposeRules, const bool renameBlankNodes, Prefixes& prefixes, std::ostream& output, std::ostream& errors, Mutex& outputMutex) :
        m_jobIndex(jobIndex),
        m_importJobs(importJobs),
        m_dataStore(dataStore),
        m_logFrequency(logFrequency),
        m_logProxy(logProxy),
        m_updateType(updateType),
        m_decomposeRules(decomposeRules),
        m_renameBlankNodes(renameBlankNodes),
        m_prefixes(prefixes),
        m_output(output),
        m_errors(errors),
        m_outputMutex(outputMutex),
        m_numberOfUpdates(0),
        m_numberOfUniqueUpdates(0),
        m_hasErrors(false)
    {
    }

    always_inline size_t getNumberOfUpdates() const {
        return m_numberOfUpdates;
    }

    always_inline size_t getNumberOfUniqueUpdates() const {
        return m_numberOfUniqueUpdates;
    }

    always_inline bool hasErrors() const {
        return m_hasErrors;
    }

    always_inline void write(std::ostream& stream, const std::string& message) {
        MutexHolder holder(m_outputMutex);
        stream << message << std::flush;
    }

    virtual void run();

};

void ImporterThread::run() {
    TupleTable& tupleTable = m_dataStore.getTupleTable("internal$rdf");
    InputConsumer* inputConsumer = nullptr;
    std::unique_ptr<AbstractInputImporter> inputImporter;
    std::unique_ptr<TupleTableProxy> proxy;
    std::unique_ptr<ImportProgressLogger> importProgressLogger;
    std::string inputFileName;
    while (m_importJobs.getJob(inputFileName)) {
        File input;
        try {
            input.open(inputFileName, File::OPEN_EXISTING_FILE, true, false, true);
        }
        catch (const RDFStoreException& e) {
            std::ostringstream message;
            message << "Cannot open the specified file." << std::endl << e;
            write(m_errors, message.str());
            m_hasErrors = true;
        }
        if (input.isOpen()) {
            if (inputConsumer == nullptr) {
                TupleReceiver& tupleReceiver = chooseTupleReceiver(m_dataStore, tupleTable, m_updateType, proxy);
                ::chooseInputImporter(m_dataStore, tupleTable, m_updateType, tupleReceiver, m_errors, inputImporter);
                inputImporter->setDecomposeRules(m_decomposeRules);
                inputImporter->setRenameBlankNodes(m_renameBlankNodes);
                inputConsumer = inputImporter.get();
                if (m_logFrequency != 0) {
                    importProgressLogger.reset(new ImportProgressLogger(m_jobIndex, m_logProxy ? proxy .get() : 0, *inputImporter, m_output, m_outputMutex, m_logFrequency));
                    inputConsumer = importProgressLogger.get();
                }
            }
            inputImporter->setCurrentFile(inputFileName);
            try {
                std::ostringstream message;
                message << "Thread " << m_jobIndex << " importing file '" << inputFileName << "'." << std::endl;
                write(m_output, message.str());
                Prefixes prefixes(m_prefixes);
                parseFile(input, prefixes, *inputConsumer);
            }
            catch (const RDFStoreException& e) {
                std::ostringstream message;
                message << "Cannot import the specified file." << std::endl << e;
                write(m_errors, message.str());
                m_hasErrors = true;
            }
            input.close();
        }
    }
    if (proxy.get() != nullptr) {
        ThreadContext& threadContext = ThreadContext::getCurrentThreadContext();
        proxy->invalidateRemainingBuffer(threadContext);
    }
    if (inputImporter.get() != nullptr) {
        m_numberOfUpdates += inputImporter->getNumberOfUpdates();
        m_numberOfUniqueUpdates += inputImporter->getNumberOfUniqueUpdates();
    }
}

// Adding a tuple

std::pair<bool, TupleIndex> addTuple(TupleReceiver& tupleReceiver, const EqualityManager& equalityManager, std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes) {
    return ::addTuple(ThreadContext::getCurrentThreadContext(), tupleReceiver, equalityManager, argumentsBuffer, argumentIndexes);
}

std::pair<bool, TupleIndex> addTuple(ThreadContext& threadContext, TupleReceiver& tupleReceiver, const EqualityManager& equalityManager, std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes) {
    if (equalityManager.isNormal(argumentsBuffer, argumentIndexes))
        return tupleReceiver.addTuple(threadContext, argumentsBuffer, argumentIndexes, 0, TUPLE_STATUS_EDB | TUPLE_STATUS_IDB);
    else {
        std::pair<bool, TupleIndex> result = tupleReceiver.addTuple(threadContext, argumentsBuffer, argumentIndexes, 0, TUPLE_STATUS_EDB);
        equalityManager.normalize(argumentsBuffer, argumentIndexes);
        tupleReceiver.addTuple(threadContext, argumentsBuffer, argumentIndexes, 0, TUPLE_STATUS_IDB);
        return result;
    }
}

// Import text from memory

bool importText(DataStore& dataStore, const char* const textStart, const size_t textLength, const Duration logFrequency, const UpdateType updateType, const bool decomposeRules, const bool renameBlankNodes, Prefixes& prefixes, std::ostream& output, std::ostream& errors, size_t& numberOfUpdates, size_t& numberOfUniqueUpdates) {
    numberOfUpdates = 0;
    numberOfUniqueUpdates = 0;
    TupleTable& tripleTable = dataStore.getTupleTable("internal$rdf");
    std::unique_ptr<AbstractInputImporter> inputImporter;
    chooseInputImporter(dataStore, tripleTable, updateType, tripleTable, errors, inputImporter);
    inputImporter->setDecomposeRules(decomposeRules);
    inputImporter->setRenameBlankNodes(renameBlankNodes);
    InputConsumer* inputConsumer = inputImporter.get();
    std::unique_ptr<ImportProgressLogger> importProgressLogger;
    Mutex outputMutex;
    if (logFrequency != 0) {
        importProgressLogger.reset(new ImportProgressLogger(0, 0, *inputImporter, output, outputMutex, logFrequency));
        inputConsumer = importProgressLogger.get();
    }
    LogicFactory logicFactory(::newLogicFactory());
    MemorySource memorySource(textStart, textLength);
    std::string formatName;
    ::load(memorySource, prefixes, logicFactory, *inputConsumer, formatName);
    if (inputImporter->getNumberOfErrors() > 0)
        return false;
    numberOfUpdates += inputImporter->getNumberOfUpdates();
    numberOfUniqueUpdates += inputImporter->getNumberOfUniqueUpdates();
    return true;
}

bool importFiles(DataStore& dataStore, const std::vector<std::string>& fileNames, const Duration logFrequency, const bool logProxies, const UpdateType updateType, const bool decomposeRules, const bool renameBlankNodes, Prefixes& prefixes, std::ostream& output, std::ostream& errors, size_t& numberOfUpdates, size_t& numberOfUniqueUpdates) {
    numberOfUpdates = 0;
    numberOfUniqueUpdates = 0;
    TupleTable& tripleTable = dataStore.getTupleTable("internal$rdf");
    Mutex outputMutex;
    if (dataStore.getNumberOfThreads() == 1 || fileNames.size() <= 1) {
        for (std::vector<std::string>::const_iterator fileIterator = fileNames.begin(); fileIterator != fileNames.end(); fileIterator++) {
            File input;
            input.open(*fileIterator, File::OPEN_EXISTING_FILE, true, false, true);
            std::unique_ptr<AbstractInputImporter> inputImporter;
            chooseInputImporter(dataStore, tripleTable, updateType, tripleTable, errors, inputImporter);
            inputImporter->setCurrentFile(*fileIterator);
            inputImporter->setDecomposeRules(decomposeRules);
            inputImporter->setRenameBlankNodes(renameBlankNodes);
            InputConsumer* inputConsumer = inputImporter.get();
            std::unique_ptr<ImportProgressLogger> importProgressLogger;
            if (logFrequency != 0) {
                importProgressLogger.reset(new ImportProgressLogger(0, 0, *inputImporter, output, outputMutex, logFrequency));
                inputConsumer = importProgressLogger.get();
            }
            parseFile(input, prefixes, *inputConsumer);
            if (inputImporter->getNumberOfErrors() > 0)
                return false;
            numberOfUpdates += inputImporter->getNumberOfUpdates();
            numberOfUniqueUpdates += inputImporter->getNumberOfUniqueUpdates();
        }
    }
    else {
        ImportJobs importJobs;
        for (std::vector<std::string>::const_iterator fileIterator = fileNames.begin(); fileIterator != fileNames.end(); fileIterator++)
            importJobs.addJob(*fileIterator);
        unique_ptr_vector<ImporterThread> importerThreads;
        for (size_t threadIndex = 0; threadIndex < dataStore.getNumberOfThreads(); ++threadIndex)
            importerThreads.push_back(std::unique_ptr<ImporterThread>(new ImporterThread(threadIndex, importJobs, dataStore, logFrequency, logProxies, updateType, decomposeRules, renameBlankNodes, prefixes, output, errors, outputMutex)));
        for (unique_ptr_vector<ImporterThread>::iterator iterator = importerThreads.begin(); iterator != importerThreads.end(); ++iterator)
            (*iterator)->start();
        for (unique_ptr_vector<ImporterThread>::iterator iterator = importerThreads.begin(); iterator != importerThreads.end(); ++iterator) {
            if ((*iterator)->hasErrors())
                return false;
            (*iterator)->join();
            numberOfUpdates += (*iterator)->getNumberOfUpdates();
            numberOfUniqueUpdates += (*iterator)->getNumberOfUniqueUpdates();
        }
    }
    return true;
}

bool importEDB(DataStore& targetDataStore, const DataStore& sourceDataStore, const Duration logFrequency, const UpdateType updateType, const bool decomposeRules, std::ostream& output, std::ostream& errors, size_t& numberOfUpdates, size_t& numberOfUniqueUpdates) {
    numberOfUpdates = numberOfUniqueUpdates = 0;
    ThreadContext& threadContext = ThreadContext::getCurrentThreadContext();
    const Dictionary& sourceDictionary = sourceDataStore.getDictionary();
    Dictionary& targetDictionary = targetDataStore.getDictionary();
    const EqualityManager& targetEqualityManager = targetDataStore.getEqualityManager();
    bool noErrors = true;
    ArgumentIndexSet noBindings;
    ResourceValue resourceValue;
    const std::map<std::string, TupleTable*>& sourceTupleTablesByName = sourceDataStore.getTupleTablesByName();
    for (auto iterator = sourceTupleTablesByName.begin(); iterator != sourceTupleTablesByName.end(); ++iterator) {
        if (targetDataStore.containsTupleTable(iterator->first)) {
            const TupleTable& sourceTupleTable = *iterator->second;
            const size_t arity = sourceTupleTable.getArity();
            std::vector<ResourceID> sourceArgumentsBuffer(arity, INVALID_RESOURCE_ID);
            std::vector<ResourceID> targetArgumentsBuffer(arity, INVALID_RESOURCE_ID);
            std::vector<ArgumentIndex> argumentIndexes(arity, INVALID_ARGUMENT_INDEX);
            for (ArgumentIndex argumentIndex = 0; argumentIndex < arity; ++argumentIndex)
                argumentIndexes[argumentIndex] = argumentIndex;
            TupleTable& targetTupleTable = targetDataStore.getTupleTable(iterator->first);
            std::unique_ptr<TupleIterator> tupleIterator = sourceTupleTable.createTupleIterator(sourceArgumentsBuffer, argumentIndexes, noBindings, noBindings, TUPLE_STATUS_EDB, TUPLE_STATUS_EDB);
            for (size_t multiplicity = tupleIterator->open(); multiplicity != 0; multiplicity = tupleIterator->advance()) {
                bool tupleOK = true;
                for (ArgumentIndex argumentIndex = 0; tupleOK && argumentIndex < arity; ++argumentIndex) {
                    if (sourceDictionary.getResource(sourceArgumentsBuffer[argumentIndex], resourceValue))
                        targetArgumentsBuffer[argumentIndex] = targetDictionary.resolveResource(threadContext, nullptr, resourceValue);
                    else
                        tupleOK = false;
                }
                if (tupleOK) {
                    ++numberOfUpdates;
                    switch (updateType) {
                    case ADD:
                        if (::addTuple(threadContext, targetTupleTable, targetEqualityManager, targetArgumentsBuffer, argumentIndexes).first)
                            ++numberOfUniqueUpdates;
                        break;
                    case SCHEDULE_FOR_ADDITION:
                        if (targetTupleTable.scheduleForAddition(threadContext, targetArgumentsBuffer, argumentIndexes))
                            ++numberOfUniqueUpdates;
                        break;
                    case SCHEDULE_FOR_DELETION:
                        if (targetTupleTable.scheduleForDeletion(threadContext, targetArgumentsBuffer, argumentIndexes))
                            ++numberOfUniqueUpdates;
                        break;
                    }
                }
            }
        }
        else {
            errors << "Target data store does not contain a tuple table called '" << iterator->first << "'." << std::endl;
            noErrors = false;
        }
    }
    return noErrors;
}

// Print rule plan

void printRulePlan(std::ostream& output, Prefixes& prefixes, const DataStore& dataStore) {
    std::unique_ptr<RuleIterator> ruleIterator = dataStore.createRuleIterator();
    for (bool valid1 = ruleIterator->open(); valid1; valid1 = ruleIterator->advance()) {
        const Rule& rule = ruleIterator->getRule();
        output << rule->toString(prefixes);
        bool hasAnnotation = false;
        if (ruleIterator->isInternalRule()) {
            if (!hasAnnotation) {
                hasAnnotation = true;
            }
            else
                output << ", ";
            output << "INTERNAL";
        }
        if (ruleIterator->isJustAdded()) {
            if (!hasAnnotation) {
                output << "    [";
                hasAnnotation = true;
            }
            else
                output << ", ";
            output << "ADDED";
        }
        if (ruleIterator->isJustDeleted()) {
            if (!hasAnnotation) {
                output << "    [";
                hasAnnotation = true;
            }
            else
                output << ", ";
            output << "DELETED";
        }
        if (hasAnnotation)
            output << "]";
        output << std::endl;
        const size_t numberOfHeadAtoms = rule->getNumberOfHeadAtoms();
        for (size_t headAtomIndex = 0; headAtomIndex < numberOfHeadAtoms; ++headAtomIndex)
            output << "      + " << rule->getHead(headAtomIndex)->toString(prefixes) << '@' << ruleIterator->getHeadAtomComponentLevel(headAtomIndex) << std::endl;
        std::unique_ptr<RulePlanIterator> rulePlanIterator = ruleIterator->createRulePlanIterator();
        for (bool valid2 = rulePlanIterator->open(); valid2; valid2 = rulePlanIterator->advance()) {
            const size_t numberOfLiterals = rulePlanIterator->getNumberOfLiterals();
            if (numberOfLiterals > 0) {
                output << "    - ";
                const Literal& pivotLiteral = rulePlanIterator->getLiteral(0);
                // Output the pivot
                switch (pivotLiteral->getType()) {
                case ATOM_FORMULA:
                    output << pivotLiteral->toString(prefixes);
                    break;
                case NEGATION_FORMULA:
                    {
                        const std::vector<Variable>& existentialVariables = to_reference_cast<Negation>(pivotLiteral).getExistentialVariables();
                        const size_t numberOfUnderlyingLiteralsInPivot = rulePlanIterator->getNumberOfUnderlyingLiteralsInPivot();
                        output << "NOT";
                        if (!existentialVariables.empty()) {
                            output << " EXISTS ";
                            for (auto iterator = existentialVariables.begin(); iterator != existentialVariables.end(); ++iterator) {
                                if (iterator != existentialVariables.begin())
                                    output << ", ";
                                output << (*iterator)->toString(prefixes);
                            }
                            output << " IN";
                        }
                        output << '(';
                        for (size_t underlyingLiteralIndex = 0; underlyingLiteralIndex < numberOfUnderlyingLiteralsInPivot; ++underlyingLiteralIndex) {
                            if (underlyingLiteralIndex > 0)
                                output << ", ";
                            output << rulePlanIterator->getUnderlyingLiteral(underlyingLiteralIndex)->toString(prefixes);
                            if (underlyingLiteralIndex > 0)
                                output << (rulePlanIterator->isUnderlyingLiteralfterPivot(underlyingLiteralIndex) ? "^<=" : "^<");
                        }
                        output << ')';
                    }
                    break;
                default:
                    UNREACHABLE;
                }
                output << '@' << rulePlanIterator->getLiteralComponentLevel(0);
                // Output the rest of the plan.
                for (size_t literalIndex = 1; literalIndex < numberOfLiterals; ++literalIndex)
                    output << ", " << rulePlanIterator->getLiteral(literalIndex)->toString(prefixes) << (rulePlanIterator->isLiteralAfterPivot(literalIndex) ? "^<=" : "^<") << '@' << rulePlanIterator->getLiteralComponentLevel(literalIndex);
                output << " ." << std::endl;
            }
        }
        // Finish
        output << std::endl;
    }
}
