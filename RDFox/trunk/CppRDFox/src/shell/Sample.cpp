// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "../RDFStoreException.h"
#include "../tasks/Tasks.h"
#include "../storage/Parameters.h"
#include "../storage/TupleTable.h"
#include "../dictionary/Dictionary.h"
#include "../Common.h"
#include "../util/Vocabulary.h"
#include "../util/Random.h"
#include "ShellCommand.h"

const TupleStatus SAMPLING_TUPLE_STATUS_MASK = TUPLE_STATUS_COMPLETE | TUPLE_STATUS_EDB | TUPLE_STATUS_IDB_MERGED;
const TupleStatus SAMPING_TUPLE_STATUS_EXPECTED_VALUE = TUPLE_STATUS_COMPLETE | TUPLE_STATUS_EDB;

class Sample : public ShellCommand {
    
public:
    
    Sample() : ShellCommand("sample") {
    }
    
    virtual std::string getOneLineHelp() const {
        return "samples a dataset";
    }
    
    virtual void printHelpPage(std::ostream& output) const {
        output
        << "sample [-] <size> [c|p|pm] <seed> (<store name> | <file name>)" << std::endl
        << "    Extracts a sample S of size <size> from the dataset E stored in <store name>/<file name>, "
        << "    and adds to the current store the sample E\\S, if '-' is specified, or S, otherwise. "
        << "    the size can be specified as a count 'c', as a percentage 'p' or as a permil \"pm\"." << std::endl;
        
    }
    
    enum SizeType {
        Count,
        Percent,
        Permil
    };
    
    size_t getTuplesCount(TupleTable& sourceTable) const {
        size_t numberOfTuples = 0;
        std::vector<ResourceID> tupleBuffer {0, 0, 0};
        std::vector<ArgumentIndex> argumentIndexes {0, 1, 2};
        ArgumentIndexSet emptyIndexSet;
        auto tuplesIterator = sourceTable.createTupleIterator(tupleBuffer, argumentIndexes, emptyIndexSet, emptyIndexSet, SAMPLING_TUPLE_STATUS_MASK, SAMPING_TUPLE_STATUS_EXPECTED_VALUE);
        for(size_t multiplicity = tuplesIterator->open(); multiplicity > 0; multiplicity = tuplesIterator->advance())
            numberOfTuples++;
        return numberOfTuples;
    }
    
    void sampleUniformly(TupleTable& sourceTable, const size_t absoluteSampleSize, Random& rand, std::vector<bool>& sample) const {
        std::vector<ResourceID> tupleBuffer {0, 0, 0};
        std::vector<ArgumentIndex> argumentIndexes {0, 1, 2};
        ArgumentIndexSet emptyIndexSet;
        auto tuplesIterator = sourceTable.createTupleIterator(tupleBuffer, argumentIndexes, emptyIndexSet, emptyIndexSet, SAMPLING_TUPLE_STATUS_MASK, SAMPING_TUPLE_STATUS_EXPECTED_VALUE);
        std::vector<TupleIndex> tupleIndexes;
        for(size_t multiplicity = tuplesIterator->open(); multiplicity > 0; multiplicity = tuplesIterator->advance())
            tupleIndexes.push_back(tuplesIterator->getCurrentTupleIndex());
        size_t currentSampleSize = 0;
        while (currentSampleSize < absoluteSampleSize) {
            size_t index = rand.next() % tupleIndexes.size();
            if (!sample[tupleIndexes[index]]) {
                currentSampleSize++;
                sample[tupleIndexes[index]] = true;
            }
        }
    }
    
    struct Edge {
        
        TupleIndex m_tupleIndex;
        ResourceID m_predicate;
        ResourceID m_object;
        
        Edge(TupleIndex tupleIndex, ResourceID predicate, ResourceID object) : m_tupleIndex(tupleIndex), m_predicate(predicate), m_object(object) {
        }
        
    };
    
    struct SubjectEdges {
        
        size_t m_numberOfEdges;
        std::vector<Edge> m_edges;
        
        SubjectEdges() : m_numberOfEdges(static_cast<size_t>(-1)), m_edges() {
        }
        
    };
    
    struct ResourceIDComparer {
        
        const Dictionary& m_dictionary;
        
        ResourceIDComparer(const Dictionary& dictionary) : m_dictionary(dictionary) {
        }
        
        bool operator()(ResourceID first, ResourceID second) {
            ResourceText firstResourceText;
            m_dictionary.getResource(first, firstResourceText);
            ResourceText secondResourceText;
            m_dictionary.getResource(first, secondResourceText);
            int datatypeDiff = std::strcmp(firstResourceText.m_datatypeIRI.c_str(), secondResourceText.m_datatypeIRI.c_str());
            if (datatypeDiff < 0)
                return true;
            if (datatypeDiff == 0)
                return std::strcmp(firstResourceText.m_lexicalForm.c_str(), secondResourceText.m_lexicalForm.c_str()) < 0;
            return false;
        }
        
        bool operator()(Edge firstTuple, Edge secondTuple) {
            if ((*this)(firstTuple.m_predicate, secondTuple.m_predicate))
                return true;
            if (firstTuple.m_predicate == secondTuple.m_predicate)
                return (*this)(firstTuple.m_object, secondTuple.m_object);
            return false;
        }
        
    };
    
    void sampleUsingRandomWalk(Dictionary& dictionary, TupleTable& sourceTable, const size_t sourceSize, const size_t absoluteSampleSize, Random& rand, std::vector<bool>& sample) const {
        size_t currentSampleSize = 0;
        ResourceID rootResourceID = INVALID_RESOURCE_ID;
        std::vector<SubjectEdges> cache(dictionary.getNextResourceID(), SubjectEdges());
        std::vector<bool> subjectHash(dictionary.getNextResourceID(), false);
        std::vector<ResourceID> subjects;
        std::vector<ResourceID> tupleBuffer {0, 0, 0};
        std::vector<ArgumentIndex> argumentIndexes {0, 1, 2};
        ArgumentIndexSet emptyIndexSet;
        auto subjectsIterator = sourceTable.createTupleIterator(tupleBuffer, argumentIndexes, emptyIndexSet, emptyIndexSet, SAMPLING_TUPLE_STATUS_MASK, SAMPING_TUPLE_STATUS_EXPECTED_VALUE);
        for(size_t multiplicity = subjectsIterator->open(); multiplicity > 0; multiplicity = subjectsIterator->advance())
            if (!subjectHash[tupleBuffer[argumentIndexes[0]]]) {
                subjectHash[tupleBuffer[argumentIndexes[0]]] = true;
                subjects.push_back(tupleBuffer[argumentIndexes[0]]);
            }
        ResourceIDComparer resourceIDComparer(dictionary);
        std::sort(subjects.begin(), subjects.end(), resourceIDComparer);
        const size_t iterationsBeforeResettingRoot = 100 * sourceSize;
        size_t iterationsSinceLastRootReset = iterationsBeforeResettingRoot;
        uint64_t jumpToRootThreshold = static_cast<uint64_t>(0.15 * UINT64_MAX);
        size_t failuresBeforeResettingRoot = 100;
        size_t failures = 0;
        bool shouldResetRoot = true;
        ArgumentIndexSet firstInputArgument;
        firstInputArgument.add(0);
        std::unique_ptr<TupleIterator> sourceTuplesIterator = sourceTable.createTupleIterator(tupleBuffer, argumentIndexes, firstInputArgument, firstInputArgument, SAMPLING_TUPLE_STATUS_MASK, SAMPING_TUPLE_STATUS_EXPECTED_VALUE);
        while (currentSampleSize < absoluteSampleSize) {
            if (shouldResetRoot) {
                tupleBuffer[0] = subjects[rand.next() % subjects.size()];
                shouldResetRoot = false;
                iterationsSinceLastRootReset = 0;
                failures = 0;
                rootResourceID = tupleBuffer[0];
            }
            if (cache[tupleBuffer[0]].m_numberOfEdges == static_cast<size_t>(-1)) {
                cache[tupleBuffer[0]].m_numberOfEdges = 0;
                if (subjectHash[tupleBuffer[0]]) {
                    for(size_t multiplicity = sourceTuplesIterator->open(); multiplicity > 0; multiplicity = sourceTuplesIterator->advance()) {
                        cache[tupleBuffer[0]].m_edges.push_back(Edge(sourceTuplesIterator->getCurrentTupleIndex(), tupleBuffer[1], tupleBuffer[2]));
                        cache[tupleBuffer[0]].m_numberOfEdges++;
                    }
                    std::sort(cache[tupleBuffer[0]].m_edges.begin(), cache[tupleBuffer[0]].m_edges.end(), resourceIDComparer);
                }
            }
            if (cache[tupleBuffer[0]].m_numberOfEdges == 0) {
                if (tupleBuffer[0] == rootResourceID)
                    shouldResetRoot = true;
                else
                    tupleBuffer[0] = rootResourceID;
            }
            else if (rand.next() < jumpToRootThreshold)
                tupleBuffer[0] = rootResourceID;
            else {
                auto edge = cache[tupleBuffer[0]].m_edges[rand.next() % cache[tupleBuffer[0]].m_numberOfEdges];
                if (!sample[edge.m_tupleIndex]) {
                    sample[edge.m_tupleIndex] = true;
                    currentSampleSize++;
                }
                else if (failures++ >= failuresBeforeResettingRoot)
                    shouldResetRoot = true;
                tupleBuffer[0] = edge.m_object;
            }
            if (iterationsSinceLastRootReset++ > iterationsBeforeResettingRoot)
                shouldResetRoot = true;
        }
    }
    
    virtual void execute(Shell& shell, Shell::ArgumentsTokenizer& arguments) const {
        if (isDataStoreActive(shell)) {
            try {
                bool complement = false;
                if (arguments.nonSymbolTokenEquals('-')) {
                    complement = true;
                    arguments.nextToken();
                }
                if (arguments.isNumber()) {
                    const size_t requestedSize = static_cast<size_t>(atoi(arguments.getToken(0).c_str()));
                    SizeType sizeType = Count;
                    arguments.nextToken();
                    if (arguments.symbolLowerCaseTokenEquals("c")) {
                        sizeType = Count;
                        arguments.nextToken();
                    }
                    else if (arguments.symbolLowerCaseTokenEquals("p")) {
                        sizeType = Percent;
                        arguments.nextToken();
                    }
                    else if (arguments.symbolLowerCaseTokenEquals("pm")) {
                        sizeType = Permil;
                        arguments.nextToken();
                    }
                    if (arguments.isNumber()) {
                        const uint64_t seed = static_cast<uint64_t>(atoi(arguments.getToken(0).c_str()));
                        arguments.nextToken();
                        if (arguments.isSymbol() || arguments.isQuotedString() || arguments.isQuotedIRI()) {
                            std::vector<std::string> input;
                            input.push_back(arguments.getToken());
                            arguments.nextToken();
                            TimePoint start = ::getTimePoint();
                            DataStore* sourceStore = shell.getDataStorePointer(input[0]);
                            std::unique_ptr<DataStore> sourceStoreUPtr;
                            shell.printLine("");
                            if (sourceStore == NULL) {
                                size_t numberOfUpdates;
                                size_t numberOfUniqueUpdates;
                                Parameters dataStoreParameters;
                                dataStoreParameters.setString("equality", "off");
                                sourceStoreUPtr = ::newDataStore("seq", dataStoreParameters);
                                sourceStoreUPtr->initialize();
                                shell.printLine("Saimpling from file: ", input[0], ". Importing file...");
                                if(::importFiles(*sourceStoreUPtr, input, static_cast<Duration>(0), false, ADD, false, false, shell.getPrefixes(), shell.getOutput(), shell.getOutput(), numberOfUpdates, numberOfUniqueUpdates)) {
                                    sourceStore = &*sourceStoreUPtr;
                                    shell.printLine((::getTimePoint() - start) / 1000, "s: Import finished.");
                                }
                                else {
                                    shell.printLine("    Errors were encountered during import");
                                    sourceStore = NULL;
                                }
                            }
                            else
                                shell.printLine("Sampling from store ", input[0]);
                            if (sourceStore != NULL) {
                                TupleTable& sourceTable = sourceStore->getTupleTable("internal$rdf");
                                const size_t sourceSize = getTuplesCount(sourceTable);
                                size_t absoluteSampleSize = requestedSize;
                                if (sizeType == Percent)
                                    absoluteSampleSize = requestedSize * sourceSize / 100;
                                else if (sizeType == Permil)
                                    absoluteSampleSize = requestedSize * sourceSize / 1000;
                                if (sourceSize > absoluteSampleSize) {
                                    start = ::getTimePoint();
                                    shell.getOutput() << "    Generating a sample of size " << absoluteSampleSize << " from the input dataset of size " << sourceSize << " using seed " << seed;
                                    Random rand(seed);
                                    std::vector<bool> sample(sourceTable.getFirstFreeTupleIndex(), false);
                                    if (shell.getStringVariable("sampling-method") == "random-walk") {
                                        shell.getOutput() << " and sampling-method \"random-walk\"";
                                        sampleUsingRandomWalk(sourceStore->getDictionary(), sourceTable, sourceSize, absoluteSampleSize, rand, sample);
                                    }
                                    else {
                                        shell.getOutput() << " and sampling-method \"uniform\"";
                                        sampleUniformly(sourceTable, absoluteSampleSize, rand, sample);
                                    }
                                    shell.getOutput() << " (" << (::getTimePoint() - start) << "ms)" << std::endl;
                                    start = ::getTimePoint();
                                    shell.getOutput() << "    Importing sample into the current store ";
                                    std::vector<ResourceID> tupleBuffer {0, 0, 0};
                                    std::vector<ArgumentIndex> argumentIndexes {0, 1, 2};
                                    TupleTable& destinationTable = shell.getDataStore().getTupleTable("internal$rdf");
                                    Dictionary& destinationDictionary = shell.getDataStore().getDictionary();
                                    std::unordered_map<ResourceID, ResourceID> resourcesMap;
                                    ArgumentIndexSet emptyIndexSet;
                                    std::unique_ptr<TupleIterator> allTuplesIterator = sourceTable.createTupleIterator(tupleBuffer, argumentIndexes, emptyIndexSet, emptyIndexSet, SAMPLING_TUPLE_STATUS_MASK, SAMPING_TUPLE_STATUS_EXPECTED_VALUE);
                                    for(size_t multiplicity = allTuplesIterator->open(); multiplicity > 0; multiplicity = allTuplesIterator->advance()) {
                                        if (complement == !sample[allTuplesIterator->getCurrentTupleIndex()]) {
                                            for (size_t index = 0; index < 3; index++) {
                                                auto resourceMapIterator = resourcesMap.find(tupleBuffer[index]);
                                                if (resourceMapIterator == resourcesMap.end()) {
                                                    ResourceValue resourceValue;
                                                    sourceStore->getDictionary().getResource(tupleBuffer[index], resourceValue);
                                                    resourceMapIterator = resourcesMap.insert(std::make_pair(tupleBuffer[index], destinationDictionary.resolveResource(resourceValue))).first;
                                                }
                                                tupleBuffer[index] = resourceMapIterator->second;
                                            }
                                            ::addTuple(destinationTable, shell.getDataStore().getEqualityManager(), tupleBuffer, argumentIndexes);
                                        }
                                    }
                                    shell.printLine("(", (::getTimePoint() - start), "ms)");
                                    shell.printLine("Sampling Finished");
                                    shell.printLine("");
                                }
                                else
                                    shell.printLine("The number of imported tuples ", sourceSize, " is less than the requested sample size ", absoluteSampleSize, ".");
                            }
                            else
                                shell.printLine("Input couldn't be initialized");
                        }
                        else
                            shell.printLine("No input was specified");
                    }
                    else
                        shell.getOutput() << "No seed was specified" << std::endl;
                }
                else
                    shell.printLine("No size for the sample was specified");
            }
            catch (const RDFStoreException& e) {
                shell.printError(e, "An error occurred while sampling data.");
            }
        }
    }
    
};

static Sample s_sample;
