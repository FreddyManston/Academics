// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.


#include "../all.h"
#include "../equality/EqualityManager.h"
#include "../dictionary/Dictionary.h"
#include "../formats/turtle/SPARQLParser.h"
#include "../logic/Logic.h"
#include "../storage/ArgumentIndexSet.h"
#include "../storage/TupleIterator.h"
#include "../storage/TupleTable.h"
#include "../util/ThreadContext.h"
#include "ShellCommand.h"

class Diff : public ShellCommand {

public:

    Diff() : ShellCommand("diff") {
    }

    virtual std::string getOneLineHelp() const {
        return "compares the current store with a store in a file";
    }

    virtual void printHelpPage(std::ostream& output) const {
        output <<
                "diff <store name> [edb]" << std::endl <<
                "    Compares the active data store with the one with the specified name." << std::endl <<
                "    The EDBs are compared if 'edb' is specified, and the IDBs are compared otherwise." << std::endl;
    }

    std::string toString(const Dictionary& dictionary, const Prefixes& prefixes, const ResourceValue& resourceValue) const {
        ResourceText resourceText;
        dictionary.toLexicalForm(resourceValue, resourceText.m_lexicalForm);
        switch (resourceValue.getDatatypeID()) {
        case D_IRI_REFERENCE:
            resourceText.m_resourceType = IRI_REFERENCE;
            break;
        case D_BLANK_NODE:
            resourceText.m_resourceType = BLANK_NODE;
            break;
        default:
            resourceText.m_resourceType = LITERAL;
            resourceText.m_datatypeIRI = Dictionary::getDatatypeIRI(resourceValue.getDatatypeID());
            break;
        }
        return resourceText.toString(prefixes);
    }

    always_inline bool containsNormalized(ThreadContext& threadContext, std::vector<ResourceID>& correctNormalizedResources, ResourceValue& resourceValue, const std::vector<ArgumentIndex>& argumentIndexes, const TupleStatus tupleStatusMask, const TupleStatus tupleStatusExpectedValue, const EqualityManager& firstEqualityManager, const Dictionary& firstDictionary, const std::vector<ResourceID>& firstArgumentsBuffer, const EqualityManager& secondEqualityManager, const Dictionary& secondDictionary, const TupleTable& secondTupleTable, std::vector<ResourceID>& secondArgumentsBuffer) const {
        const ResourceID INVALID_SECOND_NORMALIZED_ID = static_cast<ResourceID>(-1);
        for (size_t index = 0; index < 3; index++) {
            const ResourceID firstResourceID = firstArgumentsBuffer[index];
            ResourceID secondNormalizedID = correctNormalizedResources[firstResourceID];
            if (secondNormalizedID == INVALID_SECOND_NORMALIZED_ID)
                return false;
            else if (secondNormalizedID == INVALID_RESOURCE_ID) {
                if (!firstDictionary.getResource(firstResourceID, resourceValue))
                    throw RDF_STORE_EXCEPTION("Internal error: cannot resolve resource from the first store in the first store's dictionary.");
                const ResourceID secondResourceID = secondDictionary.tryResolveResource(resourceValue);
                if (secondResourceID == INVALID_RESOURCE_ID) {
                    correctNormalizedResources[firstResourceID] = INVALID_SECOND_NORMALIZED_ID;
                    return false;
                }
                secondNormalizedID = secondEqualityManager.normalize(secondResourceID);
                for (ResourceID firstInClassResourceID = firstEqualityManager.getNextEqual(firstResourceID); firstInClassResourceID != INVALID_RESOURCE_ID; firstInClassResourceID = firstEqualityManager.getNextEqual(firstInClassResourceID)) {
                    if (!firstDictionary.getResource(firstInClassResourceID, resourceValue))
                        throw RDF_STORE_EXCEPTION("Internal error: cannot resolve resource from the first store in the first store's dictionary.");
                    const ResourceID secondInClassResourceID = secondDictionary.tryResolveResource(resourceValue);
                    if (secondInClassResourceID == INVALID_RESOURCE_ID || secondEqualityManager.normalize(secondInClassResourceID) != secondNormalizedID) {
                        correctNormalizedResources[firstResourceID] = INVALID_SECOND_NORMALIZED_ID;
                        return false;
                    }
                }
                correctNormalizedResources[firstResourceID] = secondNormalizedID;
            }
            secondArgumentsBuffer[index] = secondNormalizedID;
        }
        return secondTupleTable.containsTuple(threadContext, secondArgumentsBuffer, argumentIndexes, tupleStatusMask, tupleStatusExpectedValue);
    }

    always_inline void checkViaExpansion(ThreadContext& threadContext, std::ostream* const outputStream, size_t& count, ResourceValue resourceValues[3], const std::vector<ArgumentIndex>& argumentIndexes, const TupleStatus tupleStatusMask, const TupleStatus tupleStatusExpectedValue, const EqualityManager& firstEqualityManager, const Dictionary& firstDictionary, std::vector<ResourceID>& firstArgumentsBuffer, const bool firstUsesOptimizedEquality, const EqualityManager& secondEqualityManager, const Dictionary& secondDictionary, const TupleTable& secondTupleTable, std::vector<ResourceID>& secondArgumentsBuffer) const {
        size_t numberOfErrors = 0;
        const ResourceID firstSubjectID = firstArgumentsBuffer[0];
        const ResourceID firstPredicateID = firstArgumentsBuffer[1];
        const ResourceID firstObjectID = firstArgumentsBuffer[2];
        for (firstArgumentsBuffer[0] = firstSubjectID; firstArgumentsBuffer[0] != INVALID_RESOURCE_ID; firstArgumentsBuffer[0] = firstEqualityManager.getNextEqual(firstArgumentsBuffer[0]))
            for (firstArgumentsBuffer[1] = firstPredicateID; firstArgumentsBuffer[1] != INVALID_RESOURCE_ID; firstArgumentsBuffer[1] = firstEqualityManager.getNextEqual(firstArgumentsBuffer[1]))
                for (firstArgumentsBuffer[2] = firstObjectID; firstArgumentsBuffer[2] != INVALID_RESOURCE_ID; firstArgumentsBuffer[2] = firstEqualityManager.getNextEqual(firstArgumentsBuffer[2])) {
                    bool tupleResolved = true;
                    for (size_t index = 0; index < 3; index++) {
                        const ResourceID firstResourceID = firstArgumentsBuffer[index];
                        if (!firstDictionary.getResource(firstResourceID, resourceValues[index]))
                            throw RDF_STORE_EXCEPTION("Internal error: cannot resolve resource from the first store in the first store's dictionary.");
                        const ResourceID secondResourceID = secondDictionary.tryResolveResource(resourceValues[index]);
                        if (secondResourceID == INVALID_RESOURCE_ID) {
                            if (outputStream != 0)
                                *outputStream << "Resource '" << toString(firstDictionary, Prefixes::s_defaultPrefixes, resourceValues[index]) << "' not found in the dictionary of the target store." << std::endl;
                            tupleResolved = false;
                        }
                        else
                            secondArgumentsBuffer[index] = secondEqualityManager.normalize(secondResourceID);
                    }
                    if (!tupleResolved || !secondTupleTable.containsTuple(threadContext, secondArgumentsBuffer, argumentIndexes, tupleStatusMask, tupleStatusExpectedValue)) {
                        ++numberOfErrors;
                        if (outputStream != 0) {
                            for (size_t index = 0; index < 3; index++)
                                *outputStream << toString(firstDictionary, Prefixes::s_defaultPrefixes, resourceValues[index]) << " ";
                            *outputStream << " ." << std::endl;
                        }
                        ++count;
                    }
                }
        if (firstUsesOptimizedEquality && numberOfErrors != 0 && outputStream != 0) {
            const size_t numberOfEqualSubjects = firstEqualityManager.getEquivalenceClassSize(firstSubjectID);
            const size_t numberOfEqualPredicates = firstEqualityManager.getEquivalenceClassSize(firstPredicateID);
            const size_t numberOfEqualObjects = firstEqualityManager.getEquivalenceClassSize(firstObjectID);
            const size_t possibleErrors= numberOfEqualSubjects * numberOfEqualPredicates * numberOfEqualObjects;
            if (numberOfErrors == possibleErrors)
                *outputStream << "# ALL OF " << numberOfEqualSubjects << " x " << numberOfEqualPredicates << " x " <<  numberOfEqualObjects << " = " << possibleErrors << " POSSIBLE TUPLES ARE MISSING"<< std::endl << std::endl;
            else
                *outputStream << "# " << numberOfErrors << " TUPLES ARE MISSING OUT OF " << numberOfEqualSubjects << " x " << numberOfEqualPredicates << " x " <<  numberOfEqualObjects << " = " << possibleErrors << " POSSIBLE TUPLES"<< std::endl << std::endl;
        }
    }

    size_t printInFirstNotInSecond(DataStore& firstStore, DataStore& secondStore, const bool compareEDBs, std::ostream* outputStream) const {
        ThreadContext& threadContext = ThreadContext::getCurrentThreadContext();
        size_t count = 0;
        ResourceValue resourceValues[3];
        const EqualityManager& firstEqualityManager = firstStore.getEqualityManager();
        const Dictionary& firstDictionary = firstStore.getDictionary();
        const TupleTable& firstTupleTable = firstStore.getTupleTable("internal$rdf");
        const EqualityManager& secondEqualityManager = secondStore.getEqualityManager();
        const Dictionary& secondDictionary = secondStore.getDictionary();
        const TupleTable& secondTupleTable = secondStore.getTupleTable("internal$rdf");
        std::vector<ResourceID> correctNormalizedResources(firstDictionary.getMaxResourceID(), INVALID_RESOURCE_ID);
        TupleStatus tupleStatusMask;
        TupleStatus tupleStatusExpectedValue;
        if (compareEDBs) {
            tupleStatusMask = TUPLE_STATUS_COMPLETE | TUPLE_STATUS_EDB;
            tupleStatusExpectedValue = TUPLE_STATUS_COMPLETE | TUPLE_STATUS_EDB;
        }
        else {
            tupleStatusMask = TUPLE_STATUS_COMPLETE | TUPLE_STATUS_IDB | TUPLE_STATUS_IDB_MERGED;
            tupleStatusExpectedValue = TUPLE_STATUS_COMPLETE | TUPLE_STATUS_IDB;
        }
        std::vector<ResourceID> firstArgumentsBuffer(3, INVALID_RESOURCE_ID);
        std::vector<ResourceID> secondArgumentsBuffer(3,INVALID_RESOURCE_ID);
        std::vector<ArgumentIndex> argumentIndexes;
        argumentIndexes.push_back(0);
        argumentIndexes.push_back(1);
        argumentIndexes.push_back(2);
        ArgumentIndexSet noArguments;
        std::unique_ptr<TupleIterator> tupleIterator = firstTupleTable.createTupleIterator(firstArgumentsBuffer, argumentIndexes, noArguments, noArguments, tupleStatusMask, tupleStatusExpectedValue);
        size_t multiplicity = tupleIterator->open();
        while (multiplicity != 0) {
            if (!containsNormalized(threadContext, correctNormalizedResources, resourceValues[0], argumentIndexes, tupleStatusMask, tupleStatusExpectedValue, firstEqualityManager, firstDictionary, firstArgumentsBuffer, secondEqualityManager, secondDictionary, secondTupleTable, secondArgumentsBuffer))
                checkViaExpansion(threadContext, outputStream, count, resourceValues, argumentIndexes, tupleStatusMask, tupleStatusExpectedValue, firstEqualityManager, firstDictionary, firstArgumentsBuffer, firstStore.getEqualityAxiomatizationType() != EQUALITY_AXIOMATIZATION_OFF, secondEqualityManager, secondDictionary, secondTupleTable, secondArgumentsBuffer);
            multiplicity = tupleIterator->advance();
        }
        return count;
    }

    virtual void execute(Shell& shell, Shell::ArgumentsTokenizer& arguments) const {
        if (arguments.isSymbol() || arguments.isQuotedString() || arguments.isQuotedIRI()) {
            std::string otherDataStoreName = arguments.getToken();
            arguments.nextToken();
            bool compareEDBs = false;
            if (arguments.symbolLowerCaseTokenEquals("edb")) {
                compareEDBs = true;
                arguments.nextToken();
            }
            if (!isDataStoreActive(shell)) {
                shell.getOutput() << "No data store is active." << std::endl;
                return;
            }
            const std::string& activeDataStoreName = shell.getStringVariable("active");
            DataStore* otherDataStore = shell.getDataStorePointer(otherDataStoreName);
            if (otherDataStore == 0) {
                shell.printLine("A data store with name '", otherDataStoreName, "' has not been loaded.");
                return;
            }
            std::unique_ptr<std::ofstream> resultsOutputFile;
            std::ostream* selectedOutput;
            if (!shell.selectOutput(selectedOutput, resultsOutputFile))
                return;
            try {
                shell.printLine("Analyzing differences between '", activeDataStoreName, "' and '", otherDataStoreName, "'.");
                if (selectedOutput != 0) {
                    *selectedOutput << "----------------------------------------------------------------------" << std::endl;
                    *selectedOutput << "    IN '" << activeDataStoreName << "', BUT NOT IN '" << otherDataStoreName << "'" << std::endl;
                    *selectedOutput << "----------------------------------------------------------------------" << std::endl;
                }
                const size_t inActiveNotInOther = printInFirstNotInSecond(shell.getDataStore(), *otherDataStore, compareEDBs, selectedOutput);
                if (selectedOutput != 0) {
                    *selectedOutput << "----------------------------------------------------------------------" << std::endl;
                    *selectedOutput << "   IN '" << otherDataStoreName << "', BUT NOT IN '" << activeDataStoreName << "'" << std::endl;
                    *selectedOutput << "----------------------------------------------------------------------" << std::endl;
                }
                const size_t inOtherNotInActive = printInFirstNotInSecond(*otherDataStore, shell.getDataStore(), compareEDBs, selectedOutput);
                if (selectedOutput != 0)
                    *selectedOutput << "----------------------------------------------------------------------" << std::endl;
                if (inActiveNotInOther == 0 && inOtherNotInActive == 0)
                    shell.printLine("No differences between '", activeDataStoreName, "' and '", otherDataStoreName, "' encountered.");
                else {
                    if (inActiveNotInOther > 0)
                        shell.printLine(inActiveNotInOther, " tuples were found in '", activeDataStoreName, "', but not in '", otherDataStoreName, "'.");
                    if (inOtherNotInActive > 0)
                        shell.printLine(inOtherNotInActive, " tuples were found in '", otherDataStoreName, "', but not in '", activeDataStoreName, "'.");
                }
            }
            catch (const RDFStoreException& e) {
                shell.printError(e, "An error occurred while comparing stores.");
            }
        }
    }

};

static Diff s_diff;
