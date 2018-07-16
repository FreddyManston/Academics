// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef TASKS_H_
#define TASKS_H_

#include "../Common.h"

class File;
class InputSource;
class ThreadContext;
class EqualityManager;
class DataStore;
class TupleReceiver;
class Prefixes;
class InputConsumer;

extern void parseFile(File& file, Prefixes& prefixes, InputConsumer& inputConsumer);

extern std::unique_ptr<InputSource> createInputSource(File& file);

extern std::pair<bool, TupleIndex> addTuple(TupleReceiver& tupleReceiver, const EqualityManager& equalityManager, std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes);

extern std::pair<bool, TupleIndex> addTuple(ThreadContext& threadContext, TupleReceiver& tupleReceiver, const EqualityManager& equalityManager, std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes);

extern bool importText(DataStore& dataStore, const char* const textStart, const size_t textLength, const Duration logFrequency, const UpdateType updateType, const bool decomposeRules, const bool renameBlankNodes, Prefixes& prefixes, std::ostream& output, std::ostream& errors, size_t& numberOfUpdates, size_t& numberOfUniqueUpdates);

extern bool importFiles(DataStore& dataStore, const std::vector<std::string>& fileNames, const Duration logFrequency, const bool logProxies, const UpdateType updateType, const bool decomposeRules, const bool renameBlankNodes, Prefixes& prefixes, std::ostream& output, std::ostream& errors, size_t& numberOfUpdates, size_t& numberOfUniqueUpdates);

extern bool importEDB(DataStore& targetDataStore, const DataStore& sourceDataStore, const Duration logFrequency, const UpdateType updateType, const bool decomposeRules, std::ostream& output, std::ostream& errors, size_t& numberOfUpdates, size_t& numberOfUniqueUpdates);

extern void printRulePlan(std::ostream& output, Prefixes& prefixes, const DataStore& dataStore);

#endif /* TASKS_H_ */
