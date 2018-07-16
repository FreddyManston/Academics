// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "../RDFStoreException.h"
#include "../tasks/Tasks.h"
#include "ShellCommand.h"

class Import : public ShellCommand {

public:

    Import() : ShellCommand("import") {
    }

    virtual std::string getOneLineHelp() const {
        return "imports/adds/removes data to/from the store";
    }

    virtual void printHelpPage(std::ostream& output) const {
        output
            << "import [+|-]? (! <text> | * <store name> | <file name>*)" << std::endl
            << "    Imports (if neither '+' nor '-' is specified), adds (if '+' is specified)," << std::endl
            << "    or removes (if '-') the specified files into the store." << std::endl;
    }

    always_inline void printStartMessage(Shell& shell, const UpdateType updateType, const bool decomposeRules) const {
        OutputProtector protector(shell);
        if (updateType == ADD)
            shell.getOutput() << "Importing";
        else if (updateType == SCHEDULE_FOR_ADDITION)
            shell.getOutput() << "Scheduling for addition";
        else if (updateType == SCHEDULE_FOR_DELETION)
            shell.getOutput() << "Scheduling for deletion";
        shell.getOutput() << " data sequentially.";
        if (decomposeRules)
            shell.getOutput() << " Rules will be decomposed.";
        shell.getOutput() << std::endl;
    }

    always_inline void printStartMessage(Shell& shell, const UpdateType updateType, const std::string& fileName, const bool decomposeRules) const {
        OutputProtector protector(shell);
        if (updateType == ADD)
            shell.getOutput() << "Importing";
        else if (updateType == SCHEDULE_FOR_ADDITION)
            shell.getOutput() << "Scheduling for addition";
        else if (updateType == SCHEDULE_FOR_DELETION)
            shell.getOutput() << "Scheduling for deletion";
        shell.getOutput() << " data in file '" << fileName << "' sequentially.";
        if (decomposeRules)
            shell.getOutput() << " Rules will be decomposed.";
        shell.getOutput() << std::endl;
    }

    always_inline void printStartMessage(Shell& shell, const UpdateType updateType, const size_t numberOfFileNames, const size_t numberOfThreads, const bool decomposeRules) const {
        OutputProtector protector(shell);
        if (updateType == ADD)
            shell.getOutput() << "Importing";
        else if (updateType == SCHEDULE_FOR_ADDITION)
            shell.getOutput() << "Scheduling for addition";
        else if (updateType == SCHEDULE_FOR_DELETION)
            shell.getOutput() << "Scheduling for deletion";
        shell.getOutput() << " data in " << numberOfFileNames << " files in parallel using " << numberOfThreads << " threads.";
        if (decomposeRules)
            shell.getOutput() << " Rules will be decomposed.";
        shell.getOutput() << std::endl;
    }

    virtual void execute(Shell& shell, Shell::ArgumentsTokenizer& arguments) const {
        if (isDataStoreActive(shell)) {
            const int64_t logFrequencySeconds = shell.getIntegerVariable("log-frequency");
            if (logFrequencySeconds < 0) {
                shell.printLine("Log frequency cannot be negative.");
                return;
            }
            const bool logProxies = shell.getBooleanVariable("log-proxies");
            UpdateType updateType = ADD;
            if (arguments.nonSymbolTokenEquals('+')) {
                updateType = SCHEDULE_FOR_ADDITION;
                arguments.nextToken();
            }
            else if (arguments.nonSymbolTokenEquals('-')) {
                updateType = SCHEDULE_FOR_DELETION;
                arguments.nextToken();
            }
            const bool decomposeRules = shell.getBooleanVariable("decompose-rules");
            const bool renameBlankNodes = shell.getBooleanVariable("rename-blank-nodes");
            try {
                if (arguments.nonSymbolTokenEquals('!')) {
                    arguments.nextToken();
                    const char* remainingTextStart;
                    size_t remainingTextLength;
                    arguments.getRemainingText(remainingTextStart, remainingTextLength);
                    printStartMessage(shell, updateType, decomposeRules);
                    size_t numberOfUpdates;
                    size_t numberOfUniqueUpdates;
                    const TimePoint startTimePoint = ::getTimePoint();
                    if (::importText(shell.getDataStore(), remainingTextStart, remainingTextLength, static_cast<Duration>(logFrequencySeconds) * 1000, updateType, decomposeRules, renameBlankNodes, shell.getPrefixes(), shell.getOutput(), shell.getOutput(), numberOfUpdates, numberOfUniqueUpdates)) {
                        Duration duration = ::getTimePoint() - startTimePoint;
                        OutputProtector protector(shell);
                        std::streamsize oldPrecision = shell.getOutput().precision(3);
                        shell.getOutput()
                            << "Import operation took " << duration / 1000.0 << " s." << std::endl
                            << numberOfUpdates << " data items were updated, of which " << numberOfUniqueUpdates << " were unique." << std::endl;
                        shell.getOutput().precision(oldPrecision);
                    }
                    else
                        shell.printLine("Errors were encountered during import.");
                }
                else if (arguments.nonSymbolTokenEquals('*')) {
                    arguments.nextToken();
                    if (!arguments.isSymbol() && !arguments.isQuotedIRI())
                        shell.printLine("The name of the store from which to import the data from is missing.");
                    else {
                        const std::string sourceDataStoreName = arguments.getToken();
                        arguments.nextToken();
                        DataStore* sourceDataStore = shell.getDataStorePointer(sourceDataStoreName);
                        if (sourceDataStore == nullptr)
                            shell.printLine("Data store with name '", sourceDataStoreName, "' does not exist.");
                        else {
                            size_t numberOfUpdates;
                            size_t numberOfUniqueUpdates;
                            shell.printLine("Importing EDB facts from data store '", sourceDataStoreName, "'.");
                            const TimePoint startTimePoint = ::getTimePoint();
                            if (::importEDB(shell.getDataStore(), *sourceDataStore, static_cast<Duration>(logFrequencySeconds) * 1000, updateType, decomposeRules,  shell.getOutput(), shell.getOutput(), numberOfUpdates, numberOfUniqueUpdates)) {
                                Duration duration = ::getTimePoint() - startTimePoint;
                                OutputProtector protector(shell);
                                std::streamsize oldPrecision = shell.getOutput().precision(3);
                                shell.getOutput()
                                    << "Import operation took " << duration / 1000.0 << " s." << std::endl
                                    << numberOfUpdates << " EDB facts were updated, of which " << numberOfUniqueUpdates << " were unique." << std::endl;
                                shell.getOutput().precision(oldPrecision);
                            }
                            else
                                shell.printLine("Errors were encountered during import.");
                        }
                    }
                }
                else {
                    std::vector<std::string> fileNames;
                    while (arguments.isSymbol() || arguments.isQuotedString() || arguments.isQuotedIRI()) {
                        std::string inputFileName = arguments.getToken();
                        arguments.nextToken();
                        if (::fileExists(inputFileName.c_str()))
                            fileNames.push_back(inputFileName);
                        else if (isAbsoluteFileName(inputFileName))
                            shell.printLine("File with name '", inputFileName, "' cannot be found.");
                        else {
                            const std::string factsFileName = shell.expandRelativeFileNameEx(inputFileName, "dir.facts");
                            if (::fileExists(factsFileName.c_str()))
                                fileNames.push_back(factsFileName);
                            else {
                                const std::string dlogFileName = shell.expandRelativeFileNameEx(inputFileName, "dir.dlog");
                                if (::fileExists(dlogFileName.c_str()))
                                    fileNames.push_back(dlogFileName);
                                else
                                    shell.printLine("File with name '", inputFileName, "' cannot be found.");
                            }
                        }
                    }
                    if (fileNames.size() == 0) {
                        OutputProtector protector(shell);
                        shell.getOutput() << "There are no files to ";
                        if (updateType == ADD)
                            shell.getOutput() << "import";
                        else if (updateType == SCHEDULE_FOR_ADDITION)
                            shell.getOutput() << "schedule for addition";
                        else if (updateType == SCHEDULE_FOR_DELETION)
                            shell.getOutput() << "schedule for deletion";
                        shell.getOutput() << "." << std::endl;
                    }
                    else if (shell.getDataStore().getNumberOfThreads() == 1 || fileNames.size() == 1) {
                        std::vector<std::string> oneFileName;
                        for (std::vector<std::string>::iterator iterator = fileNames.begin(); iterator != fileNames.end(); ++iterator) {
                            oneFileName.clear();
                            oneFileName.push_back(*iterator);
                            printStartMessage(shell, updateType, *iterator, decomposeRules);
                            size_t numberOfUpdates;
                            size_t numberOfUniqueUpdates;
                            Prefixes prefixes;
                            const TimePoint startTimePoint = ::getTimePoint();
                            if (::importFiles(shell.getDataStore(), oneFileName, static_cast<Duration>(logFrequencySeconds * 1000), logProxies, updateType, decomposeRules, renameBlankNodes, prefixes, shell.getOutput(), shell.getOutput(), numberOfUpdates, numberOfUniqueUpdates)) {
                                Duration duration = ::getTimePoint() - startTimePoint;
                                OutputProtector protector(shell);
                                std::streamsize oldPrecision = shell.getOutput().precision(3);
                                shell.getOutput()
                                    << "Import operation took " << duration / 1000.0 << " s." << std::endl
                                    << numberOfUpdates << " data items were updated, of which " << numberOfUniqueUpdates << " were unique." << std::endl;
                                shell.getOutput().precision(oldPrecision);
                            }
                            else
                                shell.printLine("Errors were encountered during import.");
                        }
                    }
                    else {
                        printStartMessage(shell, updateType, fileNames.size(), shell.getDataStore().getNumberOfThreads(), decomposeRules);
                        size_t numberOfUpdates;
                        size_t numberOfUniqueUpdates;
                        Prefixes prefixes;
                        const TimePoint startTimePoint = ::getTimePoint();
                        if (::importFiles(shell.getDataStore(), fileNames, static_cast<Duration>(logFrequencySeconds * 1000), logProxies, updateType, decomposeRules, renameBlankNodes, prefixes, shell.getOutput(), shell.getOutput(), numberOfUpdates, numberOfUniqueUpdates)) {
                            Duration duration = ::getTimePoint() - startTimePoint;
                            OutputProtector protector(shell);
                            std::streamsize oldPrecision = shell.getOutput().precision(3);
                            shell.getOutput() <<
                                "Import operation took " << duration / 1000.0 << " s." << std::endl <<
                                numberOfUpdates << " data items were updated, of which " << numberOfUniqueUpdates << " were unique." << std::endl;
                            shell.getOutput().precision(oldPrecision);
                        }
                        else
                            shell.printLine("Errors were encountered during import.");
                    }
                }
            }
            catch (const RDFStoreException& e) {
                shell.printError(e, "An error occurred while loading data.");
            }
        }
    }

};

static Import s_import;
