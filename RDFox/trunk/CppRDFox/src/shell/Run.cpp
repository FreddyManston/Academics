// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "../RDFStoreException.h"
#include "../dictionary/ResourceValueCache.h"
#include "../dictionary/Dictionary.h"
#include "../logic/Logic.h"
#include "../querying/TermArray.h"
#include "../querying/QueryIterator.h"
#include "../query-answer-format/QueryAnswerFormat.h"
#include "../storage/TupleIterator.h"
#include "../storage/Parameters.h"
#include "../util/ThreadContext.h"
#include "../util/IteratorPrinting.h"
#include "../util/File.h"
#include "../util/MemoryMappedFileView.h"
#include "../util/BufferedOutputStream.h"
#include "../formats/turtle/SPARQLParser.h"
#include "ShellCommand.h"

std::unique_ptr<QueryIterator> compileQuery(Shell& shell, Query& query, TermArray& termArray, bool silent) {
    Parameters queryParameters;
    size_t maxKeyLength = 0;
    for (auto iterator = shell.getVariables().begin(); iterator != shell.getVariables().end(); ++iterator)
        if (iterator->first.compare(0, 6, "query.") == 0) {
            std::string key = iterator->first.substr(6);
            if (key.length() > maxKeyLength)
                maxKeyLength = key.length();
            const Shell::Variable& variable = iterator->second;
            switch (variable.m_variableValue.m_valueType) {
            case Shell::BOOLEAN:
                queryParameters.setBoolean(key, variable.m_variableValue.m_boolean);
                break;
            case Shell::STRING:
                queryParameters.setString(key, variable.m_variableValue.m_string);
                break;
            case Shell::INTEGER:
                queryParameters.setNumber(key, variable.m_variableValue.m_integer);
                break;
            }
        }
    if (!silent) {
        Shell::OutputProtector protector(shell);
        for (auto iterator = queryParameters.begin(); iterator != queryParameters.end(); ++iterator) {
            shell.getOutput() << iterator->first;
            for (size_t padding = iterator->first.length(); padding < maxKeyLength; ++padding)
                shell.getOutput() << ' ';
            shell.getOutput() << ": " << iterator->second << std::endl;
        }
    }
    return shell.getDataStore().compileQuery(query, termArray, queryParameters, nullptr);
}

void executeQuery(Shell& shell, Prefixes& prefixes, const char* const queryText, const size_t queryLength) {
    try {
        LogicFactory factory(newLogicFactory());
        Query query;
        SPARQLParser parser(prefixes);
        query = parser.parse(factory, queryText, queryLength);
        std::unique_ptr<File> outputFile;
        std::unique_ptr<OutputStream> outputStream;
        std::string outputFileName;
        shell.selectOutput(outputStream, outputFile, outputFileName);
        const TimePoint compilationStartTimePoint = ::getTimePoint();
        TermArray termArray;
        std::unique_ptr<QueryIterator> queryIterator = compileQuery(shell, query, termArray, false);
        const Duration compilationDuration = ::getTimePoint() - compilationStartTimePoint;
        if (shell.getBooleanVariable("query.explain")) {
            IteratorPrinting iteratorPrinting(prefixes, termArray, shell.getDataStore().getDictionary());
            iteratorPrinting.printIteratorSubtree(*queryIterator, shell.getOutput());
        }
        size_t numberOfReturnedTuples = 0;
        size_t totalNumberAnswers = 0;
        const ResourceValueCache& resourceValueCache = queryIterator->getResourceValueCache();
        const std::vector<ResourceID>& argumentsBuffer = queryIterator->getArgumentsBuffer();
        const std::vector<ArgumentIndex>& argumentIndexes = queryIterator->getArgumentIndexes();
        const TimePoint evaluationStartTimePoint = ::getTimePoint();
        size_t multiplicity = queryIterator->open();
        if (outputStream.get()) {
            BufferedOutputStream bufferedOutputStream(*outputStream);
            std::unique_ptr<QueryAnswerFormat> queryAnswerFormat = ::newQueryAnswerFormat(shell.getStringVariable("query.answer-format"), bufferedOutputStream, false, query->getAnswerTerms(), shell.getPrefixes());
            queryAnswerFormat->printPrologue();
            while (multiplicity != 0) {
                queryAnswerFormat->printResult(resourceValueCache, argumentsBuffer, argumentIndexes, multiplicity);
                ++numberOfReturnedTuples;
                totalNumberAnswers += multiplicity;
                multiplicity = queryIterator->advance();
            }
            queryAnswerFormat->printEpilogue();
        }
        else {
            while (multiplicity != 0) {
                ++numberOfReturnedTuples;
                totalNumberAnswers += multiplicity;
                multiplicity = queryIterator->advance();
            }
        }
        const Duration evaluationDuration = ::getTimePoint() - evaluationStartTimePoint;
        const bool shouldDeleteFile = (numberOfReturnedTuples == 0) && (outputFile.get() != nullptr) && shell.getBooleanVariable("query.delete-output-if-answer-empty");
        if (outputStream.get())
            outputStream.reset(nullptr);
        if (outputFile.get()) {
            outputFile->close();
            outputFile.reset(nullptr);
        }
        if (shouldDeleteFile)
            ::deleteFile(outputFileName.c_str());
        Shell::OutputProtector protector(shell);
        std::streamsize oldPrecision = shell.getOutput().precision(3);
        shell.getOutput()
            << "Number of returned tuples: " << numberOfReturnedTuples << std::endl
            << "Total number of answers:   " << totalNumberAnswers << std::endl
            << "Query compilation time:    " << compilationDuration/1000.0 << " s" << std::endl
            << "Query evaluation time:     " << evaluationDuration/1000.0 << " s" << std::endl;
        shell.getOutput().precision(oldPrecision);
    }
    catch(RDFStoreException& e) {
        shell.printError(e, "The supplied query is invalid.");
        return;
    }
}

class Run : public ShellCommand {

public:

    Run() : ShellCommand("run") {
    }

    virtual std::string getOneLineHelp() const {
        return "queries the store";
    }

    virtual void printHelpPage(std::ostream& output) const {
        output
            << "run (! <query text> | <file name>*)" << std::endl
            << "    Queries the store with the given query or with queries stored in the specified files." << std::endl;
    }

    virtual void execute(Shell& shell, Shell::ArgumentsTokenizer& arguments) const {
        if (isDataStoreActive(shell)) {
            if (arguments.nonSymbolTokenEquals('!')) {
                arguments.nextToken();
                const char* remainingTextStart;
                size_t remainingTextLength;
                arguments.getRemainingText(remainingTextStart, remainingTextLength);
                executeQuery(shell, shell.getPrefixes(), remainingTextStart, remainingTextLength);
            }
            else {
                while (arguments.isSymbol() || arguments.isQuotedString() || arguments.isQuotedIRI()) {
                    std::string inputFileName = arguments.getToken();
                    arguments.nextToken();
                    shell.expandRelativeFileName(inputFileName, "dir.queries");
                    File file;
                    try {
                        file.open(inputFileName, File::OPEN_EXISTING_FILE, true, false, true, false);
                        const size_t fileSize = file.getSize();
                        MemoryMappedFileView fileView;
                        fileView.open(file);
                        fileView.mapView(0, fileSize);
                        std::string queryText(reinterpret_cast<const char*>(fileView.getMappedData()), fileSize);
                        file.close();
                        shell.printLine(queryText);
                        Prefixes prefixes;
                        executeQuery(shell, prefixes, queryText.c_str(), queryText.length());
                    }
                    catch (const RDFStoreException& e) {
                        shell.printError(e, "The query could not be evaluated.");
                    }
                }
            }
        }
    }

};

static Run s_run;
