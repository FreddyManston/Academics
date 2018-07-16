// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "../Common.h"
#include "../RDFStoreException.h"
#include "../dictionary/Dictionary.h"
#include "../formats/turtle/SPARQLParser.h"
#include "../logic/Logic.h"
#include "../querying/TermArray.h"
#include "../querying/QueryIterator.h"
#include "../util/File.h"
#include "../util/MemoryMappedFileView.h"
#include "../util/IteratorPrinting.h"
#include "ShellCommand.h"

extern std::unique_ptr<QueryIterator> compileQuery(Shell& shell, Query& query, TermArray& termArray, bool silent);

class Analyze : public ShellCommand {

protected:

    void analyze(Shell& shell, Prefixes& prefixes, const char* const queryText, const size_t queryLength, const char* const queryFileName) const {
        try {
            LogicFactory factory(newLogicFactory());
            Query query;
            SPARQLParser parser(prefixes);
            query = parser.parse(factory, queryText, queryLength);
            std::unique_ptr<std::ofstream> resultsOutputFile;
            std::ostream* selectedOutput;
            if (!shell.selectOutput(selectedOutput, resultsOutputFile))
                return;
            const TimePoint compilationStartTimePoint = ::getTimePoint();
            TermArray termArray;
            std::unique_ptr<QueryIterator> queryIterator = compileQuery(shell, query, termArray, false);
            const Duration compilationDuration = ::getTimePoint() - compilationStartTimePoint;
            size_t numberOfReturnedTuples = 0;
            size_t totalNumberAnswers = 0;
            const TimePoint evaluationStartTimePoint = ::getTimePoint();
            std::vector<size_t> multiplicities;
            size_t multiplicity = queryIterator->open();
            double mean = 0;
            double stdvs = 0;
            size_t tupleIndex = 0;
            while (multiplicity != 0) {
                multiplicities.push_back(multiplicity);
                ++numberOfReturnedTuples;
                totalNumberAnswers += multiplicity;
                mean = ++tupleIndex == 1 ? multiplicity : mean + (multiplicity - mean)/tupleIndex;
                stdvs += (multiplicity - mean) * (multiplicity - mean);
                multiplicity = queryIterator->advance();
            }
            double standardDeviation = sqrt((tupleIndex > 1) ? (stdvs / (tupleIndex - 1)) : 0);
            const Duration evaluationDuration = ::getTimePoint() - evaluationStartTimePoint;
            std::sort(multiplicities.begin(), multiplicities.end());
            std::reverse(multiplicities.begin(), multiplicities.end());
            if (selectedOutput)
                for (size_t i = 0; i < multiplicities.size(); i++)
                    *selectedOutput << multiplicities[i] << std::endl;
            if (resultsOutputFile.get()) {
                resultsOutputFile->close();
                resultsOutputFile.reset(0);
            }
            OutputProtector protector(shell);
            const std::streamsize oldPrecision = shell.getOutput().precision(3);
            shell.getOutput() <<
                "Number of returned tuples: " << numberOfReturnedTuples << std::endl <<
                "Total number of answers:   " << totalNumberAnswers << std::endl <<
                "Query compilation time:    " << compilationDuration/1000.0 << " s" << std::endl <<
                "Query evaluation time:     " << evaluationDuration/1000.0 << " s" << std::endl <<
                "Mean:                      " << mean << std::endl <<
                "Standard Deviation:        " << standardDeviation << std::endl;
            shell.getOutput().precision(oldPrecision);
        }
        catch (const RDFStoreException& e) {
            shell.printError(e);
            return;
        }
    }

public:

    Analyze() : ShellCommand("analyze") {
    }

    virtual std::string getOneLineHelp() const {
        return "computes statistics for the result set of a query";
    }

    virtual void printHelpPage(std::ostream& output) const {
        output
            << "analyze (! <query text> | <file name>*)" << std::endl
            << "    Queries the store with queries stored in specified files and "
            << "    outputs statistics for their respective result sets." << std::endl;
    }

    virtual void execute(Shell& shell, Shell::ArgumentsTokenizer& arguments) const {
        if (isDataStoreActive(shell)) {
            if (arguments.nonSymbolTokenEquals('!')) {
                arguments.nextToken();
                const char* remainingTextStart;
                size_t remainingTextLength;
                arguments.getRemainingText(remainingTextStart, remainingTextLength);
                analyze(shell, shell.getPrefixes(), remainingTextStart, remainingTextLength, "explicit query");
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
                        analyze(shell, prefixes, queryText.c_str(), queryText.length(), inputFileName.c_str());
                    }
                    catch (const RDFStoreException& e) {
                        shell.printError(e);
                    }
                }
            }
        }
    }
};

static Analyze s_analyze;
