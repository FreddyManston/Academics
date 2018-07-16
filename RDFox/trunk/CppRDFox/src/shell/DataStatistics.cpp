// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "../Common.h"
#include "../RDFStoreException.h"
#include "../dictionary/Dictionary.h"
#include "../formats/turtle/SPARQLParser.h"
#include "../logic/Logic.h"
#include "../querying/TermArray.h"
#include "../querying/QueryIterator.h"
#include "ShellCommand.h"

extern void executeQuery(Shell& shell, const char* const queryText, const size_t queryLength, const char* const queryFileName);

extern std::unique_ptr<QueryIterator> compileQuery(Shell& shell, Query& query, TermArray& termArray, bool silent);

class DataStatistics: public ShellCommand {

    struct ResultSetSummary {
        size_t m_size;
        size_t m_distinctSize;
        double m_mean;
        double m_standardDeviation;

        ResultSetSummary(size_t size, size_t distinctSize, double mean, double standardDeviation) : m_size(size), m_distinctSize(distinctSize), m_mean(mean), m_standardDeviation(standardDeviation) {
        }
    };

protected:

    void analyze(Shell& shell, std::ostream& output) const {
        output << "p1\tp2\ttotal\tdistinct\tmean\tstdv\tin_total\tin_distinct\tin_mean\tin_stdv\tout_total\tout_distinct\tout_mean\tout_stdv" << std::endl;
        shell.getOutput() << "Analyzing predicates " << std::endl;
        std::vector<std::string> predicates = predicateStatistics(shell, output);
        shell.getOutput() << "Analyzing predicate pairs of the form P1(x,y) and P2(y,z) " << std::endl;
        predicatePairsStatistics(shell, output, predicates, false, false);
        shell.getOutput() << "Analyzing predicate pairs of the form P1(x,y) and P2(z,y) " << std::endl;
        predicatePairsStatistics(shell, output, predicates, false, true);
        shell.getOutput() << "Analyzing predicate pairs of the form P1(y,x) and P2(y,z) " << std::endl;
        predicatePairsStatistics(shell, output, predicates, true, false);
        shell.getOutput() << "Done." << std::endl;
    }

    std::vector<std::string> predicateStatistics(Shell & shell, std::ostream& output) const {
        std::vector<std::string> predicates = getPredicates(shell);
        LogicFactory factory(newLogicFactory());
        SPARQLParser parser(shell.getPrefixes());
        for (size_t i = 0; i < predicates.size(); i++) {
            std::string whereClause(" where { ");
            whereClause.append("?x ").append(predicates.at(i)).append(" ?z . }");
            analyzePath(whereClause, shell, output, factory, parser, predicates.at(i), false, "", false);
        }
        return predicates;
    }

    void predicatePairsStatistics(Shell& shell, std::ostream& output, std::vector<std::string>& predicates, bool invertP1, bool invertP2) const {
        LogicFactory factory(newLogicFactory());
        SPARQLParser parser(shell.getPrefixes());
        for (size_t i = 0; i < predicates.size(); i++)
            for (size_t j = ((invertP1 && invertP2) || (!invertP1 && !invertP2)) ? 0 : i; j < predicates.size(); j++) {
                std::string whereClause(" where { ");
                whereClause.append(invertP1 ? "?y " : "?x ").append(predicates.at(i)).append(invertP1 ? " ?x . " : " ?y . ");
                whereClause.append(invertP2 ? "?z " : "?y ").append(predicates.at(j)).append(invertP2 ? " ?y . }" : " ?z . }");
                analyzePath(whereClause, shell, output, factory, parser, predicates.at(i), invertP1, predicates.at(j), invertP2);
            }
    }

    void analyzePath(std::string whereClause, Shell & shell, std::ostream& output, LogicFactory & factory, SPARQLParser & parser, std::string predicate1, bool invertP1, std::string predicate2, bool invertP2) const {
        try {
            std::string queryText("select distinct ?x ?z ");
            queryText.append(whereClause);
            ResultSetSummary summary = getResultSetSummary(shell, factory, parser, queryText);
            if (summary.m_size > 10000) {
                queryText.clear();
                queryText.append("select distinct ?x ").append(whereClause);
                ResultSetSummary inDegreeSummary = getResultSetSummary(shell, factory, parser, queryText);
                queryText.clear();
                queryText.append("select distinct ?z ").append(whereClause);
                ResultSetSummary outDegreeSummary = getResultSetSummary(shell, factory, parser, queryText);
                printSummary(output, predicate1, invertP1, predicate2, invertP2, summary, inDegreeSummary, outDegreeSummary);
            }
        }
        catch (const RDFStoreException&) {
            printSummary(output, predicate1, invertP1, predicate2, invertP2);
        }
    }

    void printSummary(std::ostream& output, std::string predicate1, bool invertP1, std::string predicate2, bool invertP2) const {
        output << predicate1 << (invertP1 ? "-" : "") << "\t" << predicate2 << (invertP2 ? "-" : "") << "\tERROR" << std::endl;
    }

    void printSummary(std::ostream& output, std::string predicate1, bool invertP1, std::string predicate2, bool invertP2, ResultSetSummary summary, ResultSetSummary inDegreeSummary, ResultSetSummary outDegreeSummary) const {
        output
            << predicate1 << (invertP1 ? "-" : "") << "\t"
            << predicate2 << (invertP2 ? "-" : "") << "\t"
            << summary.m_size << "\t"
            << summary.m_distinctSize << "\t"
            << summary.m_mean << "\t"
            << summary.m_standardDeviation << "\t"
            << inDegreeSummary.m_size << "\t"
            << inDegreeSummary.m_distinctSize << "\t"
            << inDegreeSummary.m_mean << "\t"
            << inDegreeSummary.m_standardDeviation << "\t"
            << outDegreeSummary.m_size << "\t"
            << outDegreeSummary.m_distinctSize << "\t"
            << outDegreeSummary.m_mean << "\t"
            << outDegreeSummary.m_standardDeviation << std::endl;
    }

    ResultSetSummary getResultSetSummary(TupleIterator& tupleIterator) const {
        size_t numberOfReturnedTuples = 0;
        size_t numberOfTotalTuples = 0;
        double mean = 0;
        double stdvs = 0;
        size_t tupleIndex = 0;
        size_t multiplicity = tupleIterator.open();
        while (multiplicity != 0) {
            ++numberOfReturnedTuples;
            numberOfTotalTuples += multiplicity;
            mean = ++tupleIndex == 1 ? multiplicity : mean + (multiplicity - mean) / tupleIndex;
            stdvs += (multiplicity - mean) * (multiplicity - mean);
            multiplicity = tupleIterator.advance();
        }
        double standardDeviation = sqrt((tupleIndex > 1) ? (stdvs / (tupleIndex - 1)) : 0);
        ResultSetSummary summary(numberOfTotalTuples, numberOfReturnedTuples, mean, standardDeviation);
        return summary;
    }

    ResultSetSummary getResultSetSummary(Shell& shell, LogicFactory& factory, SPARQLParser& parser, const std::string& queryText) const {
        Query query = parser.parse(factory, queryText.c_str(), queryText.length());
        TermArray termArray;
        std::unique_ptr<QueryIterator> queryIterator = compileQuery(shell, query, termArray, true);
        return getResultSetSummary(*queryIterator);
    }

    std::vector<std::string> getPredicates(Shell& shell) const {
        std::vector<std::string> predicates;
        LogicFactory factory(newLogicFactory());
        SPARQLParser parser(shell.getPrefixes());
        std::string queryText("select distinct ?p where { ?x ?p ?z . } ");
        Query query = parser.parse(factory, queryText.c_str(), queryText.length());
        TermArray termArray;
        std::unique_ptr<QueryIterator> queryIterator = compileQuery(shell, query, termArray, true);
        size_t multiplicity = queryIterator->open();
        ResourceText resourceText;
        Dictionary& dictionary = shell.getDataStore().getDictionary();
        Prefixes& prefixes = shell.getPrefixes();
        while (multiplicity != 0) {
            const std::vector<ResourceID>& argumentsBuffer = queryIterator->getArgumentsBuffer();
            const std::vector<ArgumentIndex>& argumentIndexes = queryIterator->getArgumentIndexes();
            dictionary.getResource(argumentsBuffer[argumentIndexes[0]], resourceText);
            std::string predicate = resourceText.toString(prefixes);
            if (multiplicity > 10000 && predicate.substr(0, 4) != "rdf:" && predicate.substr(0, 5) != "rdfs:")
                predicates.push_back(predicate);
            multiplicity = queryIterator->advance();
        }
        return predicates;
    }

public:

    DataStatistics() : ShellCommand("datastats") {
    }

    virtual std::string getOneLineHelp() const {
        return "analyzes currently loaded data";
    }

    virtual void printHelpPage(std::ostream& output) const {
        output
            << "datastats" << std::endl
            << "    Conducts a statistical analysis of the currently loaded data." << std::endl;
    }

    virtual void execute(Shell& shell, Shell::ArgumentsTokenizer& arguments) const {
        if (isDataStoreActive(shell)) {
            std::unique_ptr<std::ofstream> resultsOutputFile;
            std::ostream* output;
            if (shell.selectOutput(output, resultsOutputFile) && output != nullptr)
                analyze(shell, *output);
        }
    }
};

static DataStatistics s_dataStatistics;
