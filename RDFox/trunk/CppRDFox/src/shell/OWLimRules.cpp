// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "../Common.h"
#include "../RDFStoreException.h"
#include "../storage/TupleTable.h"
#include "../dictionary/Dictionary.h"
#include "../formats/InputOutput.h"
#include "../formats/InputSource.h"
#include "../util/InputImporter.h"
#include "../util/File.h"
#include "../util/Vocabulary.h"
#include "../tasks/Tasks.h"
#include "ShellCommand.h"

class OWLimRules: public ShellCommand {

public:

    class QueryAtom {

    public:

        const Literal& m_literal;
        const std::unordered_set<Variable> m_variables;
        const size_t m_sizeEstimate;

        QueryAtom(const Literal& literal, const size_t sizeEstimate) :
                m_literal(literal), m_variables(literal->getFreeVariables()), m_sizeEstimate(sizeEstimate)
        {
        }

        size_t getRelativeEstmiate(const std::unordered_set<Variable>& boundVariables) {
            size_t unboundVariablesCount = 0;
            for (std::unordered_set<Variable>::const_iterator iter = m_variables.begin(); iter != m_variables.end(); iter++)
                if (boundVariables.find(*iter) == boundVariables.end())
                    unboundVariablesCount++;
            return static_cast<size_t>(std::pow(m_sizeEstimate, (1.0 * unboundVariablesCount) / m_variables.size()));
        }
    };

    OWLimRules() :
            ShellCommand("owlim-rules") {
    }

    virtual std::string getOneLineHelp() const {
        return "converts a datalog program into OWLim rules format.";
    }

    virtual void printHelpPage(std::ostream& output) const {
        output
            << "owlim-rules <datalog file name> <output file name>" << std::endl
            << "    Converts a datalog program into OWLim format based on the statistics of the currently loaded store." << std::endl;
    }

    always_inline std::ostream& transformRule(const size_t ruleIndex, const Rule rule, TupleTable& tupleTable, Dictionary& dictionary, const Prefixes& prefixes, const Term rdfTypeResourceByID, const Term& rdfTypeResourceByName, std::ostream& stream) const {
        std::vector<Literal> plan;
        std::vector<size_t> weights;
        getPlan(rule->getBody(), tupleTable, dictionary, plan, weights);
        stream << "\n//";
        for (size_t index = 0; index < weights.size(); index++)
            stream << weights[index] << " ";
        stream << "\n";
        return printOWLimRule(ruleIndex, rule->getHead(0), plan, prefixes, rdfTypeResourceByID, rdfTypeResourceByName, stream);
    }

    always_inline void getPlan(const std::vector<Literal>& literals, TupleTable& tupleTable, Dictionary& dictionary, std::vector<Literal>& plan, std::vector<size_t>& weights) const {
        plan.clear();
        unique_ptr_vector<QueryAtom> queryAtoms;
        for (size_t atomIndex = 0; atomIndex < literals.size(); atomIndex++) {
            std::unique_ptr<QueryAtom> queryAtom(new QueryAtom(literals[atomIndex], getCountEstimate(literals[atomIndex], tupleTable, dictionary)));
            queryAtoms.push_back(std::move(queryAtom));
        }
        std::vector<QueryAtom*> unprocessedAtoms;
        for (unique_ptr_vector<QueryAtom>::iterator iterator = queryAtoms.begin(); iterator != queryAtoms.end(); ++iterator)
            unprocessedAtoms.push_back((*iterator).get());
        std::unordered_set<Variable> boundVariables;
        bool isFirst = true;
        while (unprocessedAtoms.size() > 1) {
            size_t smallestSize = static_cast<size_t>(-1);
            int bestAtomIndex = -1;
            for (size_t atomIndex = 0; atomIndex < unprocessedAtoms.size(); atomIndex++) {
                size_t currentAtomSize = 0;
                if (isFirst && unprocessedAtoms.size() > 2)
                    for (size_t atomIndex2 = 0; atomIndex2 < unprocessedAtoms.size(); atomIndex2++) {
                        size_t newSize = unprocessedAtoms[atomIndex]->getRelativeEstmiate(unprocessedAtoms[atomIndex2]->m_variables);
                        size_t maxSize = (newSize > unprocessedAtoms[atomIndex2]->m_sizeEstimate ? newSize : unprocessedAtoms[atomIndex2]->m_sizeEstimate);
                        if (newSize * unprocessedAtoms[atomIndex2]->m_sizeEstimate > 0 && newSize * unprocessedAtoms[atomIndex2]->m_sizeEstimate < maxSize)
                            newSize = static_cast<size_t>(-1);
                        else
                            newSize = newSize * unprocessedAtoms[atomIndex2]->m_sizeEstimate;
                        if (newSize + currentAtomSize < newSize)
                            currentAtomSize = static_cast<size_t>(-1);
                        else
                            currentAtomSize += newSize;
                    }
                else
                    currentAtomSize = unprocessedAtoms[atomIndex]->getRelativeEstmiate(boundVariables);
                if (currentAtomSize < smallestSize) {
                    smallestSize = currentAtomSize;
                    bestAtomIndex = static_cast<int>(atomIndex);
                }
            }
            plan.push_back(unprocessedAtoms[bestAtomIndex]->m_literal);
            weights.push_back(smallestSize);
            const std::unordered_set<Variable>& atomVariables = unprocessedAtoms[bestAtomIndex]->m_variables;
            boundVariables.insert(atomVariables.begin(), atomVariables.end());
            unprocessedAtoms.erase(unprocessedAtoms.begin() + bestAtomIndex);
            isFirst = false;
        }
        if (unprocessedAtoms.size() == 1) {
            plan.push_back(unprocessedAtoms[0]->m_literal);
            weights.push_back(unprocessedAtoms[0]->getRelativeEstmiate(boundVariables));
        }
    }

    always_inline size_t getCountEstimate(const Literal& literal, TupleTable& tupleTable, Dictionary& dictionary) const {
        std::vector<ResourceID> argumentsBuffer;
        std::vector<ArgumentIndex> argumentsIndexes;
        ArgumentIndexSet allInputArguments;
        for (ArgumentIndex i = 0; i < 3; i++) {
            Term term = literal->getArgument(i);
            if (term->getType() == RESOURCE_BY_ID || term->getType() == RESOURCE_BY_NAME) {
                if (term->getType() == RESOURCE_BY_ID)
                    argumentsBuffer.push_back(to_pointer_cast<ResourceByID>(term)->getResourceID());
                else
                    argumentsBuffer.push_back(dictionary.resolveResource(to_pointer_cast<ResourceByName>(term)->getResourceText()));
                allInputArguments.add(i);
            }
            else
                argumentsBuffer.push_back(0);
            argumentsIndexes.push_back(i);
        }
        size_t countEstimate = tupleTable.getCountEstimate(argumentsBuffer, argumentsIndexes, allInputArguments);
        return countEstimate;
    }

    always_inline std::ostream& printOWLimRule(const size_t ruleIndex, const Atom& head, const std::vector<Literal>& body, const Prefixes& prefixes, const Term rdfTypeResourceByID, const Term& rdfTypeResourceByName, std::ostream& stream) const {
        stream << "Id: Rule" << ruleIndex << std::endl;
        for (size_t bodyLiteralIndex = 0; bodyLiteralIndex < body.size(); bodyLiteralIndex++)
            printOWLimLiteral(body[bodyLiteralIndex], prefixes, rdfTypeResourceByID, rdfTypeResourceByName, stream) << std::endl;
        stream << "--------------------------------------------------" << std::endl;
        return printOWLimLiteral(head, prefixes, rdfTypeResourceByID, rdfTypeResourceByName, stream) << std::endl;
    }

    always_inline std::ostream& printOWLimLiteral(const Literal& literal, const Prefixes& prefixes, const Term rdfTypeResourceByID, const Term& rdfTypeResourceByName, std::ostream& stream) const {
        for (size_t index = 0; index < 3; index++)
            if (literal->getArgument(index)->getType() == VARIABLE)
                stream << literal->getArgument(index)->toString(prefixes).substr(1) << " ";
            else {
                std::string termString = literal->getArgument(index)->toString(prefixes);
                if (termString[0] != '<')
                    stream << "<" << termString << "> ";
                else if ((termString.find(":")) == std::string::npos)
                    stream << "<owl:" << std::string(termString.begin() + 1, termString.end() - 1) << "> ";
                else
                    stream << termString;
            }
        return stream;
    }

    always_inline std::ostream& printDatalogRule(const size_t ruleIndex, const Atom& head, const std::vector<Atom>& body, const Prefixes& prefixes, const Term rdfTypeResourceByID, const Term& rdfTypeResourceByName, std::ostream& stream) const {
        printDatalogLiteral(head, prefixes, rdfTypeResourceByID, rdfTypeResourceByName, stream) << ":-";
        for (size_t bodyLiteralIndex = 0; bodyLiteralIndex < body.size(); bodyLiteralIndex++) {
            if (bodyLiteralIndex > 0)
                stream << ",";
            printDatalogLiteral(body[bodyLiteralIndex], prefixes, rdfTypeResourceByID, rdfTypeResourceByName, stream);
        }
        return stream << ".";
    }

    always_inline std::ostream& printDatalogLiteral(const Atom& atom, const Prefixes& prefixes, const Term rdfTypeResourceByID, const Term& rdfTypeResourceByName, std::ostream& stream) const {
        if (atom->getArgument(1) == rdfTypeResourceByID || atom->getArgument(1) == rdfTypeResourceByName)
            return stream << atom->getArgument(2)->toString(prefixes) << "(" << atom->getArgument(0)->toString(prefixes) << ")";
        else
            return stream << atom->getArgument(1)->toString(prefixes) << "(" << atom->getArgument(0)->toString(prefixes) << "," << atom->getArgument(2)->toString(prefixes) << ")";
    }

    virtual void execute(Shell& shell, Shell::ArgumentsTokenizer& arguments) const {
        if (isDataStoreActive(shell)) {
            if (arguments.isSymbol() || arguments.isQuotedString() || arguments.isQuotedIRI()) {
                std::string inputFileName = arguments.getToken();
                arguments.nextToken();
                shell.expandRelativeFileName(inputFileName, "dir.dlog");
                TupleTable& tupleTable = shell.getDataStore().getTupleTable("internal$rdf");
                File file;
                try {
                    file.open(inputFileName, File::OPEN_EXISTING_FILE, true, false, true);
                }
                catch (const RDFStoreException& e) {
                    shell.printError(e, "Cannot open the specified file.");
                    return;
                }
                std::unique_ptr<InputSource> inputSource = ::createInputSource(file);
                LogicFactory factory(::newLogicFactory());
                Prefixes prefixes;
                DatalogProgram datalogProgram;
                std::vector<Atom> facts;
                DatalogProgramInputImporter datalogProgramInputImporter(factory, datalogProgram, facts, &shell.getOutput());
                try {
                    std::string formatName;
                    ::load(*inputSource, prefixes, factory, datalogProgramInputImporter, formatName);
                }
                catch (const RDFStoreException& e) {
                    shell.printError(e, "Error while loading file '", inputFileName, "'.");
                    return;
                }
                shell.printLine("Writing results to '", inputFileName, ".pie'.");
                std::ofstream output;
                output.open((inputFileName + ".pie").c_str());
                Term rdfTypeResourceByID = factory->getResourceByID(shell.getDataStore().getDictionary().resolveResource(::RDF_TYPE, D_IRI_REFERENCE));
                Term rdfTypeResourceByName = factory->getIRIReference(::RDF_TYPE);
                printPrefixes(prefixes, output);
                for (size_t ruleIndex = 0; ruleIndex < datalogProgram.size(); ruleIndex++)
                    transformRule(ruleIndex, datalogProgram[ruleIndex], tupleTable, shell.getDataStore().getDictionary(), prefixes, rdfTypeResourceByID, rdfTypeResourceByName, output);
                output << std::endl << "}" << std::endl;
                output.close();
            }
            else
                shell.printLine("No datalog program specified.");
        }
    }

    void printPrefixes(Prefixes& prefixes, std::ostream& output) const {
        const std::map<std::string, std::string>& prefixIRIsByPrefixNames = prefixes.getPrefixIRIsByPrefixNames();
        output << "Prefices{" << std::endl;
        std::map<std::string, std::string>::const_iterator iterator;
        for (iterator = prefixIRIsByPrefixNames.begin(); iterator != prefixIRIsByPrefixNames.end(); ++iterator)
            output << (*iterator).first << " " << (*iterator).second << std::endl;
        output << "}" << std::endl << std::endl;
        output << "Axioms" << std::endl << "{" << std::endl << "    <rdf:type> <rdf:type> <rdf:Property>" << std::endl << "}" << std::endl << std::endl << "Rules " << std::endl << "{" << std::endl;
        output << "Id: Dummy" << std::endl << "X <xsd:DUMMY> Y" << std::endl << "--------------------------------------------------" << std::endl << "X <rdf:type> Y" << std::endl << std::endl;
    }

};

static OWLimRules s_OWLimRules;
