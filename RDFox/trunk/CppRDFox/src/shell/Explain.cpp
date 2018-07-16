// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "../RDFStoreException.h"
#include "../logic/Logic.h"
#include "../formats/datalog/DatalogParser.h"
#include "../formats/sources/MemorySource.h"
#include "../storage/DataStore.h"
#include "../storage/Explanation.h"
#include "ShellCommand.h"

class Explain : public ShellCommand {

public:

    Explain() : ShellCommand("explain") {
    }

    virtual std::string getOneLineHelp() const {
        return "explains why a fact has been derived";
    }

    virtual void printHelpPage(std::ostream& output) const {
        output
            << "explain [shortest] [<max depth> [<max rule instances>]] <fact>" << std::endl
            << "    Prints the (shortest) explanation for a fact up to the given depth and the number of rule instances." << std::endl;
    }

    virtual void execute(Shell& shell, Shell::ArgumentsTokenizer& arguments) const {
        if (isDataStoreActive(shell)) {
            bool shortest = false;
            if (arguments.symbolLowerCaseTokenEquals("shortest")) {
                arguments.nextToken();
                shortest = true;
            }
            size_t maxDepth = static_cast<size_t>(-1);
            size_t maxRuleInstances = static_cast<size_t>(-1);
            if (arguments.isNumber()) {
                maxDepth = static_cast<size_t>(atoi(arguments.getToken(0).c_str()));
                arguments.nextToken();
                if (arguments.isNumber()) {
                    maxRuleInstances = static_cast<size_t>(atoi(arguments.getToken(0).c_str()));
                    arguments.nextToken();
                }
            }
            LogicFactory logicFactory(::newLogicFactory());
            const char* remainingTextStart;
            size_t remainingTextLength;
            arguments.getRemainingText(remainingTextStart, remainingTextLength);
            MemorySource memorySource(remainingTextStart, remainingTextLength);
            DatalogParser datalogParser(shell.getPrefixes());
            datalogParser.bind(memorySource);
            Atom atom;
            try {
                atom = datalogParser.parseAtom(logicFactory);
            }
            catch (RDFStoreException& e) {
                shell.printError(e, "Malformed atom.");
                return;
            }
            datalogParser.unbind();
            std::unique_ptr<std::ofstream> resultsOutputFile;
            std::ostream* output;
            if (shell.selectOutput(output, resultsOutputFile)) {
                if (output == nullptr)
                    output = &shell.getOutput();
                std::unique_ptr<ExplanationProvider> explanationProvider(shell.getDataStore().createExplanationProvider());
                const ExplanationAtomNode& rootNode = explanationProvider->getNode(atom);
                Shell::OutputProtector outputProtector(shell);
                std::unordered_set<const ExplanationAtomNode*> visitedNodes;
                print(shell.getPrefixes(), *output, shortest, maxDepth, maxRuleInstances, 0, 0, visitedNodes, *explanationProvider, rootNode);
            }
        }
    }

    void print(const Prefixes& prefixes, std::ostream& output, const bool shortest, const size_t maxDepth, const size_t maxRuleInstances, const size_t level, const size_t indent, std::unordered_set<const ExplanationAtomNode*>& visitedNodes, ExplanationProvider& explanationProvider, const ExplanationAtomNode& atomNode) const {
        for (size_t index = 0; index < indent; ++index)
            output << ' ';
        output << atomNode.getAtom()->toString(prefixes);
        switch (atomNode.getType()) {
        case ExplanationAtomNode::FALSE_ATOM:
            output << "  FALSE" << std::endl;
            break;
        case ExplanationAtomNode::EDB_ATOM:
            output << "  EDB" << std::endl;
            break;
        case ExplanationAtomNode::EQUAL_TO_EDB_ATOM:
            output << "  =EDB" << std::endl;
            break;
        case ExplanationAtomNode::IDB_ATOM:
            if (!visitedNodes.insert(&atomNode).second)
                output << "  SEE ABOVE" << std::endl;
            else if (level >= maxDepth)
                output << "  ..." << std::endl;
            else if (shortest) {
                output << "  {" << atomNode.getHeight() << "}" << std::endl;
                const ExplanationRuleInstanceNode* const shortestChild = atomNode.getShortestChild();
                if (shortestChild != nullptr)
                    print(prefixes, output, shortest, maxDepth, maxRuleInstances, level, indent, visitedNodes, explanationProvider, *shortestChild);
            }
            else {
                output << std::endl;
                const std::vector<std::unique_ptr<ExplanationRuleInstanceNode> >& nodeChildren = atomNode.getChildren();
                auto childIterator = nodeChildren.begin();
                size_t printedRuleInstances = 0;
                while (childIterator != nodeChildren.end() && printedRuleInstances < maxRuleInstances) {
                    print(prefixes, output, shortest, maxDepth, maxRuleInstances, level, indent, visitedNodes, explanationProvider, **childIterator);
                    ++childIterator;
                    ++printedRuleInstances;
                }
                if (childIterator != nodeChildren.end()) {
                    for (size_t index = 0; index < indent + 4; ++index)
                        output << ' ';
                    output << "..." << std::endl;
                }
            }
            break;
        }
    }

    void print(const Prefixes& prefixes, std::ostream& output, const bool shortest, const size_t maxDepth, const size_t maxRuleInstances, const size_t level, const size_t indent, std::unordered_set<const ExplanationAtomNode*>& visitedNodes, ExplanationProvider& explanationProvider, const ExplanationRuleInstanceNode& ruleInstanceNode) const {
        for (size_t index = 0; index < indent + 4; ++index)
            output << ' ';
        output << ruleInstanceNode.getRuleInstance()->toString(prefixes) << std::endl;
        const std::vector<const ExplanationAtomNode*>& ruleInstanceChildren = ruleInstanceNode.getChildren();
        for (auto iterator = ruleInstanceChildren.begin(); iterator != ruleInstanceChildren.end(); ++iterator)
            print(prefixes, output, shortest, maxDepth, maxRuleInstances, level + 1, indent + 8, visitedNodes, explanationProvider, **iterator);
    }
};

static Explain s_explain;
