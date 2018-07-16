// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "../Common.h"
#include "../logic/Logic.h"
#include "../storage/RuleIterator.h"
#include "../util/Vocabulary.h"
#include "ShellCommand.h"

class RuleStatistics: public ShellCommand {

protected:

    const char* const COMPONENT = "Component";
    const char* const BODY_SIZE = "Body size";
    const char* const NONRECURSIVE_RULES = "Nonrecursive rules";
    const char* const RECURSIVE_RULES = "Recursive rules";

    always_inline static size_t getNumberOfDigits(size_t number) {
        if (number == 0)
            return 1;
        else {
            size_t digits = 0;
            while (number != 0) {
                ++digits;
                number /= 10;
            }
            return digits;
        }
    }

public:

    RuleStatistics() : ShellCommand("rulestats") {
    }

    virtual std::string getOneLineHelp() const {
        return "prints statistics about the currently loaded rules";
    }

    virtual void printHelpPage(std::ostream& output) const {
        output
            << "rulestats [print-rules]" << std::endl
            << "    Prints the statistics about the rules sorted by the component level." << std::endl;
    }

    virtual void execute(Shell& shell, Shell::ArgumentsTokenizer& arguments) const {
        if (isDataStoreActive(shell)) {
            bool printRules = false;
            while (!arguments.isEOF()) {
                if (arguments.symbolLowerCaseTokenEquals("print-rules")) {
                    arguments.nextToken();
                    printRules = true;
                }
                else {
                    shell.printLine("Invalid option '", arguments.getToken(), "'.");
                    return;
                }
            }
            shell.printLine("Producing rule statistics; rules will", (printRules ? "" : "not "), "be printed.");
            std::unique_ptr<std::ofstream> resultsOutputFile;
            std::ostream* selectedOutput;
            if (!shell.selectOutput(selectedOutput, resultsOutputFile) || selectedOutput == nullptr)
                return;
            std::unique_ptr<RuleIterator> ruleIterator = shell.getDataStore().createRuleIterator();
            size_t numberOfNonrecursiveRules = 0;
            size_t numberOfRecursiveRules = 0;
            size_t componentLevelWidth = ::strlen(COMPONENT);
            size_t bodySizeWidth = ::strlen(BODY_SIZE);
            std::map<size_t, std::map<size_t, std::pair<std::vector<Rule>, std::vector<Rule> > > > rulesByComponentBodySizeRecursivity;
            for (bool valid = ruleIterator->open(); valid; valid = ruleIterator->advance()) {
                const Rule& rule = ruleIterator->getRule();
                const size_t numberOfBodyLiterals = rule->getNumberOfBodyLiterals();
                const size_t numberOfHeadLiterals = rule->getNumberOfHeadAtoms();
                bodySizeWidth = std::max(bodySizeWidth, getNumberOfDigits(numberOfBodyLiterals));
                for (size_t headAtomIndex = 0; headAtomIndex < numberOfHeadLiterals; ++headAtomIndex) {
                    const size_t componentLevel = ruleIterator->getHeadAtomComponentLevel(headAtomIndex);
                    componentLevelWidth = std::max(componentLevelWidth, getNumberOfDigits(componentLevel));
                    std::pair<std::vector<Rule>, std::vector<Rule> >& rulesByComponentBodySize = rulesByComponentBodySizeRecursivity[componentLevel][numberOfBodyLiterals];
                    if (ruleIterator->isRecursive(headAtomIndex)) {
                        rulesByComponentBodySize.second.push_back(rule);
                        ++numberOfRecursiveRules;
                    }
                    else {
                        rulesByComponentBodySize.first.push_back(rule);
                        ++numberOfNonrecursiveRules;
                    }
                }
            }
            const size_t numberOfNonrecursiveRulesWidth = std::max(::strlen(NONRECURSIVE_RULES), getNumberOfDigits(numberOfNonrecursiveRules));
            const size_t numberOfRecursiveRulesWidth = std::max(::strlen(RECURSIVE_RULES), getNumberOfDigits(numberOfRecursiveRules));
            const std::streamsize currentWidth = selectedOutput->width();
            *selectedOutput
                << "================================ RULES STATISTICS ================================" << std::endl
                << std::setw(static_cast<int>(componentLevelWidth)) << std::right << COMPONENT << std::setw(static_cast<int>(currentWidth)) << std::left << "    "
                << std::setw(static_cast<int>(bodySizeWidth)) << std::right << BODY_SIZE << std::setw(static_cast<int>(currentWidth)) << std::left << "    "
                << std::setw(static_cast<int>(numberOfNonrecursiveRulesWidth)) << std::right << NONRECURSIVE_RULES << std::setw(static_cast<int>(currentWidth)) << "    " << std::left
                << std::setw(static_cast<int>(numberOfRecursiveRulesWidth)) << std::right << RECURSIVE_RULES << std::setw(static_cast<int>(currentWidth)) << std::left << std::endl;
            for (auto componentIterator = rulesByComponentBodySizeRecursivity.begin(); componentIterator != rulesByComponentBodySizeRecursivity.end(); ++componentIterator) {
                for (auto bodySizeIterator = componentIterator->second.begin(); bodySizeIterator != componentIterator->second.end(); ++bodySizeIterator) {
                    *selectedOutput
                        << std::setw(static_cast<int>(componentLevelWidth)) << std::right << componentIterator->first << std::setw(static_cast<int>(currentWidth)) << std::left << "    "
                        << std::setw(static_cast<int>(bodySizeWidth)) << std::right << bodySizeIterator->first << std::setw(static_cast<int>(currentWidth)) << std::left << "    "
                        << std::setw(static_cast<int>(numberOfNonrecursiveRulesWidth)) << std::right << bodySizeIterator->second.first.size() << std::setw(static_cast<int>(currentWidth)) << "    " << std::left
                        << std::setw(static_cast<int>(numberOfRecursiveRulesWidth)) << std::right << bodySizeIterator->second.second.size() << std::setw(static_cast<int>(currentWidth)) << std::left << std::endl;
                }
            }
            *selectedOutput
                << "----------------------------------------------------------------------------------" << std::endl
                << std::setw(static_cast<int>(componentLevelWidth + bodySizeWidth + 4)) << std::left << "Total:" << std::setw(static_cast<int>(currentWidth)) << std::left << "    "
                << std::setw(static_cast<int>(numberOfNonrecursiveRulesWidth)) << std::right << numberOfNonrecursiveRules << std::setw(static_cast<int>(currentWidth)) << "    " << std::left
                << std::setw(static_cast<int>(numberOfRecursiveRulesWidth)) << std::right << numberOfRecursiveRules << std::setw(static_cast<int>(currentWidth)) << std::left << std::endl;
            if (printRules) {
                for (auto componentIterator = rulesByComponentBodySizeRecursivity.begin(); componentIterator != rulesByComponentBodySizeRecursivity.end(); ++componentIterator) {
                    size_t nonrecursiveRulesInComponent = 0;
                    size_t recursiveRulesInComponent = 0;
                    for (auto bodySizeIterator = componentIterator->second.begin(); bodySizeIterator != componentIterator->second.end(); ++bodySizeIterator) {
                        nonrecursiveRulesInComponent += bodySizeIterator->second.first.size();
                        recursiveRulesInComponent += bodySizeIterator->second.second.size();
                    }
                    *selectedOutput
                        << "----------------------------------------------------------------------------------" << std::endl
                        << "-- COMPONENT:          " << componentIterator->first << std::endl
                        << "-- NONRECURSIVE RULES: " << nonrecursiveRulesInComponent << std::endl
                        << "-- RECURSIVE RULES:    " << recursiveRulesInComponent << std::endl;
                    for (auto bodySizeIterator = componentIterator->second.begin(); bodySizeIterator != componentIterator->second.end(); ++bodySizeIterator) {
                        const std::pair<std::vector<Rule>, std::vector<Rule> >& rulesByComponentBodySize = bodySizeIterator->second;
                        if (!rulesByComponentBodySize.first.empty()) {
                            *selectedOutput
                                << "**********************************************************************************" << std::endl
                                << "** BODY SIZE:          " << bodySizeIterator->first << std::endl
                                << "** NONRECURSIVE RULES: " << rulesByComponentBodySize.first.size() << std::endl << std::endl;
                            for (auto ruleIterator = rulesByComponentBodySize.first.begin(); ruleIterator != rulesByComponentBodySize.first.end(); ++ruleIterator)
                                *selectedOutput << (*ruleIterator)->toString(shell.getPrefixes()) << std::endl;
                        }
                        if (!rulesByComponentBodySize.second.empty()) {
                            *selectedOutput
                            << "**********************************************************************************" << std::endl
                            << "** BODY SIZE:          " << bodySizeIterator->first << std::endl
                            << "** RECURSIVE RULES:    " << rulesByComponentBodySize.second.size() << std::endl << std::endl;
                            for (auto ruleIterator = rulesByComponentBodySize.second.begin(); ruleIterator != rulesByComponentBodySize.second.end(); ++ruleIterator)
                                *selectedOutput << (*ruleIterator)->toString(shell.getPrefixes()) << std::endl;
                        }
                    }
                }
            }
            *selectedOutput << "==================================================================================" << std::endl;
            shell.printLine("Rule statistics printed.");
        }
    }
};

static RuleStatistics s_ruleStatistics;
