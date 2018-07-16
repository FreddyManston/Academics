// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "../tasks/Tasks.h"
#include "ShellCommand.h"

class RulePlan : public ShellCommand {

public:

    RulePlan() : ShellCommand("ruleplan") {
    }

    virtual std::string getOneLineHelp() const {
        return "prints the current compiled rules";
    }

    virtual void printHelpPage(std::ostream& output) const {
        output
            << "ruleplan" << std::endl
            << "    Prints the current compiled rules." << std::endl;
    }

    virtual void execute(Shell& shell, Shell::ArgumentsTokenizer& arguments) const {
        if (isDataStoreActive(shell)) {
            shell.printLine("Printing the rule plan.");
            std::unique_ptr<std::ofstream> resultsOutputFile;
            std::ostream* selectedOutput;
            if (!shell.selectOutput(selectedOutput, resultsOutputFile) || selectedOutput == nullptr)
                return;
            printRulePlan(shell.getOutput(), shell.getPrefixes(), shell.getDataStore());
            shell.printLine("Finished printing the plans for the compiled rules.");
        }
    }
};

static RulePlan s_rulePlan;
