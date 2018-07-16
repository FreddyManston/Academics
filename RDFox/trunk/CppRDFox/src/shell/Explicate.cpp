// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "ShellCommand.h"

class Explicate : public ShellCommand {

public:

    Explicate() : ShellCommand("explicate") {
    }

    virtual std::string getOneLineHelp() const {
        return "makes all facts explicit and possibly clears rules";
    }

    virtual void printHelpPage(std::ostream& output) const {
        output <<
                "explicate [clr]" << std::endl <<
                "    Makes all facts explicit, and clears the rules if 'clr' is specified." << std::endl;
    }

    virtual void execute(Shell& shell, Shell::ArgumentsTokenizer& arguments) const {
        if (isDataStoreActive(shell)) {
            if (arguments.symbolLowerCaseTokenEquals("clr")) {
                arguments.nextToken();
                shell.getDataStore().clearRulesAndMakeFactsExplicit();
                shell.printLine("All rules were cleared and all facts were made explicit.");
            }
            else {
                shell.getDataStore().makeFactsExplicit();
                shell.printLine("All facts were made explicit.");
            }
        }
    }
};

static Explicate s_explicate;
