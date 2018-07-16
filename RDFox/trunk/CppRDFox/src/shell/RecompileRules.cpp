// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "ShellCommand.h"

class RecompileRules : public ShellCommand {

public:

    RecompileRules() : ShellCommand("recompilerules") {
    }

    virtual std::string getOneLineHelp() const {
        return "recompiles the rules in the current data store";
    }

    virtual void printHelpPage(std::ostream& output) const {
        output
            << "recompilerules" << std::endl
            << "    Recompiles the rules in the current data store according to the current data statistics." << std::endl;
    }

    virtual void execute(Shell& shell, Shell::ArgumentsTokenizer& arguments) const {
        if (isDataStoreActive(shell)) {
            shell.printLine("Started recompiling the rules in the current data store.");
            const TimePoint startTime = ::getTimePoint();
            shell.getDataStore().recompileRules();
            const Duration duration = ::getTimePoint() - startTime;
            shell.printLine("The rules in the current data store were recompiled in ", duration / 1000.0, " s.");
        }
    }
};

static RecompileRules s_recompileRules;
