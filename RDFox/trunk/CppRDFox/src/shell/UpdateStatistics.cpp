// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "ShellCommand.h"

class UpdateStatistics : public ShellCommand {

public:

    UpdateStatistics() : ShellCommand("updatestats") {
    }

    virtual std::string getOneLineHelp() const {
        return "updates the statistics in the store";
    }

    virtual void printHelpPage(std::ostream& output) const {
        output <<
            "updatestats" << std::endl <<
            "    Updates the statistics used to compile queries and rules. This command should be run after materialisation." << std::endl;
    }

    virtual void execute(Shell& shell, Shell::ArgumentsTokenizer& arguments) const {
        if (isDataStoreActive(shell)) {
            shell.printLine("Started updating statistics.");
            const TimePoint startTime = ::getTimePoint();
            shell.getDataStore().updateStatistics();
            const Duration duration = ::getTimePoint() - startTime;
            shell.printLine("Statistics were updated in ", duration / 1000.0, " s.");
        }
    }
};

static UpdateStatistics s_updateStatistics;
