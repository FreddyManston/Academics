// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "../util/ComponentStatistics.h"
#include "ShellCommand.h"

class Statistics : public ShellCommand {

public:

    Statistics() : ShellCommand("stats") {
    }

    virtual std::string getOneLineHelp() const {
        return "prints statistics about the current store";
    }

    virtual void printHelpPage(std::ostream& output) const {
        output
            << "stats" << std::endl
            << "    Prints statistics about the current store." << std::endl;
    }

    virtual void execute(Shell& shell, Shell::ArgumentsTokenizer& arguments) const {
        if (isDataStoreActive(shell)) {
            std::unique_ptr<ComponentStatistics> componentStatistcs = shell.getDataStore().getComponentStatistics();
            componentStatistcs->print(shell.getOutput());
        }
    }

};

static Statistics s_statistics;
