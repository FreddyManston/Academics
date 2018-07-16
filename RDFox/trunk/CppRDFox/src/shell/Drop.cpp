// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "ShellCommand.h"

class Drop : public ShellCommand {

public:

    Drop() : ShellCommand("drop") {
    }

    virtual std::string getOneLineHelp() const {
        return "drops the current data store";
    }

    virtual void printHelpPage(std::ostream& output) const {
        output
            << "drop" << std::endl
            << "    Drops (i.e., erases) the current data store from memory." << std::endl;
    }

    virtual void execute(Shell& shell, Shell::ArgumentsTokenizer& arguments) const {
        if (isDataStoreActive(shell)) {
            shell.setDataStore(std::unique_ptr<DataStore>());
            shell.printLine("The current data store has been dropped.");
        }
    }
};

static Drop s_drop;
