// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "ShellCommand.h"

class Quit : public ShellCommand {

public:

    Quit() : ShellCommand("quit") {
    }

    virtual std::string getOneLineHelp() const {
        return "terminates shell";
    }

    virtual void printHelpPage(std::ostream& output) const {
        output
            << "quit" << std::endl
            << "    Terminates the currently running shell." << std::endl;
    }

    virtual void execute(Shell& shell, Shell::ArgumentsTokenizer& arguments) const {
        shell.getVariable("run").set(false);
    }

};

static Quit s_quit;
