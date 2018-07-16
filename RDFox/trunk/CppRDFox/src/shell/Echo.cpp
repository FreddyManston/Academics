// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "ShellCommand.h"

class Echo : public ShellCommand {

public:

    Echo() : ShellCommand("echo") {
    }

    virtual std::string getOneLineHelp() const {
        return "prints a message";
    }

    virtual void printHelpPage(std::ostream& output) const {
        output
            << "echo" << std::endl
            << "    Prints a message." << std::endl;
    }

    virtual void execute(Shell& shell, Shell::ArgumentsTokenizer& arguments) const {
        OutputProtector protector(shell);
        bool first = true;
        while (arguments.isGood()) {
            if (first)
                first = false;
            else
                shell.getOutput() << " ";
            shell.getOutput() << arguments.getToken();
            arguments.nextToken();
        }
        shell.getOutput() << std::endl;
    }

};

static Echo s_echo;
