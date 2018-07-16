// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "ShellCommand.h"

class TStamp : public ShellCommand {

public:

    TStamp() : ShellCommand("tstamp") {
    }

    virtual std::string getOneLineHelp() const {
        return "stores the current time stamp into the given variable";
    }

    virtual void printHelpPage(std::ostream& output) const {
        output
            << "tstamp [<variable name>]" << std::endl
            << "    Stores the current time stamp into the variable with the speficied name." << std::endl;
    }

    virtual void execute(Shell& shell, Shell::ArgumentsTokenizer& arguments) const {
        const std::string currentDateTime = ::getCurrentDateTime();
        if (arguments.isEOF())
            shell.printLine(currentDateTime);
        else if (arguments.isSymbol()) {
            std::string variableName = arguments.getToken();
            arguments.nextToken();
            shell.getVariable(variableName).set(currentDateTime);
        }
    }
};

static TStamp s_tstamp;
