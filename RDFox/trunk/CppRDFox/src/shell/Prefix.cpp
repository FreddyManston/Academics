// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "ShellCommand.h"

class Prefix : public ShellCommand {

public:

    Prefix() : ShellCommand("prefix") {
    }

    virtual std::string getOneLineHelp() const {
        return "declares a prefix to the shell.";
    }

    virtual void printHelpPage(std::ostream& output) const {
        output
            << "prefix <prefixName> <prefixIRI>" << std::endl
            << "    Declares prefixName for the prefixIRI." << std::endl;
    }

    virtual void execute(Shell& shell, Shell::ArgumentsTokenizer& arguments) const {
        if (arguments.is_PNAME_NS()) {
            const std::string prefixName = arguments.getToken();
            arguments.nextToken();
            if (arguments.isSymbol() || arguments.isQuotedString() || arguments.isQuotedIRI()) {
                const std::string prefixIRI = arguments.getToken();
                arguments.nextToken();
                if (shell.getPrefixes().declarePrefix(prefixName, prefixIRI))
                    shell.printLine("Prefix name '", prefixName, "' declared for <", prefixIRI, ">.");
                else
                    shell.printLine("Prefix name '", prefixName, "' is not of the required form.");
            }
            else
                shell.printLine("The prefixIRI is missing.");
        }
        else
            shell.printLine("The prefixName is missing.");
    }

};

static Prefix s_prefix;
