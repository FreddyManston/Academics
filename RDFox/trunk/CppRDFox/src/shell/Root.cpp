// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "../all.h"
#include "ShellCommand.h"

class Root : public ShellCommand {

public:

    Root() : ShellCommand("root") {
    }

    virtual std::string getOneLineHelp() const {
        return "sets the root directory";
    }

    virtual void printHelpPage(std::ostream& output) const {
        output
            << "root <directory>" << std::endl
            << "    Sets the root directory." << std::endl;
    }

    virtual void execute(Shell& shell, Shell::ArgumentsTokenizer& arguments) const {
        if (arguments.isSymbol() || arguments.isQuotedString() || arguments.isQuotedIRI()) {
            std::string newRoot = arguments.getToken();
            arguments.nextToken();
            shell.expandRelativeFileName(newRoot, "dir.root");
            if (newRoot[newRoot.length() - 1] != '\\' && newRoot[newRoot.length() - 1] != '/')
                newRoot += DIRECTORY_SEPARATOR;
            shell.setRootDirectory(newRoot);
        }
    }

};

static Root s_root;
