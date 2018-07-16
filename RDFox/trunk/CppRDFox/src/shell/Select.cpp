// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "ShellCommand.h"

extern void executeQuery(Shell& shell, Prefixes& prefixes, const char* const queryText, const size_t queryLength);

class Select : public ShellCommand {

public:

    Select() : ShellCommand("select") {
    }

    virtual std::string getOneLineHelp() const {
        return "runs a SPARQL query over the store";
    }

    virtual void printHelpPage(std::ostream& output) const {
        output
            << "select <remaining query text>" << std::endl
            << "    Queries the store with the given query." << std::endl;
    }

    virtual void execute(Shell& shell, Shell::ArgumentsTokenizer& arguments) const {
        if (isDataStoreActive(shell)) {
            const char* remainingTextStart;
            size_t remainingTextLength;
            arguments.getRemainingText(remainingTextStart, remainingTextLength);
            std::string fullText;
            fullText.append("SELECT ");
            fullText.append(remainingTextStart, remainingTextLength);
            executeQuery(shell, shell.getPrefixes(), fullText.c_str(), fullText.length());
        }
    }

};

static Select s_select;
