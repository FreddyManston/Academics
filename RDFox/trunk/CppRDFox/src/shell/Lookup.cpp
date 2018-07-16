// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "../RDFStoreException.h"
#include "../dictionary/Dictionary.h"
#include "../storage/DataStore.h"
#include "ShellCommand.h"

class Lookup : public ShellCommand {

public:

    Lookup() : ShellCommand("lookup") {
    }

    virtual std::string getOneLineHelp() const {
        return "looks up resource IDs";
    }

    virtual void printHelpPage(std::ostream& output) const {
        output
            << "lookup <ResourceID>*" << std::endl
            << "    Looks up the given resource IDs in the dictionary." << std::endl;
    }

    virtual void execute(Shell& shell, Shell::ArgumentsTokenizer& arguments) const {
        if (isDataStoreActive(shell)) {
            DataStore& dataStore = shell.getDataStore();
            Prefixes& prefixes = shell.getPrefixes();
            while (!arguments.isEOF()) {
                if (arguments.isNumber()) {
                    ResourceID resourceID = static_cast<ResourceID>(atoi(arguments.getToken(0).c_str()));
                    ResourceText resourceText;
                    if (dataStore.getDictionary().getResource(resourceID, resourceText))
                        shell.printLine(resourceID, " = ", resourceText.toString(prefixes));
                    else
                        shell.printLine("Resource ID ", resourceID, " is not known in the dictionary.");
                }
                else
                    shell.printLine("Invalid resource ID '", arguments.getToken(0), "'.");
                arguments.nextToken();
            }
        }
    }

};

static Lookup s_lookup;
