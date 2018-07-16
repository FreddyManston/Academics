// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "../RDFStoreException.h"
#include "ShellCommand.h"

class Reindex : public ShellCommand {

public:

    Reindex() : ShellCommand("reindex") {
    }

    virtual std::string getOneLineHelp() const {
        return "reindexes all facts, possibly dropping the IDB";
    }

    virtual void printHelpPage(std::ostream& output) const {
        output
            << "reindex [dropIDB] [<triple capacity> [<resource capacity>]]" << std::endl
            << "    Drops the derived facts and aligns the store to the given capacity." << std::endl;
    }

    virtual void execute(Shell& shell, Shell::ArgumentsTokenizer& arguments) const {
        if (isDataStoreActive(shell)) {
            bool dropIDB = false;
            if (arguments.symbolLowerCaseTokenEquals("dropidb")) {
                dropIDB = true;
                arguments.nextToken();
            }
            size_t initialTripleCapacity = 0;
            size_t initialResourceCapacity = 0;
            if (arguments.isNumber()) {
                long long int number = atoll(arguments.getToken(0).c_str());
                arguments.nextToken();
                if (number < 0) {
                    shell.printLine("Triple capacity must be positive.");
                    return;
                }
                initialTripleCapacity = static_cast<size_t>(number);
                if (arguments.isNumber()) {
                    number = atoll(arguments.getToken(0).c_str());
                    arguments.nextToken();
                    if (number < 0) {
                        shell.printLine("Resource capacity must be positive.");
                        return;
                    }
                    initialResourceCapacity = static_cast<size_t>(number);
                }
            }
            try {
                shell.getDataStore().reindex(dropIDB, initialTripleCapacity, initialResourceCapacity);
            }
            catch (const RDFStoreException& e) {
                shell.printError(e, "There was an error while retaining the data in the store store.");
                shell.setDataStore(std::unique_ptr<DataStore>());
            }
        }
    }

};

static Reindex s_reindex;
