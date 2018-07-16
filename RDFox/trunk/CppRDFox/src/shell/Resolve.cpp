// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "../RDFStoreException.h"
#include "../dictionary/Dictionary.h"
#include "../equality/EqualityManager.h"
#include "../storage/DataStore.h"
#include "ShellCommand.h"

class Resolve : public ShellCommand {

public:

    Resolve() : ShellCommand("resolve") {
    }

    virtual std::string getOneLineHelp() const {
        return "resolves resources";
    }

    virtual void printHelpPage(std::ostream& output) const {
        output
            << "resolve <URI>*" << std::endl
            << "    Looks up the IDs of the given URIs in the dictionary." << std::endl;
    }

    virtual void execute(Shell& shell, Shell::ArgumentsTokenizer& arguments) const {
        if (isDataStoreActive(shell)) {
            OutputProtector protector(shell);
            DataStore& dataStore = shell.getDataStore();
            Prefixes& prefixes = shell.getPrefixes();
            while (!arguments.isEOF()) {
                if (arguments.isSymbol() || arguments.isQuotedIRI()) {
                    std::string uri;
                    if (arguments.isSymbol())
                        uri = prefixes.decodeIRI(arguments.getToken());
                    else
                        uri = arguments.getToken();
                    const ResourceID resourceID = dataStore.getDictionary().tryResolveResource(uri, D_IRI_REFERENCE);
                    if (resourceID != INVALID_RESOURCE_ID) {
                        const ResourceID canonicalResourceID = dataStore.getEqualityManager().normalize(resourceID);
                        shell.getOutput() << "<" << uri << "> = " << resourceID;
                        if (resourceID != canonicalResourceID)
                            shell.getOutput() << " [" << canonicalResourceID << "]";
                        shell.getOutput() << std::endl;
                    }
                    else
                        shell.getOutput() << "URI '" << uri << "' is not known in the dictionary." << std::endl;
                }
                else
                    shell.getOutput() << "Invalid URI '" << arguments.getToken(0) << "'." << std::endl;
                arguments.nextToken();
            }
        }
    }

};

static Resolve s_resolve;
