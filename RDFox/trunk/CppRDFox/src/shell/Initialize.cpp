// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "../RDFStoreException.h"
#include "../storage/Parameters.h"
#include "ShellCommand.h"

class Initialize : public ShellCommand {

public:

    Initialize() : ShellCommand("init") {
    }

    virtual std::string getOneLineHelp() const {
        return "initializes the data store";
    }

    virtual void printHelpPage(std::ostream& output) const {
        output
            << "init <type> [<triple capacity> [<resource capacity>]] [<parameterKey> <parameterValue>]*" << std::endl
            << "    Initializes the data store to use the specified type. One can also specify the initial capacity for" << std::endl
            << "    triples and resources, as well as store parameters. Equality handling is determined by the" << std::endl
            << "    'equality' parameter, which can be 'off', 'noUNA', or 'UNA'." << std::endl;
    }

    virtual void execute(Shell& shell, Shell::ArgumentsTokenizer& arguments) const {
        std::string dataStoreTypeName;
        if (!arguments.isSymbol() && !arguments.isQuotedString()) {
            shell.printLine("Data store type is missing.");
            return;
        }
        dataStoreTypeName = arguments.getToken();
        ::toLowerCase(dataStoreTypeName);
        arguments.nextToken();
        Parameters dataStoreParameters;
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
        std::string key;
        std::string value;
        while (arguments.isGood()) {
            if (arguments.isSymbol() || arguments.isQuotedString()) {
                arguments.getToken(key);
                arguments.nextToken();
            }
            else {
                shell.printLine("Store parameter key '", arguments.getToken(), "' is invalid.");
                return;
            }
            if (arguments.isSymbol() || arguments.isQuotedString() || arguments.isNumber()) {
                arguments.getToken(value);
                arguments.nextToken();
                dataStoreParameters.setString(key, value);
            }
            else if (arguments.isGood()) {
                shell.printLine("Store parameter value '", arguments.getToken(), "' is invalid.");
                return;
            }
            else {
                shell.printLine("Store parameter value for key '", key, "' is missing.");
                return;
            }
        }
        try {
            shell.setDataStore(::newDataStore(dataStoreTypeName.c_str(), dataStoreParameters));
            shell.getDataStore().initialize(initialTripleCapacity, initialResourceCapacity);
            shell.getPrefixes().clear();
            shell.getPrefixes().declareStandardPrefixes();
            shell.printLine("A data store of type \"", dataStoreTypeName, "\" ", (shell.getDataStore().getEqualityAxiomatizationType() == EQUALITY_AXIOMATIZATION_OFF ? "without optimized equality" : (shell.getDataStore().getEqualityAxiomatizationType() == EQUALITY_AXIOMATIZATION_NO_UNA ? " with euqality but no UNA" : "with equality and UNA")), " was created and initialized.");
        }
        catch (const RDFStoreException& e) {
            shell.printError(e, "There was an error while initializing the data store.");
            shell.setDataStore(std::unique_ptr<DataStore>());
        }
    }

};

static Initialize s_initialize;
