// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "../RDFStoreException.h"
#include "../dictionary/Dictionary.h"
#include "../equality/EqualityManager.h"
#include "../storage/ArgumentIndexSet.h"
#include "../storage/DataStore.h"
#include "../storage/TupleTable.h"
#include "ShellCommand.h"

class IsNormalized : public ShellCommand {

public:

    IsNormalized() : ShellCommand("isnorm") {
    }

    virtual std::string getOneLineHelp() const {
        return "checks whether the contents of the data store is normalized w.r.t. equality axioms";
    }

    virtual void printHelpPage(std::ostream& output) const {
        output
            << "isnorm" << std::endl
            << "    Checks whether each fact in the data store is normalized w.r.t. the equality axioms." << std::endl;
    }

    virtual void execute(Shell& shell, Shell::ArgumentsTokenizer& arguments) const {
        if (isDataStoreActive(shell)) {
            OutputProtector protector(shell);
            std::vector<ResourceID> argumentsBuffer(3, INVALID_RESOURCE_ID);
            std::vector<ArgumentIndex> argumentIndexes;
            argumentIndexes.push_back(0);
            argumentIndexes.push_back(1);
            argumentIndexes.push_back(2);
            ArgumentIndexSet allInputArguments;
            std::unique_ptr<TupleIterator> tupleIterator = shell.getDataStore().getTupleTable("internal$rdf").createTupleIterator(argumentsBuffer, argumentIndexes, allInputArguments, allInputArguments);
            size_t multiplicity = tupleIterator->open();
            bool nonNormalFactEncountered = false;
            while (multiplicity != 0) {
                if (!shell.getDataStore().getEqualityManager().isNormal(argumentsBuffer, argumentIndexes)) {
                    if (!nonNormalFactEncountered) {
                        shell.getOutput() << "The store contains facts that are not normal:" << std::endl << "----------------------------------------------------------------" << std::endl;
                        nonNormalFactEncountered = true;
                    }
                    for (std::vector<ArgumentIndex>::iterator iterator = argumentIndexes.begin(); iterator != argumentIndexes.end(); ++iterator) {
                        ResourceText resourceText;
                        shell.getOutput() << "  ";
                        if (shell.getDataStore().getDictionary().getResource(argumentsBuffer[*iterator], resourceText))
                            shell.getOutput() << resourceText.toString(shell.getPrefixes());
                        else
                            shell.getOutput() << "Invalid resource ID '" << argumentsBuffer[*iterator] << "'.";
                    }
                    shell.getOutput() << std::endl;
                }
                multiplicity = tupleIterator->advance();
            }
            if (nonNormalFactEncountered)
                shell.getOutput() << "----------------------------------------------------------------" << std::endl;
            else
                shell.getOutput() << "All facts in the store are normal." << std::endl;
        }
    }

};

static IsNormalized s_isNormalized;
