// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "../all.h"
#include "../dictionary/Dictionary.h"
#include "../equality/EqualityManager.h"
#include "../storage/DataStore.h"
#include "../util/ThreadContext.h"
#include "ShellCommand.h"

class ListEquals : public ShellCommand {

public:

    ListEquals() : ShellCommand("listequals") {
    }

    virtual std::string getOneLineHelp() const {
        return "lists the resources equal to the specified one";
    }

    virtual void printHelpPage(std::ostream& output) const {
        output <<
            "listequals <resource>*" << std::endl <<
            "    List for each resource from the given list all resources euqal to it." << std::endl;
    }

    virtual void execute(Shell& shell, Shell::ArgumentsTokenizer& arguments) const {
        std::unique_ptr<std::ofstream> resultsOutputFile;
        std::ostream* output;
        if (shell.hasDataStore() && shell.selectOutput(output, resultsOutputFile)) {
            ThreadContext& threadContext = ThreadContext::getCurrentThreadContext();
            const Dictionary& dictionary = shell.getDataStore().getDictionary();
            const EqualityManager& equalityManager = shell.getDataStore().getEqualityManager();
            while (!arguments.isEOF()) {
                ResourceText resourceText;
                if (!arguments.getNextResource(shell.getPrefixes(), resourceText)) {
                    shell.printLine("Token '", arguments.getToken(), "' cannot be parsed as part of a resource");
                    arguments.nextToken();
                }
                else if (output != nullptr) {
                    const ResourceID resourceID = dictionary.tryResolveResource(threadContext, resourceText);
                    if (resourceID == INVALID_RESOURCE_ID)
                        *output << "Resource " << resourceText << " cannot be resolved in the dictionary." << std::endl;
                    else {
                        *output << resourceText;
                        ResourceID currentID = equalityManager.normalize(resourceID);
                        if (resourceID != currentID) {
                            *output << " -> ";
                            if (dictionary.getResource(currentID, resourceText))
                                *output << resourceText;
                            else
                                *output << "Resource ID " << currentID << " cannot be found in the dictionary.";
                        }
                        *output << std::endl;
                        for (currentID = equalityManager.getNextEqual(currentID); currentID != INVALID_RESOURCE_ID; currentID = equalityManager.getNextEqual(currentID)) {
                            if (dictionary.getResource(currentID, resourceText))
                                *output << "    " << resourceText << std::endl;
                            else
                                *output << "Resource ID " << currentID << " cannot be found in the dictionary." << std::endl;
                        }
                    }
                }
            }
        }
    }

};

static ListEquals s_listEquals;
