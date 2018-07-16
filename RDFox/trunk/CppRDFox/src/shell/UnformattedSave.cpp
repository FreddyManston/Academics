// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "../all.h"
#include "../RDFStoreException.h"
#include "../util/File.h"
#include "ShellCommand.h"

class UnformattedSave : public ShellCommand {

public:

    UnformattedSave() : ShellCommand("usave") {
    }

    virtual std::string getOneLineHelp() const {
        return "saves a data store into an unformatted file";
    }

    virtual void printHelpPage(std::ostream& output) const {
        output
            << "usave <file name>" << std::endl
            << "    Saves the contents of the current data store into an unformatted file." << std::endl;
    }

    virtual void execute(Shell& shell, Shell::ArgumentsTokenizer& arguments) const {
        if (isDataStoreActive(shell)) {
            if (arguments.isSymbol() || arguments.isQuotedString() || arguments.isQuotedIRI()) {
                std::string outputFileName = arguments.getToken();
                arguments.nextToken();
                shell.expandRelativeFileName(outputFileName, "dir.stores");
                shell.printLine("Saving the data store into unformatted file '", outputFileName, "'.");
                File outputFile;
                try {
                    outputFile.open(outputFileName, File::CREATE_NEW_OR_TRUNCATE_EXISTING_FILE, false, true, false, false);
                }
                catch (const RDFStoreException& e) {
                    shell.printError(e, "The specified file could not be opened.");
                    return;
                }
                try {
                    File::OutputStreamType outputStream(outputFile);
                    TimePoint startTimePoint = ::getTimePoint();
                    shell.getDataStore().saveUnformatted(outputStream);
                    Duration duration = ::getTimePoint() - startTimePoint;
                    OutputProtector protector(shell);
                    std::streamsize oldPrecision = shell.getOutput().precision(3);
                    shell.getOutput() << "Saving the unformatted file took " << duration/1000.0 << " s." << std::endl;
                    shell.getOutput().precision(oldPrecision);
                }
                catch (const RDFStoreException& e) {
                    shell.printError(e, "The store could not be saved.");
                }
            }
        }
    }

};

static UnformattedSave s_unformattedSave;
