// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "../all.h"
#include "../RDFStoreException.h"
#include "../util/File.h"
#include "ShellCommand.h"

class FormattedSave : public ShellCommand {

public:

    FormattedSave() : ShellCommand("fsave") {
    }

    virtual std::string getOneLineHelp() const {
        return "saves a data store into a formatted file";
    }

    virtual void printHelpPage(std::ostream& output) const {
        output
            << "fsave <file name>" << std::endl
            << "    Saves the contents of the current data store into a formatted file." << std::endl;
    }

    virtual void execute(Shell& shell, Shell::ArgumentsTokenizer& arguments) const {
        if (!shell.hasDataStore())
            shell.printLine("No data store is currently active.");
        else {
            if (arguments.isSymbol() || arguments.isQuotedString() || arguments.isQuotedIRI()) {
                std::string outputFileName = arguments.getToken();
                arguments.nextToken();
                shell.expandRelativeFileName(outputFileName, "dir.stores");
                shell.printLine("Saving the data store into formatted file '", outputFileName, "'.");
                File outputFile;
                try {
                    outputFile.open(outputFileName, File::CREATE_NEW_OR_TRUNCATE_EXISTING_FILE, false, true, false, false);
                }
                catch (const RDFStoreException& e) {
                    shell.printError(e, "Cannot open the specified file.");
                    return;
                }
                try {
                    File::OutputStreamType outputStream(outputFile);
                    TimePoint startTimePoint = ::getTimePoint();
                    shell.getDataStore().saveFormatted(outputStream);
                    Duration duration = ::getTimePoint() - startTimePoint;
                    OutputProtector protector(shell);
                    std::streamsize oldPrecision = shell.getOutput().precision(3);
                    shell.getOutput() << "Saving the formatted file took " << duration/1000.0 << " s." << std::endl;
                    shell.getOutput().precision(oldPrecision);
                }
                catch (const RDFStoreException& e) {
                    shell.printError(e, "Cannot save the data store.");
                }
            }
        }
    }

};

static FormattedSave s_formattedSave;
