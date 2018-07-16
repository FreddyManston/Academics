// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "../RDFStoreException.h"
#include "../formats/InputOutput.h"
#include "../util/File.h"
#include "../util/BufferedOutputStream.h"
#include "ShellCommand.h"

class Export : public ShellCommand {

public:

    Export() : ShellCommand("export") {
    }

    virtual std::string getOneLineHelp() const {
        return "exports data from the store";
    }

    virtual void printHelpPage(std::ostream& output) const {
        output
            << "export <file name> <format name>" << std::endl
            << "    Exports the data in the store into the specified file in the specified format (default is \"Turtle\")." << std::endl;
    }

    virtual void execute(Shell& shell, Shell::ArgumentsTokenizer& arguments) const {
        if (isDataStoreActive(shell)) {
            if (arguments.isSymbol() || arguments.isQuotedString() || arguments.isQuotedIRI()) {
                std::string outputFileName = arguments.getToken();
                arguments.nextToken();
                std::string formatName("Turtle");
                if (arguments.isSymbol() || arguments.isQuotedString()) {
                    arguments.getToken(formatName);
                    arguments.nextToken();
                }
                const FormatHandler* formatHandler = getFormatHandlerFor(formatName);
                if (formatHandler == nullptr)
                    shell.printLine("Format with name '", formatName, "' is unknown.");
                else {
                    if (formatHandler->storesFacts())
                        shell.expandRelativeFileName(outputFileName, "dir.facts");
                    else
                        shell.expandRelativeFileName(outputFileName, "dir.dlog");
                    shell.printLine("Exporting data into file '", outputFileName, "' in format with name '", formatName, "'.");
                    TimePoint startTimePoint = ::getTimePoint();
                    File outputFile;
                    outputFile.open(outputFileName, File::CREATE_NEW_OR_TRUNCATE_EXISTING_FILE, false, true, true);
                    File::OutputStreamType outputStream(outputFile);
                    BufferedOutputStream bufferedOutputStream(outputStream);
                    Prefixes prefixes(shell.getPrefixes());
                    ::save(shell.getDataStore(), prefixes, bufferedOutputStream, formatName);
                    Duration duration = ::getTimePoint() - startTimePoint;
                    bufferedOutputStream.flush();
                    outputFile.close();
                    shell.printLine("Exporting data took ", duration/1000.0, " s.");
                }
            }
        }
    }

};

static Export s_export;
