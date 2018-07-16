// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "../all.h"
#include "../RDFStoreException.h"
#include "../util/File.h"
#include "../storage/TupleTable.h"
#include "ShellCommand.h"

class Print : public ShellCommand {

public:

    Print() : ShellCommand("print") {
    }

    virtual std::string getOneLineHelp() const {
        return "prints the triple table of a data store into a text file";
    }

    virtual void printHelpPage(std::ostream& output) const {
        output
            << "prints <file name> (t | m1 | m2 | m3)*" << std::endl
            << "    Prints the contents of the triple table of the current data store into a text file." << std::endl
            << "    Option 't' requests to print the triples, and options m1, m2, and m3 request printing the respective index manager." << std::endl;
    }

    virtual void execute(Shell& shell, Shell::ArgumentsTokenizer& arguments) const {
        if (isDataStoreActive(shell)) {
            if (arguments.isSymbol() || arguments.isQuotedString() || arguments.isQuotedIRI()) {
                std::string outputFileName = arguments.getToken();
                arguments.nextToken();
                shell.expandRelativeFileName(outputFileName, "dir.stores");
                bool printTriples = false;
                bool printManager1 = false;
                bool printManager2 = false;
                bool printManager3 = false;
                bool hasErrors = false;
                while (!arguments.isEOF()) {
                    if (arguments.symbolLowerCaseTokenEquals("t"))
                        printTriples = true;
                    else if (arguments.symbolLowerCaseTokenEquals("m1"))
                        printManager1 = true;
                    else if (arguments.symbolLowerCaseTokenEquals("m2"))
                        printManager2 = true;
                    else if (arguments.symbolLowerCaseTokenEquals("m3"))
                        printManager3 = true;
                    else {
                        shell.printLine("Invalid option '", arguments.getToken(), "'.");
                        hasErrors = true;
                    }
                    arguments.nextToken();
                }
                if (hasErrors)
                    return;
                shell.printLine("Printing the contents of the triple table of the current data store into a text file '", outputFileName, "'.");
                std::ofstream output(outputFileName);
                if (!output.is_open()) {
                    shell.printLine("Cannot open the specified file.");
                    return;
                }
                TimePoint startTimePoint = ::getTimePoint();
                shell.getDataStore().getTupleTable("internal$rdf").printContents(output, printTriples, printManager1, printManager2, printManager3);
                Duration duration = ::getTimePoint() - startTimePoint;
                OutputProtector protector(shell);
                std::streamsize oldPrecision = shell.getOutput().precision(3);
                shell.getOutput() << "Printing the triple table file took " << duration/1000.0 << " s." << std::endl;
                shell.getOutput().precision(oldPrecision);
            }
        }
    }

};

static Print s_print;
