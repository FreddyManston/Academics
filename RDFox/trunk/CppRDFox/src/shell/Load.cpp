// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "../all.h"
#include "../RDFStoreException.h"
#include "../util/File.h"
#include "ShellCommand.h"

class Load : public ShellCommand {

public:

    Load() : ShellCommand("load") {
    }

    virtual std::string getOneLineHelp() const {
        return "load a data store file";
    }

    virtual void printHelpPage(std::ostream& output) const {
        output
            << "load <file name> [<number of threads>]" << std::endl
            << "    Loads a file either by replicating the format of the formatted file, or by loading the unformatted file into the current data store." << std::endl
            << "    The suggested number of threads will be used in case of an unformatted file and a concurrent store." << std::endl;
    }

    virtual void execute(Shell& shell, Shell::ArgumentsTokenizer& arguments) const {
        if (arguments.isSymbol() || arguments.isQuotedString() || arguments.isQuotedIRI()) {
            std::string inputFileName = arguments.getToken();
            arguments.nextToken();
            shell.expandRelativeFileName(inputFileName, "dir.stores");
            size_t numberOfThreads = 0;
            if (arguments.isGood()) {
                if (arguments.isNumber()) {
                    std::istringstream input(arguments.getToken());
                    input >> numberOfThreads;
                    arguments.nextToken();
                }
                else {
                    shell.printLine("Invalid format for the number of threads.");
                    return;
                }
            }
            File inputFile;
            try {
                inputFile.open(inputFileName, File::OPEN_EXISTING_FILE, true, false, true, false);
            }
            catch (const RDFStoreException& e) {
                shell.printError(e, "Cannot open the specified file.");
                return;
            }
            try {
                File::InputStreamType inputStream(inputFile);
                std::string dataStoreType;
                inputStream.readString(dataStoreType, 4096);
                if (dataStoreType == CURRENT_FORMATTED_STORE_VERSION) {
                    shell.setDataStore(std::unique_ptr<DataStore>());
                    shell.printLine("Deleting the existing data store and loading a new one from file '", inputFileName, "'.");
                    TimePoint startTimePoint = ::getTimePoint();
                    shell.setDataStore(::newDataStoreFromFormattedFile(inputStream, true));
                    Duration duration = ::getTimePoint() - startTimePoint;
                    OutputProtector protector(shell);
                    std::streamsize oldPrecision = shell.getOutput().precision(3);
                    shell.getOutput() << "Loading the formatted file took " << duration/1000.0 << " s." << std::endl;
                    shell.getOutput().precision(oldPrecision);
                }
                else if (dataStoreType == CURRENT_UNFORMATTED_STORE_VERSION) {
                    if (isDataStoreActive(shell)) {
                        shell.printLine("Deleting the contents of the existing data store and loading the contents of the formatted file '", inputFileName, "'.");
                        TimePoint startTimePoint = ::getTimePoint();
                        shell.getDataStore().loadUnformatted(inputStream, true, numberOfThreads);
                        Duration duration = ::getTimePoint() - startTimePoint;
                        OutputProtector protector(shell);
                        std::streamsize oldPrecision = shell.getOutput().precision(3);
                        shell.getOutput() << "Loading the unformatted file took " << duration/1000.0 << " s." << std::endl;
                        shell.getOutput().precision(oldPrecision);
                    }
                }
                else
                    shell.printLine("The supplied file either does not contain a formatted or an unformatted data store, or the data store is not of the correct version.");
            }
            catch (const RDFStoreException& e) {
                shell.printError(e, "The contents of the data store was erased due to errors.");
                if (shell.hasDataStore())
                    shell.getDataStore().initialize();
            }
        }
    }

};

static Load s_load;
