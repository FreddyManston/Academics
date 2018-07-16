// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "ShellCommand.h"
#include "../formats/InputOutput.h"
#include "../formats/InputSource.h"
#include "../util/File.h"
#include "../util/DatalogProgramDecomposer.h"
#include "../util/Vocabulary.h"
#include "../util/InputImporter.h"
#include "../tasks/Tasks.h"

class Decompose : public ShellCommand {

public:

    Decompose() : ShellCommand("decompose") {
    }

    virtual std::string getOneLineHelp() const {
        return "decomposes a datalog program using tree decompositions";
    }

    virtual void printHelpPage(std::ostream& output) const {
        output
            << "decompose <input file name> <output file name>" << std::endl
            << "    Applies the tree decomposition to the program in the input file and writes the result in the output file." << std::endl;
    }

    virtual void execute(Shell& shell, Shell::ArgumentsTokenizer& arguments) const {
        if (!isDataStoreActive(shell))
            return;
        if (arguments.isSymbol() || arguments.isQuotedString() || arguments.isQuotedIRI()) {
            std::string inputFileName = arguments.getToken();
            arguments.nextToken();
            shell.expandRelativeFileName(inputFileName, "dir.dlog");
            if (arguments.isSymbol() || arguments.isQuotedString() || arguments.isQuotedIRI()) {
                std::string outputFileName = arguments.getToken();
                arguments.nextToken();
                shell.expandRelativeFileName(outputFileName, "dir.dlog");
                File file;
                try {
                    file.open(inputFileName, File::OPEN_EXISTING_FILE, true, false, true);
                }
                catch (const RDFStoreException& e) {
                    shell.printError(e, "Error while opening file '", inputFileName, "'.");
                    return;
                }
                std::unique_ptr<InputSource> inputSource = ::createInputSource(file);
                LogicFactory factory(::newLogicFactory());
                DatalogProgram inputDatalogProgram;
                std::vector<Atom> inputFacts;
                Prefixes prefixes;
                DatalogProgramInputImporter datalogProgramInputImporter(factory, inputDatalogProgram, inputFacts, &shell.getOutput());
                shell.printLine("Loading datalog file ", inputFileName, ".");
                const TimePoint parseStartTime = ::getTimePoint();
                try {
                    std::string formatName;
                    ::load(*inputSource, prefixes, factory, datalogProgramInputImporter, formatName);
                }
                catch (const RDFStoreException& e) {
                    shell.printError(e, "Error while parsing the file.");
                    return;
                }
                const Duration parseDuration = ::getTimePoint() - parseStartTime;
                shell.printLine("The file was parsed in ", parseDuration / 1000.0, " s.");
                std::string rdfoxPrefixIRI;
                if (prefixes.getPrefixIRI("rdfox:", rdfoxPrefixIRI)) {
                    if (rdfoxPrefixIRI != RDFOX_NS)
                        prefixes.createAutomaticPrefix(RDFOX_NS, 256);
                }
                else
                    prefixes.declarePrefix("rdfox:", RDFOX_NS);
                std::ofstream output(outputFileName, std::ofstream::out | std::ofstream::trunc);
                const TimePoint decompositionStartTime = ::getTimePoint();
                try {
                    const std::map<std::string, std::string>& prefixIRIsByPrefixNames = prefixes.getPrefixIRIsByPrefixNames();
                    for (std::map<std::string, std::string>::const_iterator iterator = prefixIRIsByPrefixNames.begin(); iterator != prefixIRIsByPrefixNames.end(); ++iterator)
                        output << "PREFIX " << iterator->first <<  " <" << iterator->second << ">" << std::endl;
                    if (!prefixIRIsByPrefixNames.empty())
                        output << std::endl;
                    bool lastLineIsBlank = true;
                    DatalogProgramDecomposer datalogProgramDecomposer;
                    DatalogProgram outputDatalogProgram;
                    for (DatalogProgram::iterator inputIterator = inputDatalogProgram.begin(); inputIterator != inputDatalogProgram.end(); ++inputIterator) {
                        outputDatalogProgram.clear();
                        datalogProgramDecomposer.decomposeRule(*inputIterator, outputDatalogProgram);
                        if (outputDatalogProgram.size() != 1 || *inputIterator != outputDatalogProgram[0]) {
                            if (!lastLineIsBlank)
                                output << std::endl;
                            output << "#   " << (*inputIterator)->toString(prefixes) << std::endl;
                            for (DatalogProgram::iterator outputIterator = outputDatalogProgram.begin(); outputIterator != outputDatalogProgram.end(); ++outputIterator)
                                output << (*outputIterator)->toString(prefixes) << std::endl;
                            output << std::endl;
                            lastLineIsBlank = true;
                        }
                        else {
                            output << (*inputIterator)->toString(prefixes) << std::endl;
                            lastLineIsBlank = false;
                        }
                    }
                    const Duration decompositionDuration = ::getTimePoint() - decompositionStartTime;
                    shell.printLine("The program was decomposed and written to file '", outputFileName, "' in ", decompositionDuration / 1000.0, "s.");
                }
                catch (const RDFStoreException& e) {
                    shell.printError(e, "Errors were encountered while decomposing the program.");
                }
                output.close();
            }
            else
                shell.printLine("No output file has been specified.");
        }
        else
            shell.printLine("No input file has been specified.");
    }
};

static Decompose s_decompose;
