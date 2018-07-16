// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "../util/Vocabulary.h"
#include "../util/File.h"
#include "Shell.h"
#include "ShellCommand.h"

// Shell::ArgumentsTokenizer

bool Shell::ArgumentsTokenizer::getNextIRI(const Prefixes& prefixes, std::string& iri) {
    if (isQuotedIRI()) {
        getToken(iri);
        nextToken();
        return true;
    }
    else if (is_PNAME_LN()) {
        if (prefixes.decodeAbbreviatedIRI(getToken(), iri) == Prefixes::DECODE_SUCCESSFUL) {
            getToken(iri);
            nextToken();
            return true;
        }
    }
    return false;
}

bool Shell::ArgumentsTokenizer::getNextResource(const Prefixes& prefixes, ResourceText& resourceText) {
    if (getNextIRI(prefixes, resourceText.m_lexicalForm)) {
        resourceText.m_resourceType = IRI_REFERENCE;
        resourceText.m_datatypeIRI.clear();
        return true;
    }
    else if (isQuotedString()) {
        getToken(resourceText.m_lexicalForm);
        resourceText.m_resourceType = LITERAL;
        nextToken();
        if (isLanguageTag()) {
            appendToken(resourceText.m_lexicalForm);
            resourceText.m_datatypeIRI = RDF_PLAIN_LITERAL;
            nextToken();
            return true;
        }
        else if (nonSymbolTokenEquals("^^")) {
            nextToken();
            return getNextIRI(prefixes, resourceText.m_datatypeIRI);
        }
        else {
            resourceText.m_datatypeIRI = XSD_STRING;
            return true;
        }
    }
    else if (isNumber()) {
        getToken(resourceText.m_lexicalForm);
        resourceText.m_resourceType = LITERAL;
        if (resourceText.m_lexicalForm.find_first_of('e') != std::string::npos || resourceText.m_lexicalForm.find_first_of('E') != std::string::npos)
            resourceText.m_datatypeIRI = XSD_DOUBLE;
        else if (resourceText.m_lexicalForm.find_first_of('.') == std::string::npos)
            resourceText.m_datatypeIRI = XSD_INTEGER;
        else
            resourceText.m_datatypeIRI = XSD_DOUBLE; // This should be xsd:decimal, but we don't support that yet, so for the moment we parse it as a double.
        nextToken();
        return true;
    }
    else if (isBlankNode()) {
        getToken(resourceText.m_lexicalForm, 2);
        resourceText.m_resourceType = ::BLANK_NODE;
        resourceText.m_datatypeIRI.clear();
        nextToken();
        return true;
    }
    else
        return false;
}

// Shell

Shell::CommandMap& Shell::getCommandMap() {
    static CommandMap s_commandMap;
    return s_commandMap;
}

Shell::Shell(std::istream& input, std::ostream& output) :
    m_input(input), m_output(output), m_prefixes(), m_variables(), m_dataStores(), m_shellObjectsNames(), m_shellObjects()
{
    m_prefixes.declareStandardPrefixes();
    getVariable("run").initialize(true, "The shell is running while this variable is true.");
    getVariable("active").initialize("default", "The name of the currently active store.");
    getVariable("output").initialize("null", "Determines how command results (including queries) are printed: \"null\" (nothing is printed), \"out\", (to stdout), or a file name.");
    getVariable("rename-blank-nodes").initialize(false, "Determines whether blank nodes will be renamed while importing files.");
    getVariable("reason.mat-monitor").initialize("off", "Determines whether and how materialisation is monitored: \"off\" (no monitoring), \"stats\" (gather statistics), and \"trace\" (print materialisation trace).");
    getVariable("reason.by-levels").initialize(true, "If true, the rule set will be split into strongly connected components, and the latter will be processed by levels.");
    getVariable("reason.use-DRed").initialize(false, "If true, incremental reasoning will be done using the DRed algorithm.");
    getVariable("decompose-rules").initialize(false, "If true, rules will be tree-decomposed when added to / removed from the store.");
    getVariable("query.answer-format").initialize("Turtle", "Determines the answer format.");
    getVariable("query.explain").initialize(false, "If true, the query plan is printed after compilation.");
    getVariable("query.delete-output-if-answer-empty").initialize(false, "If true, then the output file is deleted when the query answer is empty.");
    getVariable("query.domain").initialize("IDB", "Determines the domain of the matched tuples: \"EDB\" queries the explicitly stated tuples; \"IDB\" queries all tuples; \"IDBrep\" queries the non-merged tuples; \"IDBrepNoEDB\" queries the non-merged tuples that are not EDBs.");
    getVariable("query.reorder-conjunctions").initialize(true, "Determines whether the query compiler should try to reorder conjunctions according to the data statistics.");
    getVariable("query.bushy").initialize(false, "Determines whether the query compiler should use bushy joins.");
    getVariable("query.root-has-answers").initialize(true, "Determines whether the root of a bushy plan should contain all answer variables.");
    getVariable("query.distinct").initialize(false, "Determines whether queries should be treated as if DISTINCT were specified.");
    getVariable("query.cardinality").initialize(true, "If true, then queries return the correct cardinality.");
    getVariable("dir.root").initialize("", "Determines the root directory of the current data set.");
    getVariable("dir.scripts").initialize("", "Determines the directory for resolving relative file names of script files.");
    getVariable("dir.stores").initialize("", "Determines the directory for resolving relative file names of binary store files.");
    getVariable("dir.dlog").initialize("", "Determines the directory for resolving relative file names of datalog programs.");
    getVariable("dir.facts").initialize("", "Determines the directory for resolving relative file names of RDF files.");
    getVariable("dir.output").initialize("", "Determines the directory for resolving relative file names of output files (as specified by the \"output\" variable).");
    getVariable("dir.queries").initialize("", "Determines the directory for resolving relative file names of query files.");
    getVariable("version").initialize(getRDFoxVersion(), "Contains the current version of RDFox (most likely this is the SVN revision number).");
    getVariable("log-frequency").initialize(static_cast<int64_t>(0), "Determines the time in seconds during which various logs are produced.");
    getVariable("log-proxies").initialize(false, "Determines whether the state of the proxies is logged during various logging operations.");
    getVariable("node-server-init-timeout").initialize(static_cast<int64_t>(10 * 60 * 1000), "Determines the time within which a node server must be started.");
    getVariable("node-server-stats").initialize(false, "Determines whether node servers should gather statistics about their operation.");
    getVariable("node-server-buffer-size").initialize(static_cast<int64_t>(1024 * 1024 * 1024), "Determines the size of each buffer used by the node server.");
    getVariable("node-server-message-batch-size").initialize(static_cast<int64_t>(100), "Determines the number of messages that the node server will extract in one batch for processing.");
    getVariable("sampling-method").initialize("random-walk", "Determines the sampling method: \"random-walk\" or \"uniform\"");
    setRootDirectory(".");
}

Shell::~Shell() {
    for (auto nameIterator = m_shellObjectsNames.rbegin(); nameIterator != m_shellObjectsNames.rend(); ++nameIterator)
        m_shellObjects.erase(m_shellObjects.find(*nameIterator));
}

void Shell::registerCommand(const std::string& commandName, const ShellCommand& command) {
    getCommandMap()[commandName] = &command;
}

const ShellCommand* Shell::getCommand(const std::string& commandName) {
    CommandMap& commandMap = getCommandMap();
    CommandMap::iterator iterator = commandMap.find(commandName);
    if (iterator == commandMap.end())
        return 0;
    else
        return iterator->second;
}

void Shell::setRootDirectory(const std::string& newRoot) {
    std::string newRootTerminated(newRoot);
    if (newRootTerminated.length() < DIRECTORY_SEPARATOR.length() || newRootTerminated.compare(newRootTerminated.length() - DIRECTORY_SEPARATOR.length(), std::string::npos, DIRECTORY_SEPARATOR) != 0)
        newRootTerminated += DIRECTORY_SEPARATOR;
    getVariable("dir.root").set(newRootTerminated);
    std::string fileName;
    fileName = "scripts" + DIRECTORY_SEPARATOR;
    expandRelativeFileName(fileName, "dir.root");
    getVariable("dir.scripts").set(fileName);
    fileName = "stores" + DIRECTORY_SEPARATOR;
    expandRelativeFileName(fileName, "dir.root");
    getVariable("dir.stores").set(fileName);
    fileName = "dlog" + DIRECTORY_SEPARATOR;
    expandRelativeFileName(fileName, "dir.root");
    getVariable("dir.dlog").set(fileName);
    fileName = "facts" + DIRECTORY_SEPARATOR;
    expandRelativeFileName(fileName, "dir.root");
    getVariable("dir.facts").set(fileName);
    fileName = "output" + DIRECTORY_SEPARATOR;
    expandRelativeFileName(fileName, "dir.root");
    getVariable("dir.output").set(fileName);
    fileName = "queries" + DIRECTORY_SEPARATOR;
    expandRelativeFileName(fileName, "dir.root");
    getVariable("dir.queries").set(fileName);
}

void Shell::expandRelativeFileName(std::string& fileName, const std::string& fileNameType) {
    if (!isAbsoluteFileName(fileName))
        fileName.insert(0, getStringVariable(fileNameType));
}

std::string Shell::expandRelativeFileNameEx(const std::string& fileName, const std::string& fileNameType) {
    std::string expandedFileName(fileName);
    expandRelativeFileName(expandedFileName, fileNameType);
    return expandedFileName;
}

std::string Shell::getScriptFileName(const std::string& rawScriptFileName) {
    std::string scriptFileName = expandRelativeFileNameEx(rawScriptFileName, "dir.scripts");
    if (!::fileExists(scriptFileName.c_str())) {
        scriptFileName += ".rdfox";
        if (!::fileExists(scriptFileName.c_str())) {
            scriptFileName = expandRelativeFileNameEx(rawScriptFileName, "dir.root");
            if (!::fileExists(scriptFileName.c_str())) {
                scriptFileName += ".rdfox";
                if (!::fileExists(scriptFileName.c_str()))
                    scriptFileName.clear();
            }
        }
    }
    return scriptFileName;
}

bool Shell::selectOutput(std::ostream*& selectedOutput, std::unique_ptr<std::ofstream>& resultsOutputFile) {
    std::string output = getStringVariable("output");
    if (output == "null") {
        selectedOutput = nullptr;
        printLine("Output will not be printed.");
    }
    else if (output == "out") {
        selectedOutput = &getOutput();
        printLine("Output will be printed.");
    }
    else {
        expandRelativeFileName(output, "dir.output");
        resultsOutputFile.reset(new std::ofstream(output.c_str(), std::ofstream::out | std::ofstream::trunc));
        if (!resultsOutputFile->good()) {
            printLine("Cannot open output file \"", output, "\" for writing.");
            return false;
        }
        selectedOutput = resultsOutputFile.get();
        printLine("Output will be printed to file \"", output, "\".");
    }
    return true;
}

void Shell::selectOutput(std::unique_ptr<OutputStream>& outputStream, std::unique_ptr<File>& outputFile, std::string& outputFileName) {
    outputFileName = getStringVariable("output");
    if (outputFileName == "null")
        printLine("Output will not be printed.");
    else if (outputFileName == "out") {
        outputStream.reset(new FileDescriptorOutputStream(true));
        printLine("Output will be printed.");
    }
    else {
        expandRelativeFileName(outputFileName, "dir.output");
        outputFile.reset(new File());
        outputFile->open(outputFileName, File::CREATE_NEW_OR_TRUNCATE_EXISTING_FILE, false, true, true, false);
        outputStream.reset(new FileDescriptorOutputStream(*outputFile));
        printLine("Output will be printed to file \"", outputFileName, "\".");
    }
}

void Shell::run() {
    run(m_input, true);
}

void Shell::run(std::istream& input, bool showPrompt) {
    std::string command;
    std::string commandPart;
    while (input.good() && getBooleanVariable("run")) {
        if (showPrompt) {
            OutputProtector protector(*this);
            m_output << "> ";
            m_output.flush();
        }
        std::getline(input, commandPart);
        if (commandPart.size() > 0) {
            command.append(commandPart);
            enum State { SEEN_NON_WS, SEEN_ONLY_WS, SEEN_LINE_END };
            State state = SEEN_ONLY_WS;
            for (size_t position = command.length() - 1; state == SEEN_ONLY_WS && position != static_cast<size_t>(-1); --position) {
                const char c = command[position];
                if (c == '\\') {
                    command[position] = '\n';
                    state = SEEN_LINE_END;
                }
                else if (c != ' ' && c != '\n' && c != '\r' && c != '\t')
                    state = SEEN_NON_WS;
            }
            if (state == SEEN_NON_WS) {
                executeCommand(command);
                command.clear();
            }
        }
    }
}

void Shell::executeCommand(const std::string& commandString) {
    std::string commandStringExpanded(commandString);
    size_t scanStart = 0;
    size_t variableOpenPosition;
    while ((variableOpenPosition = commandStringExpanded.find("$(", scanStart)) != std::string::npos) {
        const size_t variableClosePosition = commandStringExpanded.find(')', variableOpenPosition + 2);
        if (variableClosePosition == std::string::npos)
            break;
        const std::string variableName(commandStringExpanded, variableOpenPosition + 2, variableClosePosition - variableOpenPosition - 2);
        std::map<std::string, Variable>::iterator variable = m_variables.find(variableName);
        if (variable == m_variables.end())
            commandStringExpanded.erase(variableOpenPosition, variableClosePosition - variableOpenPosition + 1);
        else {
            switch (variable->second.m_variableValue.m_valueType) {
            case BOOLEAN:
                commandStringExpanded.replace(variableOpenPosition, variableClosePosition - variableOpenPosition + 1, variable->second.m_variableValue.m_boolean ? "true" : "false");
                break;
            case STRING:
                commandStringExpanded.replace(variableOpenPosition, variableClosePosition - variableOpenPosition + 1, variable->second.m_variableValue.m_string);
                break;
            case INTEGER:
                {
                    std::ostringstream value;
                    value << variable->second.m_variableValue.m_integer;
                    commandStringExpanded.replace(variableOpenPosition, variableClosePosition - variableOpenPosition + 1, value.str());
                }
                break;
            }
        }
    }
    try {
        ArgumentsTokenizer arguments(commandStringExpanded);
        arguments.nextToken();
        if (arguments.isSymbol()) {
            std::string commandName = arguments.getToken();
            toLowerCase(commandName);
            const ShellCommand* command = getCommand(commandName);
            if (command == nullptr) {
                std::string commandScriptFileName = getScriptFileName(commandName);
                if (::fileExists(commandScriptFileName.c_str()))
                    command = getCommand("exec");
            }
            else
                arguments.nextToken();
            if (command == nullptr)
                printLine("Unknown command '", commandName, "'.");
            else {
                command->execute(*this, arguments);
                if (!arguments.isEOF())
                    printLine("Command '", commandString, "' contains an error at column ", arguments.getTokenStartColumn(), ".");
            }
        }
        else if (!arguments.isEOF())
            printLine("Command '", commandString, "' contains an error at column ", arguments.getTokenStartColumn(), ".");
    }
    catch (const RDFStoreException& e) {
        printError(e, "Error while executing command.");
    }
}
