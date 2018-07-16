// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "../RDFStoreException.h"
#include "../node-server/NodeServer.h"
#include "../node-server/NodeServerListener.h"
#include "../node-server/NodeServerQueryListener.h"
#include "../dictionary/Dictionary.h"
#include "../dictionary/ResourceValueCache.h"
#include "../formats/InputOutput.h"
#include "../formats/sources/InputStreamSource.h"
#include "../formats/sources/MemoryMappedFileSource.h"
#include "../formats/sources/MemorySource.h"
#include "../util/File.h"
#include "../util/MemoryMappedFileView.h"
#include "../util/Mutex.h"
#include "../util/Condition.h"
#include "../util/NodeServerStatistics.h"
#include "../util/ThreadContext.h"
#include "ShellCommand.h"

// ShellQueryListener

class ShellQueryListener : public NodeServerQueryListener {

protected:

    Shell& m_shell;
    std::ostream* m_selectedOutput;
    std::unique_ptr<std::ofstream> m_resultsOutputFile;
    Prefixes m_prefixes;
    TimePoint m_queryAnswerStart;
    Mutex m_mutex;
    Condition m_condition;
    bool m_finished;
    aligned_size_t m_numberOfAnswers;

public:

    ShellQueryListener(Shell& shell, std::ostream* selectedOutput, std::unique_ptr<std::ofstream> resultsOutputFile) :
        m_shell(shell),
        m_selectedOutput(selectedOutput),
        m_resultsOutputFile(std::move(resultsOutputFile)),
        m_prefixes(m_shell.getPrefixes()),
        m_queryAnswerStart(::getTimePoint()),
        m_mutex(),
        m_condition(),
        m_finished(false),
        m_numberOfAnswers(0)
    {
    }

    virtual void queryAnswer(const ResourceValueCache& resourceValueCache, const ResourceIDMapper& resourceIDMapper, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const size_t multiplicity) {
        ::atomicAdd(m_numberOfAnswers, multiplicity);
        if (m_selectedOutput) {
            MutexHolder mutexHolder(m_mutex);
            ResourceValue resourceValue;
            std::string literalText;
            for (auto iterator = argumentIndexes.begin();iterator != argumentIndexes.end(); ++iterator) {
                const ResourceID resourceID = argumentsBuffer[*iterator];
                if (isGlobalResourceID(resourceID))
                    *m_selectedOutput << "<non-local-ID> ";
                else {
                    if (!resourceValueCache.getResource(resourceID, resourceValue)) {
                        std::ostringstream message;
                        message << "Cannot resolve resource ID '" << argumentsBuffer[*iterator] << "'.";
                        throw RDF_STORE_EXCEPTION(message.str());
                    }
                    Dictionary::toTurtleLiteral(resourceValue, m_prefixes, literalText);
                    *m_selectedOutput << literalText << ' ';
                }
            }
            if (multiplicity > 1)
                *m_selectedOutput << "* " << multiplicity;
            *m_selectedOutput << ".\n";
        }
    }

    virtual void queryAnswersComplete() {
        const Duration duration = ::getTimePoint() - m_queryAnswerStart;
        m_shell.printLine("Query evaluation took ", duration, " ms and it returned ", m_numberOfAnswers, " answers.");
        MutexHolder mutexHolder(m_mutex);
        m_finished = true;
        m_condition.signalAll();
    }

    virtual void queryInterrupted() {
        m_shell.printLine("Query interrupted.");
        MutexHolder mutexHolder(m_mutex);
        m_finished = true;
        m_condition.signalAll();
    }

    void waitUntilFinished() {
        MutexHolder mutexHolder(m_mutex);
        while (!m_finished)
            m_condition.wait(m_mutex);
    }

    __ALIGNED(ShellQueryListener)

};

// ShellNodeServerListener

class ShellNodeServerListener : public NodeServerListener {

protected:

    Shell& m_shell;
    const NodeID m_nodeID;

public:

    ShellNodeServerListener(Shell& shell, const NodeID nodeID) : m_shell(shell), m_nodeID(nodeID) {
    }

    virtual void nodeServerStarting() {
        m_shell.printLine("Node server '", static_cast<uint32_t>(m_nodeID), "' has been started.");
    }

    virtual void nodeServerReady() {
        m_shell.printLine("Node server '", static_cast<uint32_t>(m_nodeID), "' is ready to answer queries.");
    }

    virtual void nodeServerFinishing() {
        m_shell.printLine("Node server '", static_cast<uint32_t>(m_nodeID), "' has been terminated.");
    }

    virtual void nodeServerError(const std::exception& error) {
        m_shell.printError(error, "Node server '", static_cast<uint32_t>(m_nodeID), "' encountered an error.");
    }

};

// Server

class Server : public ShellCommand {

public:

    Server() : ShellCommand("server") {
    }

    always_inline std::string getNodeServerName(const NodeID nodeID) const {
        std::ostringstream name;
        name << "NodeServer::" << static_cast<uint32_t>(nodeID);
        return name.str();
    }

    always_inline std::string getNodeServerListenerName(const NodeID nodeID) const {
        std::ostringstream name;
        name << "NodeServerListener::" << static_cast<uint32_t>(nodeID);
        return name.str();
    }

    always_inline std::string getNodeServerStatisticsName(const NodeID nodeID) const {
        std::ostringstream name;
        name << "NodeServerStatistics::" << static_cast<uint32_t>(nodeID);
        return name.str();
    }

    always_inline NodeServer* getNodeServer(Shell& shell, const NodeID nodeID) const {
        NodeServer* nodeServer = shell.getShellObject<NodeServer>(getNodeServerName(nodeID));
        if (nodeServer == nullptr)
            shell.printLine("A node server with ID '", static_cast<uint32_t>(nodeID), "' has not been initialized.");
        return nodeServer;
    }

    virtual std::string getOneLineHelp() const {
        return "manages a node server";
    }

    virtual void printHelpPage(std::ostream& output) const {
        output
            << "server <myID> [init [<number of threads> <number of nodes>] | set <ID> <node name> <service name> | occurrences [all] (! <text> | <file>) | start | waitready | stop | run (! <query> | <file>) | stats | resetstats]" << std::endl
            << "    Manages the node server with the given ID." << std::endl;
    }

    virtual void execute(Shell& shell, Shell::ArgumentsTokenizer& arguments) const {
        try {
            if (!arguments.isNumber()) {
                shell.printLine("The server ID is missing.");
                return;
            }
            const NodeID myNodeID = static_cast<NodeID>(atoi(arguments.getToken().c_str()));
            arguments.nextToken();
            const std::string command = arguments.getToken();
            arguments.nextToken();
            if (command == "init") {
                if (isDataStoreActive(shell)) {
                    size_t numberOfThreads = 1;
                    if (!arguments.isNumber()) {
                        shell.printLine("Expected an integer specifying the number of threads.");
                        return;
                    }
                    else {
                        numberOfThreads = static_cast<size_t>(atoi(arguments.getToken(0).c_str()));
                        arguments.nextToken();
                    }
                    uint8_t numberOfNodes = 1;
                    if (!arguments.isNumber()) {
                        shell.printLine("Expected an integer specifying the number of nodes.");
                        return;
                    }
                    else {
                        numberOfNodes = static_cast<uint8_t>(atoi(arguments.getToken(0).c_str()));
                        arguments.nextToken();
                    }
                    if (myNodeID >= numberOfNodes) {
                        shell.printLine("The ID of this node must be smaller than the number of nodes.");
                        return;
                    }
                    std::string nodeServerName(getNodeServerName(myNodeID));
                    if (shell.hasShellObject(nodeServerName)) {
                        shell.printLine("A node server with ID '", static_cast<uint32_t>(myNodeID), "' has already been initialized.");
                        return;
                    }
                    std::string nodeServerStatisticsName(getNodeServerStatisticsName(myNodeID));
                    if (shell.getBooleanVariable("node-server-stats"))
                        shell.setShellObject(nodeServerStatisticsName, std::unique_ptr<NodeServerStatistics>(new NodeServerStatistics()));
                    else
                        shell.removeShellObject(nodeServerStatisticsName);
                    std::string nodeServerListenerName(getNodeServerListenerName(myNodeID));
                    shell.setShellObject(nodeServerListenerName, std::unique_ptr<NodeServerListener>(new ShellNodeServerListener(shell, myNodeID)));
                    const size_t totalSizePerMessageBuffer = static_cast<size_t>(shell.getIntegerVariable("node-server-buffer-size"));
                    const size_t messageBatchSize = static_cast<size_t>(shell.getIntegerVariable("node-server-message-batch-size"));
                    shell.setShellObject(nodeServerName, std::unique_ptr<NodeServer>(new NodeServer(numberOfThreads, numberOfNodes, myNodeID, totalSizePerMessageBuffer, messageBatchSize, shell.getDataStore(), shell.getShellObject<NodeServerListener>(nodeServerListenerName), shell.getShellObject<NodeServerStatistics>(nodeServerStatisticsName))));
                }
            }
            else if (command == "set") {
                NodeServer* nodeServer = getNodeServer(shell, myNodeID);
                if (nodeServer != nullptr) {
                    if (!arguments.isNumber()) {
                        shell.printLine("A node ID is missing.");
                        return;
                    }
                    const NodeID nodeID = atoi(arguments.getToken(0).c_str());
                    arguments.nextToken();
                    if (!arguments.isSymbol() && !arguments.isQuotedString()) {
                        shell.printLine("A node name is missing.");
                        return;
                    }
                    const std::string nodeName = arguments.getToken();
                    arguments.nextToken();
                    if (!arguments.isSymbol() && !arguments.isQuotedString() && !arguments.isNumber()) {
                        shell.printLine("A service name is missing.");
                        return;
                    }
                    const std::string serviceName = arguments.getToken();
                    arguments.nextToken();
                    if (nodeServer->getState() != NodeServer::NOT_RUNNING) {
                        shell.printLine("Node server '", static_cast<uint32_t>(myNodeID), "'cannot be changed because it is not idle.");
                        return;
                    }
                    nodeServer->setNodeInfo(nodeID, nodeName, serviceName);
                }
            }
            else if (command == "occurrences") {
                bool loadOnlyOwnedResources = true;
                if (arguments.lowerCaseTokenEquals(Shell::ArgumentsTokenizer::SYMBOL, "all")) {
                    loadOnlyOwnedResources = false;
                    arguments.nextToken();
                }
                NodeServer* nodeServer = getNodeServer(shell, myNodeID);
                if (nodeServer != nullptr) {
                    std::unique_ptr<InputSource> inputSource;
                    File file;
                    if (arguments.nonSymbolTokenEquals('!')) {
                        arguments.nextToken();
                        const char* remainingTextStart;
                        size_t remainingTextLength;
                        arguments.getRemainingText(remainingTextStart, remainingTextLength);
                        inputSource.reset(new MemorySource(remainingTextStart, remainingTextLength));
                        shell.printLine("Loading occurrences from the command; ", loadOnlyOwnedResources ? "only owned" : "all", " resources will be loaded.");
                    }
                    else {
                        if (!arguments.isSymbol() && !arguments.isQuotedString() && !arguments.isQuotedIRI()) {
                            shell.printLine("The name of the occurrences file is missing.");
                            return;
                        }
                        std::string fileName(arguments.getToken());
                        arguments.nextToken();
                        shell.expandRelativeFileName(fileName, "dir.facts");
                        try {
                            file.open(fileName.c_str(), File::OPEN_EXISTING_FILE, true, false, true);
                        }
                        catch (const RDFStoreException& e) {
                            shell.printError(e, "Cannot open the occurrences file '", fileName, "'.");
                            return;
                        }
                        if (file.isRegularFile())
                            inputSource.reset(new MemoryMappedFileSource(file, 100 * 1024 * 1024));
                        else
                            inputSource.reset(new InputStreamSource<FileDescriptorInputStream>(file, 100 * 1024 * 1024));
                        shell.printLine("Loading occurrences from file '", fileName, "; ", loadOnlyOwnedResources ? "only owned" : "all", " resources will be loaded.");
                    }
                    if (nodeServer->getState() != NodeServer::NOT_RUNNING) {
                        shell.printLine("Node server '", static_cast<uint32_t>(myNodeID), "'cannot be changed because it is not idle.");
                        return;
                    }
                    const TimePoint startTime = ::getTimePoint();
                    nodeServer->loadOccurrencesFromInputSource(shell.getPrefixes(), *inputSource, loadOnlyOwnedResources);
                    const Duration duration = ::getTimePoint() - startTime;
                    shell.printLine("Occurrences have been loaded in ", duration / 1000.0, " s.");
                }
            }
            else if (command == "start") {
                NodeServer* nodeServer = getNodeServer(shell, myNodeID);
                if (nodeServer != nullptr) {
                    const int64_t startupTimeOut = shell.getIntegerVariable("node-server-init-timeout");
                    shell.printLine("Starting node server '", static_cast<uint32_t>(myNodeID), "' with startup timeout of ", startupTimeOut, " ms.");
                    if (!nodeServer->startServer(static_cast<Duration>(startupTimeOut)))
                        shell.printLine("Node server '", static_cast<uint32_t>(myNodeID), "' could not be started.");
                }
            }
            else if (command == "waitready") {
                NodeServer* nodeServer = getNodeServer(shell, myNodeID);
                if (nodeServer != nullptr && nodeServer->getState() != NodeServer::RUNNING) {
                    shell.printLine("Waiting for node server '", static_cast<uint32_t>(myNodeID), "' to become ready.");
                    if (!nodeServer->waitForReady())
                        shell.printLine("Node server '", static_cast<uint32_t>(myNodeID), "' is not ready.");
                }
            }
            else if (command == "stop") {
                NodeServer* nodeServer = getNodeServer(shell, myNodeID);
                if (nodeServer != nullptr) {
                    if (!nodeServer->stopServer())
                        shell.printLine("Node server '", static_cast<uint32_t>(myNodeID), "' cannot be stopped because it is currently not running.");
                    shell.removeShellObject(getNodeServerName(myNodeID));
                }
            }
            else if (command == "run") {
                NodeServer* nodeServer = getNodeServer(shell, myNodeID);
                if (nodeServer != nullptr) {
                    if (nodeServer->getState() != NodeServer::RUNNING) {
                        shell.printLine("Server '", static_cast<uint32_t>(myNodeID), "' is not ready to accept queries.");
                        return;
                    }
                    std::string queryText;
                    if (arguments.nonSymbolTokenEquals('!')) {
                        arguments.nextToken();
                        const char* queryTextStart;
                        size_t queryTextLength;
                        arguments.getRemainingText(queryTextStart, queryTextLength);
                        queryText.append(queryTextStart, queryTextLength);
                    }
                    else if (!arguments.isSymbol() && !arguments.isQuotedString() && !arguments.isQuotedIRI()) {
                        shell.printLine("Invalid query file name.");
                        return;
                    }
                    else {
                        std::string inputFileName = arguments.getToken();
                        arguments.nextToken();
                        shell.expandRelativeFileName(inputFileName, "dir.queries");
                        try {
                            File file;
                            file.open(inputFileName, File::OPEN_EXISTING_FILE, true, false, true, false);
                            const size_t queryTextLength = file.getSize();
                            MemoryMappedFileView fileView;
                            fileView.open(file);
                            fileView.mapView(0, queryTextLength);
                            queryText.append(reinterpret_cast<const char*>(fileView.getMappedData()), queryTextLength);
                        }
                        catch (const RDFStoreException& e) {
                            shell.printError(e, "The query file '", inputFileName, "' cannot be loaded.");
                            return;
                        }
                    }
                    std::ostream* selectedOutput;
                    std::unique_ptr<std::ofstream> resultsOutputFile;
                    if (shell.selectOutput(selectedOutput, resultsOutputFile)) {
                        try {
                            ShellQueryListener shellQueryListener(shell, selectedOutput, std::move(resultsOutputFile));
                            nodeServer->answerQuery(queryText.c_str(), queryText.length(), shellQueryListener);
                            shellQueryListener.waitUntilFinished();
                        }
                        catch (const RDFStoreException& e) {
                            shell.printError(e, "There was an error evaluating the query.");
                        }
                    }
                }
            }
            else if (command == "stats") {
                NodeServerStatistics* nodeServerStatistics = shell.getShellObject<NodeServerStatistics>(getNodeServerStatisticsName(myNodeID));
                if (nodeServerStatistics == nullptr)
                    shell.printLine("Server '", static_cast<uint32_t>(myNodeID), "' is not tracking statistics.");
                else
                    shell.printLine("Statistics for node '", static_cast<uint32_t>(myNodeID), "':\n", nodeServerStatistics->getStatisticsCounters());
            }
            else if (command == "resetstats") {
                NodeServerStatistics* nodeServerStatistics = shell.getShellObject<NodeServerStatistics>(getNodeServerStatisticsName(myNodeID));
                if (nodeServerStatistics == nullptr)
                    shell.printLine("Server '", static_cast<uint32_t>(myNodeID), "' is not tracking statistics.");
                else {
                    nodeServerStatistics->reset();
                    shell.printLine("Statistics for node '", static_cast<uint32_t>(myNodeID), "' have been reset.");
                }
            }
            else
                shell.printLine("Invalid server management command '", command, "'.");
        }
        catch (const RDFStoreException& e) {
            shell.printError(e, "An error during server administration.");
        }
    }

};

static Server s_server;
