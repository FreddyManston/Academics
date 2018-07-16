// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "../endpoint/SPARQLEndPoint.h"
#include "ShellCommand.h"

// ShellSPARQLEndPoint

class ShellSPARQLEndPoint : public SPARQLEndPoint {

protected:

    Shell& m_shell;

    virtual DataStore* getDataStore() const;

public:

    ShellSPARQLEndPoint(Shell& shell) : m_shell(shell) {
    }

};

DataStore* ShellSPARQLEndPoint::getDataStore() const {
    return m_shell.getDataStorePointer();
}

// EndPoint

class EndPoint : public ShellCommand {

public:

    EndPoint() : ShellCommand("endpoint") {
    }

    virtual std::string getOneLineHelp() const {
        return "manages a SPARQL end point";
    }

    virtual void printHelpPage(std::ostream& output) const {
        output
            << "endpoint [start <port> | stop ]" << std::endl
            << "    Starts the server (i.e., SPARQL end point) on the specified port, or stops the currently running one." << std::endl;
    }

    virtual void execute(Shell& shell, Shell::ArgumentsTokenizer& arguments) const {
        if (arguments.isSymbol()) {
            std::string command = arguments.getToken();
            arguments.nextToken();
            if (command == "start") {
                if (arguments.isNumber()) {
                    const int portNumber = atoi(arguments.getToken(0).c_str());
                    arguments.nextToken();
                    if (shell.hasShellObject("SPARQLEndPoint"))
                        shell.printLine("A SPARQL end point is already running.");
                    else {
                        std::unique_ptr<ShellSPARQLEndPoint> endPoint(new ShellSPARQLEndPoint(shell));
                        endPoint->set_listening_port(portNumber);
                        try {
                            endPoint->start_async();
                            shell.setShellObject("SPARQLEndPoint", std::move(endPoint));
                            shell.printLine("The SPARQL end point was successfully started at port ", portNumber, ".");
                        }
                        catch (dlib::socket_error& exception) {
                            endPoint->clear();
                            shell.printLine("Error starting the SPARQL end point: ", exception.what());
                        }
                    }
                }
                else
                    shell.printLine("A port number for the end point is missing.");
            }
            else if (command == "stop") {
                if (shell.removeShellObject("SPARQLEndPoint"))
                    shell.printLine("The SPARQL end point was successfully stopped.");
                else
                    shell.printLine("The SPARQL end point is not running.");
            }
            else
                shell.printLine("Invalid command '", command, "'.");
        }
        else
            shell.printLine("A server command is missing.");
    }

};

static EndPoint s_endPoint;
