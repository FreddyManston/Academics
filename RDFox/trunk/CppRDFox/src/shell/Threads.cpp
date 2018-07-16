// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "ShellCommand.h"

class Threads : public ShellCommand {

public:

    Threads() : ShellCommand("threads") {
    }

    virtual std::string getOneLineHelp() const {
        return "sets the number of threads on the current data store";
    }

    virtual void printHelpPage(std::ostream& output) const {
        output
            << "threads <number of threads>" << std::endl
            << "    Sets the number of threads that the current data store should use." << std::endl;
    }

    virtual void execute(Shell& shell, Shell::ArgumentsTokenizer& arguments) const {
        if (!isDataStoreActive(shell))
            return;
        size_t numberOfThreads = 1;
        if (!arguments.isNumber()) {
            shell.printLine("Expected an integer for specifying the number of threads.");
            return;
        }
        else {
            numberOfThreads = static_cast<size_t>(atoi(arguments.getToken(0).c_str()));
            arguments.nextToken();
        }
        const TimePoint setThreadsStartTime = ::getTimePoint();
        shell.getDataStore().setNumberOfThreads(numberOfThreads);
        const Duration setThreadsDuration = ::getTimePoint() - setThreadsStartTime;
        shell.printLine("The number of threads was set to ", numberOfThreads, " in ", setThreadsDuration / 1000.0, " s.");
    }
};

static Threads s_threads;
