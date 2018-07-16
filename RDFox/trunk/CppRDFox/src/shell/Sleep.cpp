// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "../all.h"
#include "ShellCommand.h"

// The '_' in the class name is because Sleep clashes with defines on Windows.

class _Sleep : public ShellCommand {

public:

    _Sleep() : ShellCommand("sleep") {
    }

    virtual std::string getOneLineHelp() const {
        return "sleeps for some time";
    }

    virtual void printHelpPage(std::ostream& output) const {
        output <<
            "sleep <milliseconds>" << std::endl <<
            "    Sleeps for the specifid number of milliseconds." << std::endl;
    }

    virtual void execute(Shell& shell, Shell::ArgumentsTokenizer& arguments) const {
        if (arguments.isNumber()) {
            const Duration sleepDuration = static_cast<Duration>(atoi(arguments.getToken(0).c_str()));
            arguments.nextToken();
            ::sleepMS(sleepDuration);
        }
        else
            shell.printLine("Sleep duration is missing.");
    }

};

static _Sleep s_sleep;
