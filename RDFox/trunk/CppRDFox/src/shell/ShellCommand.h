// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef SHELLCOMMAND_H_
#define SHELLCOMMAND_H_

#include "Shell.h"

class ShellCommand {

protected:

    std::string m_commandName;

    bool isDataStoreActive(Shell& shell) const;

public:

    using OutputProtector = Shell::OutputProtector;

    ShellCommand(const std::string& commandName);

    virtual ~ShellCommand();

    const std::string& getCommandName() const {
        return m_commandName;
    }

    virtual std::string getOneLineHelp() const = 0;

    virtual void printHelpPage(std::ostream& output) const = 0;

    virtual void execute(Shell& shell, Shell::ArgumentsTokenizer& arguments) const = 0;

};

#endif /* SHELLCOMMAND_H_ */
