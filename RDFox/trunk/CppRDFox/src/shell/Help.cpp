// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "ShellCommand.h"

class Help : public ShellCommand {

public:

    Help() : ShellCommand("help") {
    }

    virtual std::string getOneLineHelp() const {
        return "prints help";
    }

    virtual void printHelpPage(std::ostream& output) const {
        output
            << "help <command name>*" << std::endl
            << "    When called without arguments, prints the list of all available commands." << std::endl
            << "    When called with arguments, prints help about the specified commands." << std::endl;
    }

    virtual void execute(Shell& shell, Shell::ArgumentsTokenizer& arguments) const {
        OutputProtector protector(shell);
        if (arguments.isEOF()) {
            const Shell::CommandMap& allCommands = Shell::getAllCommands();
            size_t longestCommandName = 0;
            for (Shell::CommandMap::const_iterator iterator = allCommands.begin(); iterator != allCommands.end(); ++iterator) {
                const size_t commandNameNameLength = iterator->second->getCommandName().length();
                if (commandNameNameLength > longestCommandName)
                    longestCommandName = commandNameNameLength;
            }
            for (Shell::CommandMap::const_iterator iterator = allCommands.begin(); iterator != allCommands.end(); ++iterator) {
                const ShellCommand* command = iterator->second;
                shell.getOutput() << command->getCommandName();
                const size_t spaceCount = longestCommandName - command->getCommandName().size() + 10;
                for (size_t index = 0; index < spaceCount; ++index)
                    shell.getOutput() << " ";
                shell.getOutput() << command->getOneLineHelp() << std::endl;
            }
        }
        else {
            while (arguments.isSymbol()) {
                std::string commandName = arguments.getToken();
                toLowerCase(commandName);
                const ShellCommand* const command = Shell::getCommand(commandName);
                if (command == 0)
                    shell.getOutput() << "Unknown command '" << commandName << "'." << std::endl;
                else
                    command->printHelpPage(shell.getOutput());
                arguments.nextToken();
            }
        }
    }

};

static Help s_help;
