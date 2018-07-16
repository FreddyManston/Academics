// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "ShellCommand.h"

ShellCommand::ShellCommand(const std::string& commandName) : m_commandName(commandName) {
    Shell::registerCommand(m_commandName, *this);
}

ShellCommand::~ShellCommand() {
}

bool ShellCommand::isDataStoreActive(Shell& shell) const {
    if (!shell.hasDataStore()) {
        shell.printLine("No data store is currently active.");
        return false;
    }
    else
        return true;
}
