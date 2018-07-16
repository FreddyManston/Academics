// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "ShellCommand.h"

class Set : public ShellCommand {

public:

    Set() : ShellCommand("set") {
    }

    virtual std::string getOneLineHelp() const {
        return "sets a variable of the shell";
    }

    virtual void printHelpPage(std::ostream& output) const {
        output
            << "set [<name> [<value>]]" << std::endl
            << "    Sets the specified variable to the given value." << std::endl
            << "    If no value is given, the current value is printed."  << std::endl
            << "    If no name is given, all values are printed." << std::endl;
    }

    size_t getNumberOfDigits(int64_t value) const {
        size_t result;
        if (value == 0)
            result = 1;
        else {
            if (value < 0) {
                result = 1;
                value = -value;
            }
            else
                result = 0;
            while (value != 0) {
                ++result;
                value /= 10;
            }
        }
        return result;
    }

    virtual void execute(Shell& shell, Shell::ArgumentsTokenizer& arguments) const {
        if (arguments.isEOF()) {
            const std::map<std::string, Shell::Variable>& variables = shell.getVariables();
            size_t maxVariableNameLength = 0;
            size_t maxVariableValueLength = 0;
            for (std::map<std::string, Shell::Variable>::const_iterator iterator = variables.begin(); iterator != variables.end(); ++iterator) {
                const Shell::Variable& variable = iterator->second;
                maxVariableNameLength = std::max(maxVariableNameLength, iterator->first.size());
                switch (variable.m_variableValue.m_valueType) {
                case Shell::BOOLEAN:
                    maxVariableValueLength = std::max(maxVariableValueLength, variable.m_variableValue.m_boolean ? static_cast<size_t>(4) : static_cast<size_t>(5));
                    break;
                case Shell::STRING:
                    maxVariableValueLength = std::max(maxVariableValueLength, variable.m_variableValue.m_string.size() + 2);
                    break;
                case Shell::INTEGER:
                    maxVariableValueLength = std::max(maxVariableValueLength, getNumberOfDigits(variable.m_variableValue.m_integer));
                    break;
                }
            }
            for (std::map<std::string, Shell::Variable>::const_iterator iterator = variables.begin(); iterator != variables.end(); ++iterator) {
                const Shell::Variable& variable = iterator->second;
                const std::streamsize oldWidth = shell.getOutput().width();
                const std::ostream::fmtflags oldFlags = shell.getOutput().flags();
                shell.getOutput() << std::setw(static_cast<int>(maxVariableNameLength)) << std::left << iterator->first << std::setw(0) << " = " << std::setw(static_cast<int>(maxVariableValueLength));
                switch (variable.m_variableValue.m_valueType) {
                case Shell::BOOLEAN:
                    shell.getOutput() << (variable.m_variableValue.m_boolean ? "true" : "false");
                    break;
                case Shell::STRING:
                    shell.getOutput() << "\"" + variable.m_variableValue.m_string + "\"";
                    break;
                case Shell::INTEGER:
                    shell.getOutput() << variable.m_variableValue.m_integer;
                    break;
                }
                shell.getOutput().width(oldWidth);
                shell.getOutput().flags(oldFlags);
                shell.getOutput() << " : "<< variable.m_description << std::endl;
            }
        }
        else if (arguments.isSymbol()) {
            std::string variableName = arguments.getToken();
            arguments.nextToken();
            if (arguments.isEOF()) {
                const std::map<std::string, Shell::Variable>& variables = shell.getVariables();
                std::map<std::string, Shell::Variable>::const_iterator iterator = variables.find(variableName);
                if (iterator == variables.end())
                    shell.printLine("Variable '", variableName, "' not found.");
                else {
                    OutputProtector protector(shell);
                    const Shell::Variable& variable = iterator->second;
                    shell.getOutput() << variableName << " = ";
                    switch (variable.m_variableValue.m_valueType) {
                    case Shell::BOOLEAN:
                        shell.getOutput() << (variable.m_variableValue.m_boolean ? "true" : "false");
                        break;
                    case Shell::STRING:
                        shell.getOutput() << "\"" << variable.m_variableValue.m_string << "\"";
                        break;
                    case Shell::INTEGER:
                        shell.getOutput() << variable.m_variableValue.m_integer;
                        break;
                    }
                    shell.getOutput() << " : " << variable.m_description << std::endl;
                }
            }
            else if (arguments.isSymbol() || arguments.isNumber() || arguments.isQuotedString() || arguments.isQuotedIRI()) {
                std::string variableValue = arguments.getToken();
                if (arguments.isSymbol()) {
                    if (variableValue == "on" || variableValue == "true" || variableValue == "off" || variableValue == "false") {
                        bool oldValue = shell.getBooleanVariable(variableName);
                        bool newValue = (variableValue == "on" || variableValue == "true");
                        shell.getVariable(variableName).set(newValue);
                        shell.printLine(variableName, " = ", (newValue ? "true" : "false"), " (was ", (oldValue ? "true" : "false"), ")");
                    }
                    else {
                        std::string oldValue = shell.getStringVariable(variableName);
                        shell.getVariable(variableName).set(variableValue);
                        shell.printLine(variableName, " = \"", variableValue, "\" (was \"", oldValue, "\")");
                    }
                }
                else if (arguments.isNumber()) {
                    int64_t newValue;
                    std::istringstream input(variableValue);
                    input >> newValue;
                    int64_t oldValue = shell.getIntegerVariable(variableName);
                    shell.getVariable(variableName).set(newValue);
                    shell.printLine(variableName, " = ", newValue, " (was ", oldValue, ")");
                }
                else if (arguments.isQuotedString() || arguments.isQuotedIRI()) {
                    std::string oldValue = shell.getStringVariable(variableName);
                    shell.getVariable(variableName).set(variableValue);
                    shell.printLine(variableName, " = \"", variableValue, "\" (was \"", "\")");
                }
                arguments.nextToken();
                if (!arguments.isEOF())
                    shell.printLine("Superfluous variable values were ignored.");
            }
            else
                shell.printLine("Value for variable '", variableName, "' is invalid.");
        }
    }

};

static Set s_set;
