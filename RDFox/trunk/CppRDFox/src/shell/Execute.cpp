// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "../all.h"
#include "ShellCommand.h"

class Execute : public ShellCommand {

public:

    Execute() : ShellCommand("exec") {
    }

    virtual std::string getOneLineHelp() const {
        return "executes scripts";
    }

    virtual void printHelpPage(std::ostream& output) const {
        output <<
            "exec [<repeat number>] <file name> arguments" << std::endl <<
            "    Executes the contents of the specified script, each repeated the specified number of times." << std::endl <<
            "    Argument tokens are passed as variables $(1), $(2),..." << std::endl;
    }

    virtual void execute(Shell& shell, Shell::ArgumentsTokenizer& arguments) const {
        size_t repeatNumber = 1;
        if (arguments.isNumber()) {
            repeatNumber = static_cast<size_t>(atoi(arguments.getToken(0).c_str()));
            arguments.nextToken();
        }
        if (arguments.isSymbol() || arguments.isQuotedString() || arguments.isQuotedIRI()) {
            std::string rawScriptFileName = arguments.getToken();
            arguments.nextToken();
            std::string scriptFileName = shell.getScriptFileName(rawScriptFileName);
            if (scriptFileName.empty())
                shell.printLine("Script file \"", rawScriptFileName, "\" cannot be found.");
            else {
                std::vector<std::pair<Shell::Variable*, Shell::VariableValue> > oldVariableValues;
                size_t argumentIndex = 1;
                while (!arguments.isEOF()) {
                    std::ostringstream variableNameBuffer;
                    variableNameBuffer << argumentIndex;
                    std::string variableName = variableNameBuffer.str();
                    Shell::Variable& variable = shell.getVariable(variableName);
                    oldVariableValues.push_back(std::make_pair(&variable, variable.m_variableValue));
                    std::string variableValue = arguments.getToken();
                    if (arguments.isSymbol()) {
                        if (variableValue == "on" || variableValue == "true" || variableValue == "off" || variableValue == "false")
                            variable.set((variableValue == "on" || variableValue == "true"));
                        else
                            variable.set(variableValue);
                    }
                    else if (arguments.isNumber()) {
                        int64_t newValue;
                        std::istringstream(variableValue) >> newValue;
                        variable.set(newValue);
                    }
                    else if (arguments.isQuotedString() || arguments.isQuotedIRI())
                        variable.set(variableValue);
                    arguments.nextToken();
                    ++argumentIndex;
                }
                std::ifstream input(scriptFileName.c_str(), std::ifstream::in);
                if (input.good()) {
                    for (size_t iteration = 0; iteration < repeatNumber; ++iteration)
                        shell.run(input, false);
                    input.close();
                }
                else
                    shell.printLine("Cannot open script \"", scriptFileName, "\".");
                for (auto iterator = oldVariableValues.begin(); iterator != oldVariableValues.end(); ++iterator)
                    iterator->first->m_variableValue = iterator->second;
            }
        }
        else
            shell.printLine("Script name is missing.");
    }

};

static Execute s_execute;
