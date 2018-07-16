// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef SHELL_H_
#define SHELL_H_

#include "../Common.h"
#include "../RDFStoreException.h"
#include "../storage/DataStore.h"
#include "../util/Mutex.h"
#include "../util/Prefixes.h"
#include "../formats/turtle/TurtleTokenizer.h"
#include "../formats/turtle/TurtleSyntax.h"
#include "../formats/sources/MemorySource.h"

class ShellCommand;
class File;
class OutputStream;

class Shell {

protected:

    class ArgumentsMemorySource : public MemorySource {

    public:

        ArgumentsMemorySource(const char* const data, const size_t dataSize) : MemorySource(data, dataSize) {
        }

        always_inline const char* getRemainingTextStart() const {
            return reinterpret_cast<const char*>(m_currentByte);
        }

        always_inline const char* getAfterTextEnd() const {
            return reinterpret_cast<const char*>(m_afterBlockEnd);
        }

    };

    class ShellObject : public Unmovable {

    public:

        always_inline ShellObject() {
        }

        virtual ~ShellObject() {
        }

    };

    template<class T>
    class ShellObjectWrapper : public ShellObject {

    protected:

        std::unique_ptr<T> m_pointer;

    public:
        
        ShellObjectWrapper(std::unique_ptr<T> pointer) : m_pointer(std::move(pointer)) {
        }

        T* get() {
            return m_pointer.get();
        }

    };
    
public:

    typedef std::map<std::string, const ShellCommand*> CommandMap;

    class ArgumentsTokenizer : public TurtleTokenizer {

    protected:

        ArgumentsMemorySource m_memorySource;
        const char* m_currentTokenStart;

    public:

        always_inline ArgumentsTokenizer(const std::string& arguments) : TurtleTokenizer(), m_memorySource(arguments.c_str(), arguments.length()), m_currentTokenStart(m_memorySource.getRemainingTextStart()) {
            initialize(m_memorySource);
        }

        always_inline void nextToken() {
            // Skip whitespace while recording current position
            bool isInComment = false;
            while (true) {
                if (isAtEOF()) {
                    m_tokenType = EOF_TOKEN;
                    return;
                }
                if (isInComment) {
                    if (getCurrentCodePoint() == '\n')
                        isInComment = false;
                }
                else if (getCurrentCodePoint() == '#')
                    isInComment = true;
                else if (!TurtleSyntax::isWhitespace(getCurrentCodePoint()))
                    break;
                m_currentTokenStart = m_memorySource.getRemainingTextStart();
                doNextCodePoint();
            }
            TurtleTokenizer::nextToken();
            if (isErrorToken()) {
                std::ostringstream message;
                message << "Invalid token at line " << getTokenStartLine() << " and column " << getTokenStartColumn() << ".";
                throw RDF_STORE_EXCEPTION(message.str());
            }
        }

        always_inline void getRemainingText(const char* & remainingTextStart, size_t& remainingTextLength) {
            remainingTextStart = m_currentTokenStart;
            remainingTextLength = (m_memorySource.getAfterTextEnd() - m_currentTokenStart);
            m_tokenType = EOF_TOKEN;
        }

        bool getNextIRI(const Prefixes& prefixes, std::string& iri);

        bool getNextResource(const Prefixes& prefixes, ResourceText& resourceText);

    };

    enum VariableValueType { BOOLEAN, STRING, INTEGER };

    struct VariableValue {
        VariableValueType m_valueType;
        bool m_boolean;
        std::string m_string;
        int64_t m_integer;

        always_inline VariableValue() : m_valueType(BOOLEAN), m_boolean(false), m_string(), m_integer(0) {
        }
    };

    struct Variable {
        std::string m_description;
        VariableValue m_variableValue;

        always_inline Variable() : m_description(), m_variableValue() {
        }

        always_inline void set(const bool value) {
            m_variableValue.m_valueType = BOOLEAN;
            m_variableValue.m_boolean = value;
            m_variableValue.m_string.clear();
            m_variableValue.m_integer = 0;
        }

        always_inline void set(const char* const value) {
            m_variableValue.m_valueType = STRING;
            m_variableValue.m_boolean = false;
            m_variableValue.m_string = value;
            m_variableValue.m_integer = 0;
        }

        always_inline void set(const std::string& value) {
            m_variableValue.m_valueType = STRING;
            m_variableValue.m_boolean = false;
            m_variableValue.m_string = value;
            m_variableValue.m_integer = 0;
        }

        always_inline void set(const int64_t value) {
            m_variableValue.m_valueType = INTEGER;
            m_variableValue.m_boolean = false;
            m_variableValue.m_string.clear();
            m_variableValue.m_integer = value;
        }

        always_inline void initialize(const bool value, const char* const description) {
            set(value);
            m_description = description;
        }

        always_inline void initialize(const std::string& value, const char* const description) {
            set(value);
            m_description = description;
        }

        always_inline void initialize(const char* const value, const char* const description) {
            set(value);
            m_description = description;
        }

        always_inline void initialize(const int64_t value, const char* const description) {
            set(value);
            m_description = description;
        }

    };

protected:

    static CommandMap& getCommandMap();

    std::istream& m_input;
    std::ostream& m_output;
    Mutex m_outputMutex;
    Prefixes m_prefixes;
    std::map<std::string, Variable> m_variables;
    std::unordered_map<std::string, std::unique_ptr<DataStore> > m_dataStores;
    std::vector<std::string> m_shellObjectsNames;
    std::unordered_map<std::string, std::unique_ptr<ShellObject> > m_shellObjects;

    always_inline void doPrintLine() {
        m_output << std::endl;
    }

    template<typename T, typename... Rest>
    always_inline void doPrintLine(T&& value, Rest&&... rest) {
        m_output << value;
        doPrintLine(std::forward<Rest>(rest)...);
    }
    
public:

    friend class OutputProtector;

    class OutputProtector : public MutexHolder {

    public:

        OutputProtector(Shell& shell) : MutexHolder(shell.m_outputMutex) {
        }

    };

    Shell(std::istream& input, std::ostream& output);

    ~Shell();

    static always_inline const CommandMap& getAllCommands() {
        return getCommandMap();
    }

    static void registerCommand(const std::string& commandName, const ShellCommand& command);

    static const ShellCommand* getCommand(const std::string& commandName);

    always_inline std::istream& getInput() {
        return m_input;
    }

    always_inline std::ostream& getOutput() {
        return m_output;
    }

    always_inline bool hasDataStore() {
        return m_dataStores[getStringVariable("active")].get() != 0;
    }

    always_inline void setDataStore(std::unique_ptr<DataStore> dataStore) {
        m_dataStores[getStringVariable("active")] = std::move(dataStore);
    }

    always_inline DataStore& getDataStore() {
        return *m_dataStores[getStringVariable("active")];
    }

    always_inline DataStore* getDataStorePointer() {
        return m_dataStores[getStringVariable("active")].get();
    }

    always_inline DataStore* getDataStorePointer(const std::string& dataStoreName) {
        std::unordered_map<std::string, std::unique_ptr<DataStore> >::iterator iterator = m_dataStores.find(dataStoreName);
        if (iterator == m_dataStores.end())
            return 0;
        else
            return iterator->second.get();
    }

    template<class T>
    always_inline void setShellObject(const std::string& objectName, std::unique_ptr<T> object) {
        removeShellObject(objectName);
        m_shellObjects[objectName] = std::unique_ptr<ShellObject>(new ShellObjectWrapper<T>(std::move(object)));
        m_shellObjectsNames.push_back(objectName);
    }

    template<class T>
    always_inline T* getShellObject(const std::string& objectName) {
        auto iterator = m_shellObjects.find(objectName);
        if (iterator == m_shellObjects.end())
            return nullptr;
        else
            return static_cast<ShellObjectWrapper<T>*>(iterator->second.get())->get();
    }

    always_inline bool hasShellObject(const std::string& objectName) {
        return m_shellObjects.find(objectName) != m_shellObjects.end();
    }

    always_inline bool removeShellObject(const std::string& objectName) {
        auto iterator = m_shellObjects.find(objectName);
        if (iterator == m_shellObjects.end())
            return false;
        else {
            for (auto nameIterator = m_shellObjectsNames.begin(); nameIterator != m_shellObjectsNames.end(); ++nameIterator)
                if (*nameIterator == objectName) {
                    m_shellObjectsNames.erase(nameIterator);
                    break;
                }
            m_shellObjects.erase(iterator);
            return true;
        }
    }

    always_inline Prefixes& getPrefixes() {
        return m_prefixes;
    }

    always_inline std::map<std::string, Variable>& getVariables() {
        return m_variables;
    }

    always_inline Variable& getVariable(const std::string& variableName) {
        return m_variables[variableName];
    }

    always_inline bool getBooleanVariable(const std::string& variableName) {
        return getVariable(variableName).m_variableValue.m_boolean;
    }

    always_inline const std::string& getStringVariable(const std::string& variableName) {
        return getVariable(variableName).m_variableValue.m_string;
    }

    always_inline int64_t getIntegerVariable(const std::string& variableName) {
        return getVariable(variableName).m_variableValue.m_integer;
    }

    always_inline void protectOutput() {
        m_outputMutex.lock();
    }

    always_inline void unprotectOutput() {
        m_outputMutex.unlock();
    }

    void setRootDirectory(const std::string& newRoot);

    void expandRelativeFileName(std::string& fileName, const std::string& fileNameType);

    std::string expandRelativeFileNameEx(const std::string& fileName, const std::string& fileNameType);

    std::string getScriptFileName(const std::string& rawScriptFileName);

    bool selectOutput(std::ostream*& selectedOutput, std::unique_ptr<std::ofstream>& resultsOutputFile);

    void selectOutput(std::unique_ptr<OutputStream>& outputStream, std::unique_ptr<File>& outputFile, std::string& outputFileName);

    template<typename... Args>
    always_inline void printLine(Args&&... args) {
        OutputProtector protector(*this);
        doPrintLine(std::forward<Args>(args)...);
    }

    template<typename... Args>
    always_inline void printError(const std::exception& error, Args&&... args) {
        OutputProtector protector(*this);
        doPrintLine(std::forward<Args>(args)...);
        m_output << error << std::endl;
    }

    void run();

    void run(std::istream& input, bool showPrompt);

    void executeCommand(const std::string& command);

};

#endif /* SHELL_H_ */
