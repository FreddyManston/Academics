// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef INPUTOUTPUT_H_
#define INPUTOUTPUT_H_

#include "../all.h"
#include "../RDFStoreException.h"
#include "../logic/Logic.h"
#include "InputConsumer.h"

class InputSource;
class Prefixes;
class DataStore;
class OutputStream;

// FormatHandler

class FormatHandler : private Unmovable {

public:

    typedef std::unordered_set<std::string> FormatNames;

protected:

    const size_t m_priority;
    const std::string m_name;
    const FormatNames m_handledFormatNames;

    FormatHandler(const size_t priority, const std::string& name, FormatNames&& formatNames);

    FormatHandler(const size_t priority, const std::string& name, const FormatNames& formatNames);

public:

    virtual ~FormatHandler();

    always_inline const size_t getPriority() const {
        return m_priority;
    }

    always_inline const std::string& getName() const {
        return m_name;
    }

    always_inline const std::unordered_set<std::string>& getHandledFormatNames() const {
        return m_handledFormatNames;
    }

    virtual bool storesFacts() const = 0;

    virtual bool storesRules() const = 0;

    virtual void load(InputSource& inputSource, Prefixes& prefixes, LogicFactory& logicFactory, InputConsumer& inputConsumer, std::string& formatName) const = 0;

    virtual void save(DataStore& dataStore, Prefixes& prefixes, OutputStream& outputStream, const std::string& formatName) const = 0;

    virtual std::unique_ptr<InputConsumer> newExporter(Prefixes& prefixes, OutputStream& outputStream, const std::string& formatName) const = 0;

};

// I/O functions

extern const FormatHandler* getFormatHandlerFor(const std::string& formatName);

extern void load(InputSource& inputSource, Prefixes& prefixes, LogicFactory& logicFactory, InputConsumer& inputConsumer, std::string& formatName);

extern void save(DataStore& dataStore, Prefixes& prefixes, OutputStream& outputStream, const std::string& formatName);

extern std::unique_ptr<InputConsumer> newExporter(Prefixes& prefixes, OutputStream& outputStream, const std::string& formatName);

#endif // INPUTOUTPUT_H_
