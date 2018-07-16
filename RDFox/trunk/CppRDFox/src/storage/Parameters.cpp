// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "../util/InputStream.h"
#include "../util/OutputStream.h"
#include "Parameters.h"

Parameters::Parameters() : m_keyValuePairs() {
}

Parameters::Parameters(const Parameters& other) : m_keyValuePairs(other.m_keyValuePairs) {
}

Parameters::Parameters(Parameters&& other) : m_keyValuePairs(std::move(other.m_keyValuePairs)) {
}

Parameters::~Parameters() {
}

std::unordered_map<std::string, std::string>::const_iterator Parameters::begin() const {
    return m_keyValuePairs.begin();
}

std::unordered_map<std::string, std::string>::const_iterator Parameters::end() const {
    return m_keyValuePairs.end();
}

bool Parameters::containsKey(const std::string& key) const {
    return m_keyValuePairs.find(key) != m_keyValuePairs.end();
}

const char* Parameters::getString(const std::string& key, const char* const defaultValue) const {
    const std::unordered_map<std::string, std::string>::const_iterator iterator = m_keyValuePairs.find(key);
    if (iterator == m_keyValuePairs.end())
        return defaultValue;
    else
        return iterator->second.c_str();
}

void Parameters::setString(const std::string& key, const std::string& value) {
    m_keyValuePairs[key] = value;
}

uint64_t Parameters::getNumber(const std::string& key, const uint64_t defaultValue, const uint64_t invalidValue) const {
    const std::unordered_map<std::string, std::string>::const_iterator iterator = m_keyValuePairs.find(key);
    if (iterator == m_keyValuePairs.end())
        return defaultValue;
    else {
        const char* stringValue = iterator->second.c_str();
        char* firstUnconverted;
        const uint64_t result = strtoull(stringValue, &firstUnconverted, 10);
        if (stringValue + iterator->second.size() != firstUnconverted)
            return invalidValue;
        else
            return result;
    }
}

void Parameters::setNumber(const std::string& key, const uint64_t value) {
    std::ostringstream buffer;
    buffer << value;
    m_keyValuePairs[key] = buffer.str();
}

bool Parameters::getBoolean(const std::string& key, const bool defaultValue) const {
    const std::unordered_map<std::string, std::string>::const_iterator iterator = m_keyValuePairs.find(key);
    if (iterator == m_keyValuePairs.end())
        return defaultValue;
    else
        return iterator->second == "true" || iterator->second == "on" || iterator->second == "yes";
}

void Parameters::setBoolean(const std::string& key, const bool value) {
    if (value)
        m_keyValuePairs[key] = "true";
    else
        m_keyValuePairs[key] = "false";
}

void Parameters::save(OutputStream& outputStream) const {
    outputStream.writeString("Parameters");
    outputStream.write(m_keyValuePairs.size());
    for (std::unordered_map<std::string, std::string>::const_iterator iterator = m_keyValuePairs.begin(); iterator != m_keyValuePairs.end(); ++iterator) {
        outputStream.writeString(iterator->first);
        outputStream.writeString(iterator->second);
    }
}

void Parameters::load(InputStream& inputStream) {
    if (!inputStream.checkNextString("Parameters"))
        throw RDF_STORE_EXCEPTION("Cannot load Parameters.");
    const size_t numberOfPairs = inputStream.read<size_t>();
    m_keyValuePairs.clear();
    std::string key;
    std::string value;
    for (size_t pairIndex = 0; pairIndex < numberOfPairs; ++pairIndex) {
        inputStream.readString(key, 4096);
        inputStream.readString(value, 4096);
        m_keyValuePairs[key] = value;
    }
}
