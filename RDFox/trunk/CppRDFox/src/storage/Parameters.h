// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef PARAMETERS_H_
#define PARAMETERS_H_

#include "../all.h"

class InputStream;
class OutputStream;

class Parameters {

protected:

    std::unordered_map<std::string, std::string> m_keyValuePairs;

public:

    Parameters();

    Parameters(const Parameters& other);

    Parameters(Parameters&& other);

    Parameters& operator=(const Parameters& other) = delete;

    Parameters& operator=(Parameters&& other) = delete;

    ~Parameters();

    std::unordered_map<std::string, std::string>::const_iterator begin() const;

    std::unordered_map<std::string, std::string>::const_iterator end() const;

    bool containsKey(const std::string& key) const;

    const char* getString(const std::string& key, const char* const defaultValue) const;

    void setString(const std::string& key, const std::string& value);

    uint64_t getNumber(const std::string& key, const uint64_t defaultValue, const uint64_t invalidValue) const;

    void setNumber(const std::string& key, const uint64_t value);
    
    bool getBoolean(const std::string& key, const bool defaultValue) const;

    void setBoolean(const std::string& key, const bool value);

    void save(OutputStream& outputStream) const;

    void load(InputStream& inputStream);

    friend bool operator==(const Parameters& parameters1, const Parameters& parameters2);

    friend bool operator!=(const Parameters& parameters1, const Parameters& parameters2);

};

always_inline bool operator==(const Parameters& parameters1, const Parameters& parameters2) {
    return parameters1.m_keyValuePairs == parameters2.m_keyValuePairs;
}

always_inline bool operator!=(const Parameters& parameters1, const Parameters& parameters2) {
    return parameters1.m_keyValuePairs != parameters2.m_keyValuePairs;
}

#endif /* PARAMETERS_H_ */
