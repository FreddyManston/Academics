// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef OUTPUTSTREAM_H_
#define OUTPUTSTREAM_H_

#include "../all.h"
#include "../RDFStoreException.h"
#include "MemoryRegion.h"

class OutputStream : private Unmovable {

public:

    virtual ~OutputStream() {
    }

    virtual void flush() = 0;

    virtual size_t write(const void* const data, const size_t numberOfBytesToWrite) = 0;

    always_inline void writeExactly(const void* data, size_t numberOfBytesToWrite) {
        while (numberOfBytesToWrite > 0) {
            const size_t bytesWritten = write(data, (numberOfBytesToWrite > 1024 * 1024 * 1024 ? 1024 * 1024 * 1024 : numberOfBytesToWrite));
            if (bytesWritten == 0)
                throw RDF_STORE_EXCEPTION("Cannot progress writing to a file.");
            data = (reinterpret_cast<const uint8_t*>(data) + bytesWritten);
            numberOfBytesToWrite -= bytesWritten;
        }
    }

    template<typename T>
    always_inline void write(const T value) {
        writeExactly(&value, sizeof(T));
    }

    always_inline void writeString(const std::string& string) {
        const size_t length = string.length();
        write<size_t>(length);
        writeExactly(string.c_str(), length);
    }

    always_inline void writeString(const char* const string) {
        const size_t length = ::strlen(string);
        write<size_t>(length);
        writeExactly(string, length);
    }

    template<typename T>
    always_inline void writeMemoryRegion(const MemoryRegion<T>& memoryRegion) {
        write(memoryRegion.getMaximumNumberOfItems());
        write(memoryRegion.getBeginIndex());
        write(memoryRegion.getEndIndex());
        writeExactly(memoryRegion.getBegin(), (memoryRegion.getEndIndex() - memoryRegion.getBeginIndex()) * sizeof(T));
    }

    always_inline void print(const char value) {
        write(value);
    }
    
    always_inline void print(const std::string& string) {
        const size_t length = string.length();
        writeExactly(string.c_str(), length);
    }
    
    always_inline void print(const char* const string) {
        const size_t length = ::strlen(string);
        writeExactly(string, length);
    }

    always_inline void print(const int value) {
        print(std::to_string(value));
    }

    always_inline void print(const long value) {
        print(std::to_string(value));
    }

    always_inline void print(const long long value) {
        print(std::to_string(value));
    }

    always_inline void print(const unsigned value) {
        print(std::to_string(value));
    }

    always_inline void print(const unsigned long value) {
        print(std::to_string(value));
    }

    always_inline void print(const unsigned long long value) {
        print(std::to_string(value));
    }

    always_inline void print(const float value) {
        print(std::to_string(value));
    }

    always_inline void print(const double value) {
        print(std::to_string(value));
    }

    always_inline void print(const long double value) {
        print(std::to_string(value));
    }

};

template<typename T>
always_inline OutputStream& operator<<(OutputStream& outputStream, T&& value) {
    outputStream.print(value);
    return outputStream;
}

#endif /* OUTPUTSTREAM_H_ */
