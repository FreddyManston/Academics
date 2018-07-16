// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef INPUTSTREAM_H_
#define INPUTSTREAM_H_

#include "../all.h"
#include "../RDFStoreException.h"
#include "MemoryRegion.h"

class InputStream : private Unmovable {

public:

    virtual ~InputStream() {
    }

    virtual void rewind() = 0;

    virtual size_t read(void* const data, const size_t numberOfBytesToRead) = 0;

    always_inline void readExactly(void* data, size_t numberOfBytesToRead) {
        while (numberOfBytesToRead > 0) {
            const size_t bytesRead = read(data, (numberOfBytesToRead > 1024 * 1024 * 1024 ? 1024 * 1024 * 1024 : numberOfBytesToRead));
            if (bytesRead == 0)
                throw RDF_STORE_EXCEPTION("Premature end of file.");
            data = (reinterpret_cast<uint8_t*>(data) + bytesRead);
            numberOfBytesToRead -= bytesRead;
        }
    }

    template<typename T>
    always_inline T read() {
        T value;
        readExactly(&value, sizeof(T));
        return value;
    }

    always_inline void readString(std::string& result, const size_t maxLength) {
        const size_t length = read<size_t>();
        if (length > maxLength)
            throw RDF_STORE_EXCEPTION("The string in the file is longer than the maximum allowed length.");
        result.clear();
        result.insert(0, length, ' ');
        readExactly(const_cast<char*>(result.c_str()), length);
    }

    always_inline bool checkNextString(const char* const expectedString) {
        const size_t expectedStringLength = ::strlen(expectedString);
        const size_t length = read<size_t>();
        if (length != expectedStringLength)
            return false;
        std::string result;
        result.insert(0, length, ' ');
        readExactly(const_cast<char*>(result.c_str()), length);
        return result == expectedString;
    }

    template<typename T>
    always_inline void readMemoryRegion(MemoryRegion<T>& memoryRegion) {
        const size_t maximumNumberOfItems = read<size_t>();
        const size_t beginIndex = read<size_t>();
        const size_t endIndex = read<size_t>();
        memoryRegion.initialize(maximumNumberOfItems);
        if (memoryRegion.ensureEndAtLeast(0, endIndex)) {
            memoryRegion.ensureBeginAtLeast(beginIndex);
            readExactly(memoryRegion.getBegin(), (endIndex - beginIndex) * sizeof(T));
        }
        else
            throw RDF_STORE_EXCEPTION("Not enough memory to load the memory region.");
    }

};

#endif /* INPUTSTREAM_H_ */
