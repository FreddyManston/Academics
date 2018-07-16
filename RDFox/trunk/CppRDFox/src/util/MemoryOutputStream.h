// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef MEMORYOUTPUTSTREAM_H_
#define MEMORYOUTPUTSTREAM_H_

#include "OutputStream.h"

class MemoryOutputStream : public OutputStream {

private:
    
    std::string& m_buffer;
    
public:

    MemoryOutputStream(std::string& buffer);
    
    virtual void flush();

    virtual size_t write(const void* const data, const size_t numberOfBytesToWrite);

};

#endif /* MEMORYOUTPUTSTREAM_H_ */
