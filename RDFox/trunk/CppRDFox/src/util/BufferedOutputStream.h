// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef BUFFEREDOUTPUTSTREAM_H_
#define BUFFEREDOUTPUTSTREAM_H_

#include "OutputStream.h"

class BufferedOutputStream : public OutputStream {

protected:

    OutputStream& m_outputStream;
    std::unique_ptr<uint8_t[]> m_buffer;
    const size_t m_bufferSize;
    uint8_t* m_nextFreePosition;
    size_t m_freeBytesLeft;

public:

    BufferedOutputStream(OutputStream& outputStream, const size_t bufferSize = 65536);

    virtual ~BufferedOutputStream();

    virtual void flush();

    virtual size_t write(const void* const data, const size_t numberOfBytesToWrite);

};

#endif /* BUFFEREDOUTPUTSTREAM_H_ */
