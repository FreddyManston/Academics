// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef MESSAGEBUFFER_H_
#define MESSAGEBUFFER_H_

#include "../Common.h"
#include "../util/MemoryRegion.h"

class MessageBuffer;

typedef uint32_t MessageSize;

// Message

class Message : public Unmovable {

    friend class MessageBuffer;

protected:

    uint8_t* m_messageStart;
    uint8_t* m_nextReadPosition;

public:

    Message() : m_messageStart(nullptr), m_nextReadPosition(nullptr) {
    }

    MessageSize getMessageSize() const;

    uint8_t* getMessageStart() const;

    const uint8_t* moveRaw(const size_t size);

    template<typename T>
    T read();

    const char* readString(size_t& length);

    void readString(std::string& string);

    void readResourceValue(ResourceValue& resourceValue);

};

// MessageBuffer

class MessageBuffer : public Unmovable {

    friend class Message;

protected:

    static const MessageSize MESSAGE_PROCESSED_FLAG = 0x80000000;

    typedef uint64_t MessageStartAlignmentType;

    const size_t m_bufferSize;
    MemoryRegion<uint8_t> m_buffer;
    uint8_t* m_fullLocation;
    uint8_t* m_nextAppendLocation;
    uint8_t* m_firstUnextractedLocation;
    uint8_t* m_firstUnprocessedLocation;

    bool invariants();

public:

    MessageBuffer(MemoryManager& memoryManager, const size_t bufferSize);

    bool isInitialized() const;

    void initialize();

    void deinitialize();

    bool hasUnprocessedData() const;

    void clear();

    bool extractMessage(Message& message);

    void messageProcessed(Message& message);

    bool reclaimProcessedSpace();

    void appendData(const uint8_t* const startLocation, const size_t dataSize);

    bool isOverLimit() const;

    const uint8_t* getBufferStart() const;

    size_t getFilledSize() const;

    uint8_t* startMessage();

    uint8_t* finishMessage(uint8_t* const messageStart);

    uint8_t* moveRaw(const size_t size);

    template<typename T>
    void write(const T value);

    void writeString(const std::string& string);

    void writeString(const char* const string);

    void writeResourceValue(const ResourceValue& resourceValue);

};

#endif // MESSAGEBUFFER_H_
