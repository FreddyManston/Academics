// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifdef WITH_TEST

#define AUTO_TEST   MessageBufferTest

#include <CppTest/AutoTest.h>

#include "../../src/RDFStoreException.h"
#include "../../src/node-server/MessageBufferImpl.h"
#include "../../src/util/MemoryManager.h"

class MessageBufferTest {

protected:

    MemoryManager m_memoryManager;
    MessageBuffer m_messageBuffer;
    uint8_t* m_messageStart;

public:

    MessageBufferTest() : m_memoryManager(1024), m_messageBuffer(m_memoryManager, 64), m_messageStart(nullptr) {
    }

    void initialize() {
        m_messageBuffer.initialize();
    }

    void appendData(const uint8_t* data, size_t dataSize) {
        m_messageBuffer.appendData(data, dataSize);
    }

    void assertExtract(const char* const fileName, const long lineNumber, Message& message, const bool expectedResult) {
        CppTest::assertEqual(expectedResult, m_messageBuffer.extractMessage(message), fileName, lineNumber);
    }

    void messageProcessed(Message& message) {
        m_messageBuffer.messageProcessed(message);
        m_messageBuffer.reclaimProcessedSpace();
    }

    void assertSize(const char* const fileName, const long lineNumber, Message& message, const MessageSize expectedMessageSize) {
        CppTest::assertEqual(expectedMessageSize, message.getMessageSize(), fileName, lineNumber);
    }

    template<typename T>
    void assertNext(const char* const fileName, const long lineNumber, Message& message, const T expectedValue) {
        CppTest::assertEqual(expectedValue, message.read<T>(), fileName, lineNumber);
    }

    void assertNextString(const char* const fileName, const long lineNumber, Message& message, const std::string& expectedString) {
        std::string actualString;
        message.readString(actualString);
        CppTest::assertEqual(expectedString, actualString, fileName, lineNumber);
    }

    void startMessage() {
        m_messageStart = m_messageBuffer.startMessage();
    }

    void finishMessage() {
        m_messageBuffer.finishMessage(m_messageStart);
    }

    template<typename T>
    void write(const T value) {
        m_messageBuffer.write(value);
    }

    void writeString(const std::string& value) {
        m_messageBuffer.writeString(value);
    }

    void writeString(const char* const value) {
        m_messageBuffer.writeString(value);
    }

    template<typename T1, typename T2>
    T1 V(const T2 value) {
        return static_cast<T1>(value);
    }

};

#define ASSERT_EXTRACT(message, expectedResult) \
    assertExtract(__FILE__, __LINE__, message, expectedResult)

#define ASSERT_SIZE(message, expectedMessageSize) \
    assertSize(__FILE__, __LINE__, message, expectedMessageSize)

#define ASSERT_NEXT(message, expectedValue) \
    assertNext(__FILE__, __LINE__, message, expectedValue)

#define ASSERT_NEXT_STRING(message, expectedValue) \
    assertNextString(__FILE__, __LINE__, message, expectedValue)

TEST(testBasic) {
    Message message;
    ASSERT_EXTRACT(message, false);

    // Write message 1
    startMessage();
    write<uint32_t>(10);
    write<uint8_t>(5);
    write<uint64_t>(20);
    finishMessage();

    // Write message 2
    startMessage();
    write<uint32_t>(17);
    write<uint64_t>(41);
    finishMessage();

    // Read message 1
    ASSERT_EXTRACT(message, true);
    ASSERT_SIZE(message, 3 * sizeof(uint64_t));
    ASSERT_NEXT(message, V<uint32_t>(10));
    ASSERT_NEXT(message, V<uint8_t>(5));
    ASSERT_NEXT(message, V<uint64_t>(20));
    messageProcessed(message);
    ASSERT_NOT_EQUAL(0, m_messageBuffer.getFilledSize());

    // Read message 2
    ASSERT_EXTRACT(message, true);
    ASSERT_SIZE(message, 2 * sizeof(uint64_t));
    ASSERT_NEXT(message, V<uint32_t>(17));
    ASSERT_NEXT(message, V<uint64_t>(41));
    messageProcessed(message);
    ASSERT_EQUAL(0, m_messageBuffer.getFilledSize());
}

TEST(testOutOfOrderConfirmation) {
    Message message1;
    Message message2;
    Message message3;
    ASSERT_EXTRACT(message1, false);

    // Write message 1
    startMessage();
    writeString("ABCD");
    write<uint64_t>(20);
    write<uint8_t>(90);
    finishMessage();
    ASSERT_FALSE(m_messageBuffer.isOverLimit());

    // Write message 2
    startMessage();
    writeString("0123456789");
    write<uint8_t>(20);
    write<uint64_t>(70);
    finishMessage();
    ASSERT_FALSE(m_messageBuffer.isOverLimit());

    // Write message 3
    startMessage();
    write<uint8_t>(50);
    writeString("ABCDEFGHI");
    write<uint64_t>(1234);
    finishMessage();
    ASSERT_TRUE(m_messageBuffer.isOverLimit());

    // Read message 1
    ASSERT_EXTRACT(message1, true);
    ASSERT_SIZE(message1, sizeof(MessageSize) + 5 + 7 + sizeof(uint64_t) + sizeof(uint8_t) + 7);
    ASSERT_NEXT_STRING(message1, "ABCD");
    ASSERT_NEXT(message1, V<uint64_t>(20));
    ASSERT_NEXT(message1, V<uint8_t>(90));
    ASSERT_TRUE(m_messageBuffer.isOverLimit());

    // Read message 2
    ASSERT_EXTRACT(message2, true);
    ASSERT_SIZE(message2, sizeof(MessageSize) + 11 + sizeof(uint8_t) + sizeof(uint64_t));
    ASSERT_NEXT_STRING(message2, "0123456789");
    ASSERT_NEXT(message2, V<uint8_t>(20));
    ASSERT_NEXT(message2, V<uint64_t>(70));
    ASSERT_TRUE(m_messageBuffer.isOverLimit());

    // Read message 3
    ASSERT_EXTRACT(message3, true);
    ASSERT_SIZE(message3, sizeof(MessageSize) + sizeof(uint8_t) + 10 + 1 + sizeof(uint64_t));
    ASSERT_NEXT(message3, V<uint8_t>(50));
    ASSERT_NEXT_STRING(message3, "ABCDEFGHI");
    ASSERT_NEXT(message3, V<uint64_t>(1234));
    ASSERT_TRUE(m_messageBuffer.isOverLimit());

    // Process the second message. Since the first message is not processed, we do not free any space.
    messageProcessed(message2);
    ASSERT_TRUE(m_messageBuffer.isOverLimit());

    // Process the thord message. Since the first message is not processed, we do not free any space.
    messageProcessed(message3);
    ASSERT_TRUE(m_messageBuffer.isOverLimit());

    // Now process the first message. This should free all space.
    messageProcessed(message1);
    ASSERT_FALSE(m_messageBuffer.isOverLimit());
    ASSERT_EQUAL(0, m_messageBuffer.getFilledSize());
}

#endif
