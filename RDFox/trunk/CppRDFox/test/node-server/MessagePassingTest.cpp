// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifdef WITH_TEST

#define AUTO_TEST   MessagePassingTest

#include <CppTest/AutoTest.h>

#include "../../src/node-server/MessageBufferImpl.h"
#include "../../src/node-server/MessagePassingImpl.h"
#include "../../src/util/MemoryManager.h"
#include "../../src/util/Mutex.h"
#include "../../src/util/Condition.h"

class MessagePassingTest {

protected:

    class MessagePassingInstance : public MessagePassing<MessagePassingInstance> {

        friend class MessagePassing<MessagePassingInstance>;

    protected:

        always_inline uint32_t getMessageBufferIndex(const uint8_t* messagePayloadStart) {
            return *reinterpret_cast<const uint32_t*>(messagePayloadStart);
        }

        always_inline void messagePassingStarting() {
        }

        always_inline void messagePassingConnected() {
        }

        always_inline void messagePassingStopping() {
        }

        always_inline void messagePassingChannelsStopped() {
        }

        always_inline void messagePassingError(const std::exception& error) {
        }
        
    public:

        MessagePassingInstance(MemoryManager& memoryManager, const uint32_t numberOfChannels, const ChannelID myChannelID, const size_t totalSizePerMessageBuffer) :
            MessagePassing(memoryManager, numberOfChannels, myChannelID, 2, totalSizePerMessageBuffer)
        {
        }

        always_inline void extractIncomingMessage(const size_t messageBufferIndex, Message& message, MessageBuffer* & messageBuffer) {
            MutexHolder mutexHolder(m_mutex);
            while (true) {
                if (messageBufferIndex < m_incomingDataInfos.size()) {
                    messageBuffer = &m_incomingDataInfos[messageBufferIndex]->m_messageBuffer;
                    if (messageBuffer->extractMessage(message))
                        return;
                }
                if (!m_condition.wait(m_mutex, 1000))
                    FAIL2("Did not receive a message in one second.");
            }
        }

        always_inline void incomingMessageProcessed(const uint32_t messageBufferIndex, Message& message, MessageBuffer& messageBuffer) {
            messageBuffer.messageProcessed(message);
            // If a message was processed on a paused peer, we interrupt the socket selector to force
            // it to run gargabe collection, and the latter must be done on the network thread.
            if (m_incomingDataInfos[messageBufferIndex]->m_peersPaused)
                m_socketSelector.interrupt();
        }

        using MessagePassing<MessagePassingInstance>::levelPaused;

    };
    
    const uint32_t NUMBER_OF_MESSAGE_PASSINGS = 2;

    MemoryManager m_memoryManager;
    unique_ptr_vector<MessagePassingInstance> m_messagePassings;
    uint8_t* m_outgoingMessageStart;
    MessageBuffer m_outgoingMessageBuffer;
    Message m_incomingMessage;
    MessageBuffer* m_messageBuffer;

public:

    MessagePassingTest() :
        m_memoryManager(1024 * 1024 * 1024),
        m_messagePassings(),
        m_outgoingMessageBuffer(m_memoryManager, 1024),
        m_incomingMessage(),
        m_messageBuffer(nullptr)
    {
        for (ChannelID channelID = 0; channelID < NUMBER_OF_MESSAGE_PASSINGS; ++channelID)
            m_messagePassings.push_back(std::unique_ptr<MessagePassingInstance>(new MessagePassingInstance(m_memoryManager, NUMBER_OF_MESSAGE_PASSINGS, channelID, 128)));
    }

    void initialize() {
        m_outgoingMessageBuffer.initialize();
    }

    void start() {
        for (ChannelID channelID = 0; channelID < NUMBER_OF_MESSAGE_PASSINGS; ++channelID) {
            std::string serviceName = std::to_string(12000 + channelID);
            for (auto iterator = m_messagePassings.begin(); iterator != m_messagePassings.end(); ++iterator)
                (*iterator)->setChannelNodeServiceName(channelID, "localhost", serviceName);
        }
        for (auto iterator = m_messagePassings.begin(); iterator != m_messagePassings.end(); ++iterator)
            ASSERT_TRUE((*iterator)->start(5000));
        for (auto iterator = m_messagePassings.begin(); iterator != m_messagePassings.end(); ++iterator)
            ASSERT_TRUE((*iterator)->waitForReady());
    }

    void stop() {
        for (auto iterator = m_messagePassings.begin(); iterator != m_messagePassings.end(); ++iterator)
            ASSERT_TRUE((*iterator)->stop() == nullptr);
    }

    void assertSize(const char* const fileName, const long lineNumber, const MessageSize expectedMessageSize) {
        CppTest::assertEqual(expectedMessageSize, m_incomingMessage.getMessageSize(), fileName, lineNumber);
    }

    template<typename T>
    void assertNext(const char* const fileName, const long lineNumber, const T expectedValue) {
        CppTest::assertEqual(expectedValue, m_incomingMessage.read<T>(), fileName, lineNumber);
    }

    void assertNextString(const char* const fileName, const long lineNumber, const std::string& expectedString) {
        std::string actualString;
        m_incomingMessage.readString(actualString);
        CppTest::assertEqual(expectedString, actualString, fileName, lineNumber);
    }

    template<typename T1, typename T2>
    T1 V(const T2 value) {
        return static_cast<T1>(value);
    }

    void startMessage(const uint32_t messageBufferIndex) {
        m_outgoingMessageStart = m_outgoingMessageBuffer.startMessage();
        m_outgoingMessageBuffer.write<uint32_t>(messageBufferIndex);
    }

    void finishMessage() {
        m_outgoingMessageBuffer.finishMessage(m_outgoingMessageStart);
    }

    bool send(const ChannelID fromChannelID, const ChannelID toChannelID, const uint8_t *const data, const size_t size) {
        return m_messagePassings[fromChannelID]->sendData(toChannelID, data, size);
    }

    void assertSend(const char* const fileName, const long lineNumber, const ChannelID fromChannelID, const ChannelID toChannelID) {
        CppTest::assertTrue(send(fromChannelID, toChannelID, m_outgoingMessageBuffer.getBufferStart(), m_outgoingMessageBuffer.getFilledSize()), fileName, lineNumber, "Data was not sent correctly.");
        m_outgoingMessageBuffer.clear();
    }

    void receive(const ChannelID channelID, const size_t messageBufferIndex) {
        m_messagePassings[channelID]->extractIncomingMessage(messageBufferIndex, m_incomingMessage, m_messageBuffer);
        ASSERT_EQUAL(messageBufferIndex, m_incomingMessage.read<uint32_t>());
    }

    void process(const ChannelID channelID, const uint32_t messageBufferIndex, Message& incomingMessage) {
        m_messagePassings[channelID]->incomingMessageProcessed(messageBufferIndex, incomingMessage, *m_messageBuffer);
    }

    void process(const ChannelID channelID, const uint32_t messageBufferIndex) {
        process(channelID, messageBufferIndex, m_incomingMessage);
    }

};

#define ASSERT_EXTRACT(expectedResult) \
    assertExtract(__FILE__, __LINE__, expectedResult)

#define ASSERT_SIZE(expectedMessageSize) \
    assertSize(__FILE__, __LINE__, expectedMessageSize)

#define ASSERT_NEXT(expectedValue) \
    assertNext(__FILE__, __LINE__, expectedValue)

#define ASSERT_NEXT_STRING(expectedValue) \
    assertNextString(__FILE__, __LINE__, expectedValue)

#define ASSERT_SEND(fromChannelID, toChannelID) \
    assertSend(__FILE__, __LINE__, fromChannelID, toChannelID)

TEST(testBasic) {
    start();

    // Send messages from 0

    // Message A on 0 -> 1:1
    startMessage(1);
    m_outgoingMessageBuffer.write<uint64_t>(15);
    m_outgoingMessageBuffer.writeString("Message A");
    m_outgoingMessageBuffer.write<uint64_t>(51);
    finishMessage();
    ASSERT_SEND(0, 1);

    // Message B on 0 -> 1:0
    startMessage(0);
    m_outgoingMessageBuffer.write<uint32_t>(67);
    m_outgoingMessageBuffer.writeString("Message B");
    finishMessage();
    ASSERT_SEND(0, 1);

    // Message C on 0 -> 0:0
    startMessage(0);
    m_outgoingMessageBuffer.write<uint32_t>(42);
    m_outgoingMessageBuffer.writeString("Message C");
    finishMessage();
    ASSERT_SEND(0, 0);


    // Send messages from 1

    // Message D on 1 -> 0:1
    startMessage(1);
    m_outgoingMessageBuffer.writeString("Message D");
    finishMessage();
    ASSERT_SEND(1, 0);

    // Message E on 1 -> 0:0
    startMessage(0);
    finishMessage();
    ASSERT_SEND(1, 0);

    // Message F on 1 -> 1:1
    startMessage(1);
    m_outgoingMessageBuffer.writeString("Message F");
    m_outgoingMessageBuffer.write<uint32_t>(78);
    finishMessage();
    ASSERT_SEND(1, 1);


    // Process messages on node 0

    // Message C received on 0:0
    receive(0, 0);
    ASSERT_NEXT(V<uint32_t>(42));
    ASSERT_NEXT_STRING("Message C");
    process(0, 0);

    // Message D received on 0:1
    receive(0, 1);
    ASSERT_NEXT_STRING("Message D");
    process(0, 1);

    // Message E received on 0:0
    receive(0, 0);
    process(0, 0);


    // Process messages on node 1

    // Message A received on 1:1
    receive(1, 1);
    ASSERT_NEXT(V<uint64_t>(15));
    ASSERT_NEXT_STRING("Message A");
    ASSERT_NEXT(V<uint64_t>(51));
    process(1, 1);

    // Message B received on 1:0
    receive(1, 0);
    ASSERT_NEXT(V<uint32_t>(67));
    ASSERT_NEXT_STRING("Message B");
    process(1, 0);

    // Message F received on 1:1
    receive(1, 1);
    ASSERT_NEXT_STRING("Message F");
    ASSERT_NEXT(V<uint32_t>(78));
    process(1, 1);

    // Done
    stop();
}

TEST(testPausing) {
    start();

    // Send 10 messages of 4 + 10 characters each.
    for (uint8_t messageIndex = 0; messageIndex < 10; ++messageIndex) {
        startMessage(0);
        std::string buffer("Message X");
        buffer[8] = '0' + messageIndex;
        m_outgoingMessageBuffer.write<uint8_t>(messageIndex);
        m_outgoingMessageBuffer.writeString(buffer.c_str());
        finishMessage();
        ASSERT_SEND(0, 1);
    }

    // Wait to allow all the messages to arrive.
    ::sleepMS(500);

    for (uint8_t messageIndex = 0; messageIndex < 10; ++messageIndex) {
        // Channel 0 should be paused as long as there are messages in the queue.
        ASSERT_TRUE(m_messagePassings[0]->levelPaused(0));
        ASSERT_TRUE(m_messagePassings[1]->levelPaused(0));
        // Read the mssage.
        receive(1, 0);
        ASSERT_NEXT(V<uint8_t>(messageIndex));
        std::string buffer("Message X");
        buffer[8] = '0' + messageIndex;
        ASSERT_NEXT_STRING(buffer.c_str());
        process(1, 0);

    }

    // Wait to allow all the messages to arrive.
    ::sleepMS(500);

    // At this point, the channel 0 should be unpaused.
    ASSERT_FALSE(m_messagePassings[0]->levelPaused(0));
    ASSERT_FALSE(m_messagePassings[1]->levelPaused(0));

    // Done
    stop();
}

#endif
