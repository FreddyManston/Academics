// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef MESSAGEPASSING_H_
#define MESSAGEPASSING_H_

#include "../Common.h"
#include "../util/Mutex.h"
#include "../util/Condition.h"
#include "../util/Socket.h"
#include "MessageBuffer.h"

class MemoryManager;
template<class Derived>
class MessagePassingThread;

typedef uint8_t ChannelID;
const ChannelID INVALID_CHANNEL_ID = static_cast<ChannelID>(-1);

const MessageSize CONTROL_MESSAGE_FLAG = 0x80000000;
const MessageSize CONTROL_MESSAGE_MASK = 0x7fffffff;
const MessageSize CONTROL_MESSAGE_PAUSE = 0x40000000;
const MessageSize CONTROL_MESSAGE_RESUME = 0;
const MessageSize CONTROL_MESSAGE_PAYLOAD = 0x00ffffff;

// MessagePassing

template<class Derived>
class MessagePassing : private Unmovable {

    friend class MessagePassingThread<Derived>;

public:

    enum State { STOPPING, NOT_RUNNING, CONNECTING, RUNNING };

    typedef MessagePassing<Derived> MessagePassingType;

protected:

    static const size_t INCOMING_DATA_BUFFER_SIZE = 1024 * 1024;

    struct Channel : private Unmovable {
        const ChannelID m_channelID;
        std::string m_nodeName;
        std::string m_serviceName;
        Mutex m_socketMutex;
        Socket m_socket;
        Socket& m_sendingSocket;
        SocketSelector m_sendingSocketSelector;
        MemoryRegion<uint8_t> m_incomingDataBuffer;
        size_t m_incomingDataSize;

        Channel(const ChannelID channelID, Socket* sendingSocket, MemoryManager& memoryManager);

        ~Channel();

        void initialize();

        void requestInterrupt();

        void deinitialize();

        bool sendDataOnSocket(const uint8_t* data, size_t size);

    };

public:

    struct IncomingDataInfo {
        MessageBuffer m_messageBuffer;
        const uint32_t m_level;
        aligned_uint32_t m_numberOfPausedPeers;
        bool m_peersPaused;
        IncomingDataInfo* m_nextPausedIncomingDataInfo;
        IncomingDataInfo* m_previousPausedIncomingDataInfo;

        IncomingDataInfo(MemoryManager& memoryManager, const size_t bufferSize, const uint32_t level);

        void initialize();

        void deinitialize();

        __ALIGNED(IncomingDataInfo)

    };

protected:

    MemoryManager& m_memoryManager;
    mutable Mutex m_mutex;
    Condition m_condition;
    unique_ptr_vector<Channel> m_channels;
    const uint8_t m_numberOfChannels;
    const ChannelID m_myChannelID;
    unique_ptr_vector<IncomingDataInfo> m_incomingDataInfos;
    IncomingDataInfo* m_firstPausedIncomingDataInfo;
    Socket m_selfSendingSocket;
    SocketSelector m_socketSelector;
    State m_state;
    std::exception_ptr m_lastError;

    bool levelPaused(const uint32_t level);

    static void configureSocket(Socket& socket);

    bool startNoLock(const Duration startupTime);

    std::exception_ptr stopNoLock();

    bool waitForReadyNoLock();

    bool establishConnections(const Duration startupTime);

    void reclaimProcessedData(IncomingDataInfo& incomingDataInfo);

    void run(const Duration startupTime);

public:

    MessagePassing(MemoryManager& memoryManager, const uint8_t numberOfChannels, const ChannelID myChannelID, const uint32_t numberOfIncomingMessageBuffers, const size_t totalSizePerMessageBuffer);

    ~MessagePassing();

    always_inline uint8_t getNumberOfChannels() const {
        return m_numberOfChannels;
    }

    always_inline ChannelID getMyChannelID() const {
        return m_myChannelID;
    }

    always_inline State getState() const {
        MutexHolder mutexHolder(m_mutex);
        return m_state;
    }

    always_inline std::exception_ptr getLastError() const {
        MutexHolder mutexHolder(m_mutex);
        return m_lastError;
    }

    const std::string& getNodeName(const ChannelID channelID) const;

    const std::string& getServiceName(const ChannelID channelID) const;

    void setChannelNodeServiceName(const ChannelID channelID, const std::string& nodeName, const std::string& serviceName);

    bool sendData(const ChannelID channelID, const uint8_t* const data, const size_t size);

    bool start(const Duration startupTime);

    std::exception_ptr stop();

    bool waitForReady();

};

#endif // MESSAGEPASSING_H_
