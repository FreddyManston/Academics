// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef MESSAGEPASSINGIMPL_H_
#define MESSAGEPASSINGIMPL_H_

#include "../util/Thread.h"
#include "MessageBufferImpl.h"
#include "MessagePassing.h"

// MessagePassingThread

template<class Derived>
class MessagePassingThread : public Thread {

protected:

    MessagePassing<Derived>& m_messagePassing;
    const Duration m_startupTime;

public:

    MessagePassingThread(MessagePassing<Derived>& messagePassing, const Duration startupTime) : Thread(true), m_messagePassing(messagePassing), m_startupTime(startupTime) {
    }

    virtual void run() {
        m_messagePassing.run(m_startupTime);
    }
    
};

// MessagePassing::Channel

template<class Derived>
always_inline bool MessagePassing<Derived>::Channel::sendDataOnSocket(const uint8_t* data, size_t size) {
    MutexHolder socketMutexHolder(m_socketMutex);
    if (!m_sendingSocket.isValid())
        return false;
    while (size != 0) {
        m_sendingSocketSelector.clear();
        m_sendingSocketSelector.monitor(m_sendingSocket, false, true, false);
        const SocketSelector::SelectResult selectResult = m_sendingSocketSelector.select();
        if (selectResult == SocketSelector::INTERRUPTED)
            return false;
        else if (selectResult == SocketSelector::SOCKET_READY && m_sendingSocketSelector.canWrite(m_sendingSocket)) {
            bool wouldBlock;
            const size_t result = m_sendingSocket.write(data, size, wouldBlock);
            if (!wouldBlock) {
                data += result;
                size -= result;
            }
        }
    }
    return true;
}

template<class Derived>
always_inline static std::string getDefaultPortNumber(const ChannelID channelID) {
    std::ostringstream buffer;
    buffer << (13000 + channelID);
    return buffer.str();
}

template<class Derived>
always_inline MessagePassing<Derived>::Channel::Channel(const ChannelID channelID, Socket* sendingSocket, MemoryManager& memoryManager) :
    m_channelID(channelID),
    m_nodeName("localhost"),
    m_serviceName(std::to_string(12000 + channelID)),
    m_socketMutex(),
    m_socket(),
    m_sendingSocket(sendingSocket == nullptr ? m_socket : *sendingSocket),
    m_sendingSocketSelector(),
    m_incomingDataBuffer(memoryManager),
    m_incomingDataSize(0)
{
}

template<class Derived>
always_inline MessagePassing<Derived>::Channel::~Channel() {
}

template<class Derived>
always_inline void MessagePassing<Derived>::Channel::initialize() {
    MutexHolder mutexHolder(m_socketMutex);
    if (!m_incomingDataBuffer.initialize(INCOMING_DATA_BUFFER_SIZE) || !m_incomingDataBuffer.ensureEndAtLeast(INCOMING_DATA_BUFFER_SIZE, 0))
        throw RDF_STORE_EXCEPTION("Out of memory while initializing the incoming data buffer.");
    m_incomingDataSize = 0;
    m_sendingSocketSelector.enableInterrupt();
}

template<class Derived>
always_inline void MessagePassing<Derived>::Channel::requestInterrupt() {
    // Exceptions are deliberately ignored as otherwise they would propagate out of destructors.
    try {
        m_sendingSocketSelector.interrupt();
    }
    catch (const RDFStoreException&) {
    }
    try {
        m_socket.shutdown(Socket::READWRITE);
    }
    catch (const RDFStoreException&) {
    }
}

template<class Derived>
always_inline void MessagePassing<Derived>::Channel::deinitialize() {
    MutexHolder mutexHolder(m_socketMutex);
    m_sendingSocketSelector.disableInterrupt();
    m_socket.close();
    m_incomingDataBuffer.deinitialize();
}

// MessagePassing::IncomingDataInfo

template<class Derived>
always_inline MessagePassing<Derived>::IncomingDataInfo::IncomingDataInfo(MemoryManager& memoryManager, const size_t bufferSize, const uint32_t level) :
    m_messageBuffer(memoryManager, bufferSize),
    m_level(level),
    m_numberOfPausedPeers(0),
    m_peersPaused(false),
    m_nextPausedIncomingDataInfo(nullptr),
    m_previousPausedIncomingDataInfo(nullptr)
{
}

template<class Derived>
always_inline void MessagePassing<Derived>::IncomingDataInfo::initialize() {
    m_messageBuffer.initialize();
    m_numberOfPausedPeers = 0;
    m_peersPaused = false;
    m_nextPausedIncomingDataInfo = nullptr;
    m_previousPausedIncomingDataInfo = nullptr;
}

template<class Derived>
always_inline void MessagePassing<Derived>::IncomingDataInfo::deinitialize() {
    m_messageBuffer.deinitialize();
}

// MessagePassing

template<class Derived>
always_inline bool MessagePassing<Derived>::levelPaused(const uint32_t level) {
    return ::atomicRead(m_incomingDataInfos[level]->m_numberOfPausedPeers) > 0;
}

template<class Derived>
always_inline void MessagePassing<Derived>::configureSocket(Socket& socket) {
    socket.setSendBufferSize(50000);
    socket.setReceiveBufferSize(50000);
    socket.makeNonBlocking(true);
    socket.setKeepAlive(true);
    socket.setTCPNoDelay(true);
}

template<class Derived>
always_inline bool MessagePassing<Derived>::startNoLock(const Duration startupTime) {
    if (m_state == NOT_RUNNING) {
        try {
            // Initialization is performed here so that it is certainly done before the server thread start.
            m_lastError = nullptr;
            m_firstPausedIncomingDataInfo = nullptr;
            m_socketSelector.enableInterrupt();
            for (auto iterator = m_channels.begin(); iterator != m_channels.end(); ++iterator)
                (*iterator)->initialize();
            for (auto iterator = m_incomingDataInfos.begin(); iterator != m_incomingDataInfos.end(); ++iterator)
                (*iterator)->initialize();
            m_state = CONNECTING;
            (new MessagePassingThread<Derived>(*this, startupTime))->start();
            return true;
        }
        catch (...) {
            // If any of the initialization operations fail, we ensure that the entire object remains uninitialized.
            m_firstPausedIncomingDataInfo = nullptr;
            m_socketSelector.disableInterrupt();
            for (auto iterator = m_channels.begin(); iterator != m_channels.end(); ++iterator)
                (*iterator)->deinitialize();
            for (auto iterator = m_incomingDataInfos.begin(); iterator != m_incomingDataInfos.end(); ++iterator)
                (*iterator)->deinitialize();
            m_selfSendingSocket.close();
            m_state = NOT_RUNNING;
            throw;
        }
    }
    else
        return false;
}

template<class Derived>
always_inline std::exception_ptr MessagePassing<Derived>::stopNoLock() {
    if (m_state == NOT_RUNNING)
        return nullptr;
    else {
        m_state = STOPPING;
        try {
            m_socketSelector.interrupt();
        }
        catch (const RDFStoreException&) {
        }
        for (auto iterator = m_channels.begin(); iterator != m_channels.end(); ++iterator)
            (*iterator)->requestInterrupt();
        m_condition.signalAll();
        while (m_state != NOT_RUNNING)
            m_condition.wait(m_mutex);
        return m_lastError;
    }
}

template<class Derived>
always_inline bool MessagePassing<Derived>::waitForReadyNoLock() {
    while (m_state == CONNECTING)
        m_condition.wait(m_mutex);
    return m_state == RUNNING;
}

template<class Derived>
always_inline bool MessagePassing<Derived>::establishConnections(const Duration startupTime) {
    struct AcceptingSocket : private Unmovable {

        enum ProcessResult { NOT_FINISHED, FINISHED, CLOSED };

        Socket m_socket;
        ChannelID m_remoteChannelID;
        uint32_t m_remoteChannelIDRead;

        AcceptingSocket(Socket& listener) : m_socket(), m_remoteChannelID(INVALID_CHANNEL_ID), m_remoteChannelIDRead(0) {
            m_socket.accept(listener);
            configureSocket(m_socket);
        }

        always_inline ProcessResult process(SocketSelector& socketSelector) {
            if (socketSelector.canRead(m_socket)) {
                bool wouldBlock;
                const size_t bytesRead = m_socket.read(reinterpret_cast<uint8_t*>(&m_remoteChannelID) + m_remoteChannelIDRead, sizeof(ChannelID) - m_remoteChannelIDRead, wouldBlock);
                if (!wouldBlock) {
                    if (bytesRead == 0)
                        return CLOSED;
                    else {
                        m_remoteChannelIDRead += static_cast<uint32_t>(bytesRead);
                        return m_remoteChannelIDRead == sizeof(ChannelID) ? FINISHED : NOT_FINISHED;
                    }
                }
            }
            return NOT_FINISHED;
        }

    };

    enum ConnectingSocketState { NO_CONNECTION, CONNECTING, WRITING_ID };

    struct ConnectingSocket : private Unmovable {
        const ChannelID m_myChannelID;
        const ChannelID m_channelID;
        const std::string m_nodeName;
        const std::string m_serviceName;
        Socket m_socket;
        SocketAddress m_socketAddress;
        uint32_t m_numberOfAddresses;
        std::vector<std::exception_ptr> m_connectExceptions;
        ConnectingSocketState m_state;
        uint32_t m_myChannelIDWritten;

        ConnectingSocket(const ChannelID myChannelID, ChannelID channelID, const std::string& nodeName, const std::string& serviceName) :
            m_myChannelID(myChannelID),
            m_channelID(channelID),
            m_nodeName(nodeName),
            m_serviceName(serviceName),
            m_socket(),
            m_socketAddress(),
            m_numberOfAddresses(0),
            m_state(NO_CONNECTION),
            m_myChannelIDWritten(0)
        {
        }

        void resolveAddress() {
            m_socketAddress.open(SocketAddress::IP_V6_TRY_FIRST, false, m_nodeName.c_str(), m_serviceName.c_str());
            if (!m_socketAddress.isValid()) {
                std::ostringstream message;
                message << "Cannot resolve the address to node name '" << m_nodeName << "' and service name '" << m_serviceName << "'.";
                throw RDF_STORE_EXCEPTION_WITH_CAUSES(message.str(), m_connectExceptions);
            }
            m_numberOfAddresses = m_socketAddress.getNumberOfAddresses();
        }

        always_inline void initiateConnection() {
            assert(m_state == NO_CONNECTION);
            m_socket.create(m_socketAddress);
            configureSocket(m_socket);
            try {
                if (m_socket.connect(m_socketAddress)) {
                    m_state = WRITING_ID;
                    m_myChannelIDWritten = 0;
                }
                else
                    m_state = CONNECTING;
            }
            catch (const RDFStoreException&) {
                while (m_connectExceptions.size() >= m_numberOfAddresses)
                    m_connectExceptions.erase(m_connectExceptions.begin());
                m_connectExceptions.push_back(std::current_exception());
                m_socketAddress.nextAddress();
                if (!m_socketAddress.isValid())
                    m_socketAddress.resetToFirstAddress();
            }
        }

        always_inline bool process(SocketSelector& socketSelector) {
            if (socketSelector.canWrite(m_socket)) {
                if (m_state == CONNECTING) {
                    try {
                        m_socket.ensureNoError();
                        m_state = WRITING_ID;
                        m_myChannelIDWritten = 0;
                    }
                    catch (const RDFStoreException&) {
                        while (m_connectExceptions.size() >= m_numberOfAddresses)
                            m_connectExceptions.erase(m_connectExceptions.begin());
                        m_connectExceptions.push_back(std::current_exception());
                        m_state = NO_CONNECTION;
                        m_socketAddress.nextAddress();
                        if (!m_socketAddress.isValid())
                            m_socketAddress.resetToFirstAddress();
                    }
                }
                else if (m_state == WRITING_ID) {
                    bool wouldBlock;
                    const size_t bytesWritten = m_socket.write(reinterpret_cast<const uint8_t*>(&m_myChannelID) + m_myChannelIDWritten, sizeof(ChannelID) - m_myChannelIDWritten, wouldBlock);
                    if (!wouldBlock) {
                        m_myChannelIDWritten += static_cast<uint32_t>(bytesWritten);
                        return m_myChannelIDWritten == sizeof(ChannelID);
                    }
                }
            }
            return false;
        }

    };

    const TimePoint startupMustEndBefore = ::getTimePoint() + startupTime;
    unique_ptr_vector<ConnectingSocket> connectingSockets;
    unique_ptr_vector<AcceptingSocket> acceptingSockets;
    size_t numberOfRequiredConnections = 0;
    std::string myServiceName;

    // Initialization is done under a lock so that nobody can change server configuration.
    // The lock is released afterwards so that the process can be stopped.
    {
        MutexHolder mutexHolder(m_mutex);
        for (auto iterator = m_channels.begin(); iterator != m_channels.end(); ++iterator) {
            Channel& channel = **iterator;
            if (m_myChannelID > channel.m_channelID)
                connectingSockets.push_back(std::unique_ptr<ConnectingSocket>(new ConnectingSocket(m_myChannelID, channel.m_channelID, channel.m_nodeName, channel.m_serviceName)));
            if (channel.m_channelID != m_myChannelID)
                ++numberOfRequiredConnections;
        }
        myServiceName = m_channels[m_myChannelID]->m_serviceName;
    }

    // Address resultion is done outside the lock so that we hold the lock for a minimum amount of time.
    for (auto iterator = connectingSockets.begin(); iterator != connectingSockets.end(); ++iterator)
        (*iterator)->resolveAddress();

    // Open the listening socket.
    SocketAddress myAddress;
    myAddress.open(SocketAddress::IP_V6_TRY_FIRST, true, nullptr, myServiceName.c_str());
    Socket listener;
    std::vector<std::exception_ptr> listenerCauses;
    while (myAddress.isValid()) {
        try {
            listener.create(myAddress);
            listener.setSendBufferSize(50000);
            listener.setReceiveBufferSize(50000);
            listener.setReuseAddress(true);
            listener.makeNonBlocking(true);
            if (myAddress.isIPv6())
                listener.setDualStack(true);
            listener.listen(myAddress, m_numberOfChannels);
            break;
        }
        catch (const RDFStoreException&) {
            listenerCauses.push_back(std::current_exception());
        }
        myAddress.nextAddress();
    }
    if (!listener.isValid())
        throw RDF_STORE_EXCEPTION_WITH_CAUSES("Cannot start the listener.", listenerCauses);

    // Connect the self-socket first
    m_channels[m_myChannelID]->m_socket.connectLocally(m_selfSendingSocket);
    configureSocket(m_channels[m_myChannelID]->m_socket);
    configureSocket(m_selfSendingSocket);

    // Now try to actually connect
    size_t numberOfEstablishedConnections = 0;
    while (numberOfEstablishedConnections < numberOfRequiredConnections) {
        m_socketSelector.clear();
        m_socketSelector.monitor(listener, true, false, false);
        for (auto iterator = connectingSockets.begin(); iterator != connectingSockets.end(); ++iterator) {
            ConnectingSocket& connectingSocket = **iterator;
            if (connectingSocket.m_state == NO_CONNECTION)
                connectingSocket.initiateConnection();
            if (connectingSocket.m_state == CONNECTING || connectingSocket.m_state == WRITING_ID)
                m_socketSelector.monitor(connectingSocket.m_socket, false, true, false);
        }
        for (auto iterator = acceptingSockets.begin(); iterator != acceptingSockets.end(); ++iterator)
            m_socketSelector.monitor((*iterator)->m_socket, true, false, false);
        const TimePoint current = ::getTimePoint();
        if (current >= startupMustEndBefore)
            throw RDF_STORE_EXCEPTION("Startup timeout exceeded.");
        Duration timeLeft = startupMustEndBefore - current;
        if (500 < timeLeft)
            timeLeft = 500;
        const SocketSelector::SelectResult result = m_socketSelector.select(timeLeft);
        if (result == SocketSelector::INTERRUPTED)
            return false;
        else if (result == SocketSelector::SOCKET_READY) {
            if (m_socketSelector.canRead(listener))
                acceptingSockets.push_back(std::unique_ptr<AcceptingSocket>(new AcceptingSocket(listener)));
            for (auto iterator = connectingSockets.begin(); iterator != connectingSockets.end();) {
                ConnectingSocket& connectingSocket = **iterator;
                if (connectingSocket.process(m_socketSelector)) {
                    Channel& channel = *m_channels[connectingSocket.m_channelID];
                    channel.m_socketMutex.lock();
                    channel.m_socket = std::move(connectingSocket.m_socket);
                    channel.m_socketMutex.unlock();
                    iterator = connectingSockets.erase(iterator);
                    ++numberOfEstablishedConnections;
                }
                else
                    ++iterator;
            }
            for (auto iterator = acceptingSockets.begin(); iterator != acceptingSockets.end();) {
                AcceptingSocket& acceptingSocket = **iterator;
                typename AcceptingSocket::ProcessResult result = acceptingSocket.process(m_socketSelector);
                if (result == AcceptingSocket::FINISHED) {
                    if (acceptingSocket.m_remoteChannelID >= m_numberOfChannels)
                        throw RDF_STORE_EXCEPTION("Remote sent an incorrect channel ID.");
                    Channel& remoteChannel = *m_channels[acceptingSocket.m_remoteChannelID];
                    remoteChannel.m_socketMutex.lock();
                    remoteChannel.m_socket = std::move(acceptingSocket.m_socket);
                    remoteChannel.m_socketMutex.unlock();
                    iterator = acceptingSockets.erase(iterator);
                    ++numberOfEstablishedConnections;
                }
                else if (result == AcceptingSocket::CLOSED)
                    iterator = acceptingSockets.erase(iterator);
                else
                    ++iterator;
            }
        }
    }
    return true;
}

template<class Derived>
always_inline void MessagePassing<Derived>::reclaimProcessedData(IncomingDataInfo& incomingDataInfo) {
    if (incomingDataInfo.m_messageBuffer.reclaimProcessedSpace()) {
        if (incomingDataInfo.m_peersPaused) {
            incomingDataInfo.m_peersPaused = false;
            if (m_firstPausedIncomingDataInfo == &incomingDataInfo)
                m_firstPausedIncomingDataInfo = incomingDataInfo.m_nextPausedIncomingDataInfo;
            if (incomingDataInfo.m_nextPausedIncomingDataInfo != nullptr)
                incomingDataInfo.m_nextPausedIncomingDataInfo->m_previousPausedIncomingDataInfo = incomingDataInfo.m_previousPausedIncomingDataInfo;
            if (incomingDataInfo.m_previousPausedIncomingDataInfo != nullptr)
                incomingDataInfo.m_previousPausedIncomingDataInfo->m_nextPausedIncomingDataInfo = incomingDataInfo.m_nextPausedIncomingDataInfo;
            incomingDataInfo.m_previousPausedIncomingDataInfo = incomingDataInfo.m_nextPausedIncomingDataInfo = nullptr;
            const MessageSize controlMessage = (CONTROL_MESSAGE_FLAG | CONTROL_MESSAGE_RESUME) | incomingDataInfo.m_level;
            for (auto channelIterator = m_channels.begin(); channelIterator != m_channels.end(); ++channelIterator)
                (*channelIterator)->sendDataOnSocket(reinterpret_cast<const uint8_t*>(&controlMessage), sizeof(MessageSize));
        }
    }
}

template<class Derived>
always_inline void MessagePassing<Derived>::run(const Duration startupTime) {
    static_cast<Derived*>(this)->messagePassingStarting();
    std::exception_ptr lastError = nullptr;
    try {
        if (establishConnections(startupTime)) {
            m_mutex.lock();
            m_state = RUNNING;
            m_condition.signalAll();
            m_mutex.unlock();
            static_cast<Derived*>(this)->messagePassingConnected();
            bool running = true;
            size_t numberOfReceivedMessages;
            while (running) {
                for (IncomingDataInfo* incomingDataInfo = m_firstPausedIncomingDataInfo; incomingDataInfo != nullptr; ) {
                    IncomingDataInfo* nextIncomingDataInfo = incomingDataInfo->m_nextPausedIncomingDataInfo;
                    reclaimProcessedData(*incomingDataInfo);
                    incomingDataInfo = nextIncomingDataInfo;
                }
                m_socketSelector.clear();
                for (auto iterator = m_channels.begin(); iterator != m_channels.end(); ++iterator)
                    m_socketSelector.monitor((*iterator)->m_socket, true, false, false);
                const SocketSelector::SelectResult result = m_socketSelector.select();
                switch (result) {
                case SocketSelector::SOCKET_READY:
                    numberOfReceivedMessages = 0;
                    for (auto iterator = m_channels.begin(); running && iterator != m_channels.end(); ++iterator) {
                        Channel* channel = iterator->get();
                        if (m_socketSelector.canRead(channel->m_socket)) {
                            bool wouldBlock;
                            const size_t actuallyRead = channel->m_socket.read(channel->m_incomingDataBuffer.getData() + channel->m_incomingDataSize, INCOMING_DATA_BUFFER_SIZE - channel->m_incomingDataSize, wouldBlock);
                            if (!wouldBlock) {
                                if (actuallyRead == 0)
                                    running = false;
                                else {
                                    const uint8_t* currentMessageStart = channel->m_incomingDataBuffer.getData();
                                    const uint8_t* const afterLast = currentMessageStart + channel->m_incomingDataSize + actuallyRead;
                                    const uint8_t* messagePayloadStart;
                                    while ((messagePayloadStart = currentMessageStart + sizeof(MessageSize)) <= afterLast) {
                                        const MessageSize messageSize = *reinterpret_cast<const MessageSize*>(currentMessageStart);
                                        if ((messageSize & CONTROL_MESSAGE_FLAG) == CONTROL_MESSAGE_FLAG) {
                                            const uint32_t messageBufferIndex = static_cast<uint32_t>(messageSize & CONTROL_MESSAGE_PAYLOAD);
                                            if ((messageSize & CONTROL_MESSAGE_PAUSE) == CONTROL_MESSAGE_PAUSE)
                                                ::atomicIncrement(m_incomingDataInfos[messageBufferIndex]->m_numberOfPausedPeers);
                                            else {
                                                if (::atomicDecrement(m_incomingDataInfos[messageBufferIndex]->m_numberOfPausedPeers) == 0) {
                                                    m_mutex.lock();
                                                    m_condition.signalAll();
                                                    m_mutex.unlock();
                                                }
                                            }
                                            currentMessageStart += sizeof(MessageSize);
                                        }
                                        else if (currentMessageStart + messageSize > afterLast)
                                            break;
                                        else {
                                            const uint32_t messageBufferIndex = static_cast<Derived*>(this)->getMessageBufferIndex(messagePayloadStart);
                                            assert(messageBufferIndex < m_incomingDataInfos.size());
                                            IncomingDataInfo& incomingDataInfo = *m_incomingDataInfos[messageBufferIndex];
                                            reclaimProcessedData(incomingDataInfo);
                                            MessageBuffer& incomingMessageBuffer = incomingDataInfo.m_messageBuffer;
                                            incomingMessageBuffer.appendData(currentMessageStart, messageSize);
                                            if (incomingMessageBuffer.isOverLimit() && !incomingDataInfo.m_peersPaused) {
                                                incomingDataInfo.m_peersPaused = true;
                                                incomingDataInfo.m_previousPausedIncomingDataInfo = m_firstPausedIncomingDataInfo;
                                                incomingDataInfo.m_nextPausedIncomingDataInfo = nullptr;
                                                m_firstPausedIncomingDataInfo = &incomingDataInfo;
                                                const MessageSize controlMessage = (CONTROL_MESSAGE_FLAG | CONTROL_MESSAGE_PAUSE) | messageBufferIndex;
                                                for (auto channelIterator = m_channels.begin(); channelIterator != m_channels.end(); ++channelIterator)
                                                    (*channelIterator)->sendDataOnSocket(reinterpret_cast<const uint8_t*>(&controlMessage), sizeof(MessageSize));
                                            }
                                            currentMessageStart += messageSize;
                                            ++numberOfReceivedMessages;
                                        }
                                    }
                                    channel->m_incomingDataSize = afterLast - currentMessageStart;
                                    std::memmove(channel->m_incomingDataBuffer.getData(), currentMessageStart, channel->m_incomingDataSize);
                                }
                            }
                        }
                    }
                    if (numberOfReceivedMessages != 0) {
                        m_mutex.lock();
                        if (numberOfReceivedMessages == 1)
                            m_condition.signalOne();
                        else
                            m_condition.signalAll();
                        m_mutex.unlock();
                    }
                    break;
                case SocketSelector::INTERRUPTED:
                    m_mutex.lock();
                    if (m_state != RUNNING)
                        running = false;
                    m_mutex.unlock();
                    break;
                case SocketSelector::TIMEOUT:
                    break;
                }
            }
            static_cast<Derived*>(this)->messagePassingStopping();
        }
    }
    catch (const std::exception& error) {
        // The mutex is not held at this point so we memorise the error in a local variable and set it later after we acquite the mutex.
        lastError = std::current_exception();
        static_cast<Derived*>(this)->messagePassingError(error);
    }
    catch (...) {
        // The mutex is not held at this point so we memorise the error in a local variable and set it later after we acquite the mutex.
        lastError = std::current_exception();
    }
    // Cleanup under a lock.
    MutexHolder mutexHolder(m_mutex);
    for (auto iterator = m_channels.begin(); iterator != m_channels.end(); ++iterator)
        (*iterator)->requestInterrupt();
    static_cast<Derived*>(this)->messagePassingChannelsStopped();
    for (auto iterator = m_channels.begin(); iterator != m_channels.end(); ++iterator)
        (*iterator)->deinitialize();
    for (auto iterator = m_incomingDataInfos.begin(); iterator != m_incomingDataInfos.end(); ++iterator)
        (*iterator)->deinitialize();
    m_selfSendingSocket.close();
    m_socketSelector.disableInterrupt();
    m_state = NOT_RUNNING;
    m_lastError = lastError;
    m_condition.signalAll();
}

template<class Derived>
always_inline MessagePassing<Derived>::MessagePassing(MemoryManager& memoryManager, const uint8_t numberOfChannels, const ChannelID myChannelID, const uint32_t numberOfIncomingMessageBuffers, const size_t totalSizePerMessageBuffer) :
    m_memoryManager(memoryManager),
    m_mutex(),
    m_condition(),
    m_channels(),
    m_numberOfChannels(numberOfChannels),
    m_myChannelID(myChannelID),
    m_incomingDataInfos(),
    m_firstPausedIncomingDataInfo(nullptr),
    m_selfSendingSocket(),
    m_socketSelector(),
    m_state(NOT_RUNNING),
    m_lastError(nullptr)
{
    if (m_myChannelID >= m_numberOfChannels)
        throw RDF_STORE_EXCEPTION("The ID of this channel cannot be larger or equal to the number of channels.");
    for (ChannelID channelID = 0; channelID < numberOfChannels; ++channelID)
        m_channels.push_back(std::unique_ptr<Channel>(new Channel(channelID, channelID == m_myChannelID ? &m_selfSendingSocket : nullptr, memoryManager)));
    for (uint32_t levelIndex = 0; levelIndex < numberOfIncomingMessageBuffers; ++levelIndex)
        m_incomingDataInfos.push_back(std::unique_ptr<IncomingDataInfo>(new IncomingDataInfo(m_memoryManager, totalSizePerMessageBuffer, levelIndex)));
}

template<class Derived>
always_inline MessagePassing<Derived>::~MessagePassing() {
}

template<class Derived>
always_inline bool MessagePassing<Derived>::sendData(const ChannelID channelID, const uint8_t* const data, const size_t size) {
    return m_channels[channelID]->sendDataOnSocket(data, size);
}

template<class Derived>
always_inline const std::string& MessagePassing<Derived>::getNodeName(const ChannelID channelID) const {
    MutexHolder mutexHolder(m_mutex);
    return m_channels[channelID]->m_nodeName;
}

template<class Derived>
always_inline const std::string& MessagePassing<Derived>::getServiceName(const ChannelID channelID) const {
    MutexHolder mutexHolder(m_mutex);
    return m_channels[channelID]->m_serviceName;
}

template<class Derived>
always_inline void MessagePassing<Derived>::setChannelNodeServiceName(const ChannelID channelID, const std::string& nodeName, const std::string& serviceName) {
    MutexHolder mutexHolder(m_mutex);
    m_channels[channelID]->m_nodeName = nodeName;
    m_channels[channelID]->m_serviceName = serviceName;
}

template<class Derived>
always_inline bool MessagePassing<Derived>::start(const Duration startupTime) {
    MutexHolder mutexHolder(m_mutex);
    return startNoLock(startupTime);
}

template<class Derived>
always_inline std::exception_ptr MessagePassing<Derived>::stop() {
    MutexHolder mutexHolder(m_mutex);
    return stopNoLock();
}

template<class Derived>
always_inline bool MessagePassing<Derived>::waitForReady() {
    MutexHolder mutexHolder(m_mutex);
    return waitForReadyNoLock();
}

#endif // MESSAGEPASSINGIMPL_H_
