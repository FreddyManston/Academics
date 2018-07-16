// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef DATAPOOLIMPL_H_
#define DATAPOOLIMPL_H_

#include "../RDFStoreException.h"
#include "../util/ComponentStatistics.h"
#include "DataPool.h"

template<bool is_parallel, typename ALIGNMENT_TYPE>
always_inline uint64_t DataPool::newDataChunk(DictionaryUsageContext* dictionaryUsageContext, const size_t chunkSize, const size_t safetyBuffer) {
    assert(chunkSize != 0);
    if (is_parallel) {
        uint64_t usageContextReservation;
        if (dictionaryUsageContext == 0)
            usageContextReservation = 0;
        else {
            const uint64_t nextFreeDataPoolSegmentAligned = ::alignValue<ALIGNMENT_TYPE>(dictionaryUsageContext->m_nextFreeDataPoolSegment);
            const uint64_t nextFreeDataPoolSegmentEnd = nextFreeDataPoolSegmentAligned + chunkSize;
            if (nextFreeDataPoolSegmentEnd <= dictionaryUsageContext->m_afterLastFreeDataPoolSegment) {
                dictionaryUsageContext->m_nextFreeDataPoolSegment = nextFreeDataPoolSegmentEnd;
                return nextFreeDataPoolSegmentAligned;
            }
            usageContextReservation = USAGE_CONTEXT_WINDOW;
        }
        if (SUPPORTS_UNALIGNED_ACCESS) {
            const uint64_t afterRange = ::atomicAdd(m_nextFreeLocation, chunkSize + usageContextReservation);
            if (!m_data.ensureEndAtLeast(afterRange, safetyBuffer))
                return INVALID_CHUNK_INDEX;
            if (dictionaryUsageContext != 0) {
                dictionaryUsageContext->m_nextFreeDataPoolSegment = afterRange - usageContextReservation;
                dictionaryUsageContext->m_afterLastFreeDataPoolSegment = afterRange;
            }
            return afterRange - chunkSize - usageContextReservation;
        }
        else {
            uint64_t nextFreeLocation;
            uint64_t result;
            uint64_t newNextFreeLocation;
            do {
                nextFreeLocation = ::atomicRead(m_nextFreeLocation);
                result = ::alignValue<ALIGNMENT_TYPE>(nextFreeLocation);
                newNextFreeLocation = result + chunkSize + usageContextReservation;
            } while (!::atomicConditionalSet(m_nextFreeLocation, nextFreeLocation, newNextFreeLocation));
            if (!m_data.ensureEndAtLeast(newNextFreeLocation, safetyBuffer))
                return INVALID_CHUNK_INDEX;
            if (dictionaryUsageContext != 0) {
                dictionaryUsageContext->m_nextFreeDataPoolSegment = result + chunkSize;
                dictionaryUsageContext->m_afterLastFreeDataPoolSegment = newNextFreeLocation;
            }
            return result;
        }
    }
    else {
        const uint64_t result = ::alignValue<ALIGNMENT_TYPE>(m_nextFreeLocation);
        const uint64_t newNextFreeLocation = result + chunkSize;
        if (!m_data.ensureEndAtLeast(newNextFreeLocation, safetyBuffer))
            return INVALID_CHUNK_INDEX;
        m_nextFreeLocation = newNextFreeLocation;
        return result;
    }
}

always_inline uint8_t* DataPool::getDataFor(const uint64_t chunkIndex) {
    return m_data + chunkIndex;
}

always_inline const uint8_t* DataPool::getDataFor(const uint64_t chunkIndex) const {
    return m_data + chunkIndex;
}

#endif /* DATAPOOLIMPL_H_ */
