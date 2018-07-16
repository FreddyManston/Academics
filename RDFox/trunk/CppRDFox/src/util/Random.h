// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef RANDOM_H_
#define RANDOM_H_

#include "../all.h"
#include "../RDFStoreException.h"

class Random {
    
protected:
    
    uint8_t m_step;
    uint64_t m_matrix[16];
    
    uint64_t xorshift64star(uint64_t x) {
        x ^= x >> 12; // a
        x ^= x << 25; // b
        x ^= x >> 27; // c
        return x * static_cast<uint64_t>(2685821657736338717);
    }
    
public:
    
    Random(const uint64_t seed) : m_step(0) {
        if (seed == 0)
            throw RDF_STORE_EXCEPTION("Random requires a non-zero seed");
        m_matrix[0] = seed;
        for (int index = 0; index < 15; index++)
            m_matrix[index + 1] = xorshift64star(m_matrix[index]);
    }

    uint64_t next(void) {
        uint64_t s0 = m_matrix[m_step];
        uint64_t s1 = m_matrix[m_step = (m_step + 1) & 15];
        s1 ^= s1 << 31; // a
        s1 ^= s1 >> 11; // b
        s0 ^= s0 >> 30; // c
        return (m_matrix[m_step] = s0 ^ s1) * static_cast<uint64_t>(1181783497276652981);
    }
    
};

#endif /* RANDOM_H_ */
