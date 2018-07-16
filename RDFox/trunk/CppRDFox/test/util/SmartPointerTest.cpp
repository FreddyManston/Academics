// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifdef WITH_TEST

#define SUITE_NAME    SmartPointerTest

#include <CppTest/AutoTest.h>

#include "../../src/util/SmartPointer.h"

class Int : protected Unmovable {

public:

    mutable int32_t m_referenceCount;
    bool& m_notifyDestroyed;
    int m_value;

    Int(bool& notifyDestroyed, int value) : m_referenceCount(0), m_notifyDestroyed(notifyDestroyed), m_value(value) {
        ASSERT_FALSE(m_notifyDestroyed);
    }

    ~Int() {
        m_notifyDestroyed = true;
    }

    always_inline size_t hash() {
        return static_cast<size_t>(m_value);
    }

    friend always_inline bool operator==(const Int& lhs, const Int& rhs) {
        return lhs.m_value == rhs.m_value;
    }

    friend always_inline bool operator!=(const Int& lhs, const Int& rhs) {
        return lhs.m_value != rhs.m_value;
    }

    friend always_inline bool operator<(const Int& lhs, const Int& rhs) {
        return lhs.m_value < rhs.m_value;
    }

    friend always_inline bool operator<=(const Int& lhs, const Int& rhs) {
        return lhs.m_value <= rhs.m_value;
    }

    friend always_inline bool operator>(const Int& lhs, const Int& rhs) {
        return lhs.m_value > rhs.m_value;
    }

    friend always_inline bool operator>=(const Int& lhs, const Int& rhs) {
        return lhs.m_value >= rhs.m_value;
    }

};

typedef SmartPointer<Int> IntPtr;

template class SmartPointer<Int>;

TEST(testBasic) {
    bool d1 = false;
    bool d2 = false;
    bool d3 = false;
    bool d5 = false;
    {
        IntPtr i1(new Int(d1, 1));
        ASSERT_EQUAL(1, i1->m_value);
        ASSERT_EQUAL(1, (*i1).m_value);
        ASSERT_EQUAL(static_cast<size_t>(1), std::hash<IntPtr>()(i1));

        IntPtr i2(new Int(d2, 2));
        ASSERT_TRUE(i1 < i2);
        i2 = i2;
        ASSERT_FALSE(d2);
        ASSERT_TRUE(i2.unique());

        IntPtr i3(new Int(d3, 1));
        ASSERT_TRUE(i1 == i3);

        IntPtr i4;
        ASSERT_EQUAL(static_cast<Int*>(0), i4.get());

        IntPtr i5;
        {
            IntPtr i6(new Int(d5, 7));
            i5 = i6;
            ASSERT_EQUAL(2L, i5.use_count());
            ASSERT_EQUAL(2L, i6.use_count());
        }
        ASSERT_FALSE(d5);
        ASSERT_EQUAL(7, i5->m_value);
        ASSERT_TRUE(i5.unique());
    }
    ASSERT_TRUE(d1);
    ASSERT_TRUE(d2);
    ASSERT_TRUE(d3);
    ASSERT_TRUE(d5);
}

TEST(testInVector) {
    bool d0 = false;
    bool d1 = false;
    {
        std::vector<IntPtr> vector;
        vector.push_back(IntPtr(new Int(d0, 0)));
        vector.push_back(IntPtr(new Int(d1, 1)));

        IntPtr p0 = vector[0];
        ASSERT_EQUAL(2L, p0.use_count());

        vector.push_back(p0);
        IntPtr p2;
        p2 = vector[2];
        ASSERT_TRUE(&p0 != &p2);
        ASSERT_TRUE(p0.get() == p2.get());
        ASSERT_EQUAL(4L, p0.use_count());

        p0 = p0;
        ASSERT_EQUAL(4L, p0.use_count());

        p0 = p2;
        ASSERT_EQUAL(4L, p0.use_count());

        p2.reset();
        ASSERT_EQUAL(3L, p0.use_count());
    }
    ASSERT_TRUE(d0);
    ASSERT_TRUE(d1);
}

TEST(testInUnorderedSet) {
    bool d0 = false;
    bool d1 = false;
    {
        bool d2 = false;
        std::unordered_set<IntPtr> set;
        IntPtr p0(new Int(d0, 0));
        IntPtr p1(new Int(d1, 1));
        set.insert(p0);
        set.insert(p1);
        set.insert(IntPtr(new Int(d2, 0)));
        ASSERT_TRUE(d2);
        ASSERT_EQUAL(static_cast<size_t>(2), set.size());
        ASSERT_EQUAL(0, set.find(p0)->get()->m_value);
        bool d3 = false;
        ASSERT_EQUAL(0, set.find(IntPtr(new Int(d3, 0)))->get()->m_value);
        ASSERT_TRUE(d3);
    }
    ASSERT_TRUE(d0);
    ASSERT_TRUE(d1);
}

#endif
