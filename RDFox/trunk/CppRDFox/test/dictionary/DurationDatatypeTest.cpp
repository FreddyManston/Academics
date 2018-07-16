// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifdef WITH_TEST

#define AUTO_TEST    DurationDatatypeTest

#include <CppTest/AutoTest.h>

#include "AbstractDatatypeTest.h"

class DurationDatatypeTest : public AbstractDatatypeTest {

public:

    virtual DatatypeID getDefaultDatatypeID() {
        return D_XSD_DURATION;
    }

};

TEST(testBasic) {
    checkValue("P", D_XSD_DURATION, 6);
    checkValue("P1Y2M3DT4H22M10.300S", D_XSD_DURATION, 7);
    checkValue("P1Y3M", D_XSD_DURATION, 8);
    checkValue("-PT3H10M5.563S", D_XSD_DURATION, 9);
}

struct DurationValueMaker {
    always_inline void makeValue(const size_t valueIndex, std::string& lexicalForm, DatatypeID& datatypeID) const {
        std::ostringstream stream;
        stream << "P" << (valueIndex + 1) << "Y";
        lexicalForm = stream.str();
        datatypeID = D_XSD_DURATION;
    }
};

TEST(testParallel) {
    runParallelTest(100000, 4, DurationValueMaker());
}

#endif
