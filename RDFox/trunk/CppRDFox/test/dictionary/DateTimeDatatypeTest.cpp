// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifdef WITH_TEST

#define AUTO_TEST    DateTimeDatatypeTest

#include <CppTest/AutoTest.h>

#include "AbstractDatatypeTest.h"

class DateTimeDatatypeTest : public AbstractDatatypeTest {

public:

    virtual DatatypeID getDefaultDatatypeID() {
        return D_INVALID_DATATYPE_ID;
    }

};

TEST(testBasic) {
    checkValue("1965-12-04T13:23:45", D_XSD_DATE_TIME, 6);
    checkValue("1965-12-04T13:23:45Z", D_XSD_DATE_TIME, 7);
    checkValue("13:23:45Z", D_XSD_TIME, 8);
    checkValue("1965-12-04", D_XSD_DATE, 9);
    checkValue("1965-12", D_XSD_G_YEAR_MONTH, 10);
    checkValue("1965", D_XSD_G_YEAR, 11);
    checkValue("--10-16", D_XSD_G_MONTH_DAY, 12);
    checkValue("---16", D_XSD_G_DAY, 13);
}

struct DateTimeValueMaker {
    always_inline void makeValue(const size_t valueIndex, std::string& lexicalForm, DatatypeID& datatypeID) const {
        std::ostringstream stream;
        stream.width(4);
        stream.fill('0');
        stream << (valueIndex + 1) << "-01-01T12:00:00Z";
        lexicalForm = stream.str();
        datatypeID = D_XSD_DATE_TIME;
    }
};

TEST(testParallel) {
    runParallelTest(100000, 4, DateTimeValueMaker());
}

#endif
