// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "../util/XSDDateTime.h"
#include "../util/XSDDuration.h"
#include "CommonBuiltinExpressionEvaluators.h"

#define UNARY_DATE_TIME_FUNCTION_START(FC, functionName)\
    UNARY_FUNCTION_START(FC, functionName)\
        switch (argumentValue.getDatatypeID()) {\
        case D_XSD_DATE_TIME:\
        case D_XSD_TIME:\
        case D_XSD_DATE:\
        case D_XSD_G_YEAR_MONTH:\
        case D_XSD_G_YEAR:\
        case D_XSD_G_MONTH_DAY:\
        case D_XSD_G_DAY:\
        case D_XSD_G_MONTH:\
            {\
                const XSDDateTime& argumentDateTime = argumentValue.getData<XSDDateTime>();

#define UNARY_DATE_TIME_FUNCTION_END\
            }\
            break;\
        default:\
            return true;\
        }\
    UNARY_FUNCTION_END

// YearEvaluator

UNARY_DATE_TIME_FUNCTION_START(YearEvaluator, "YEAR")
    result.setInteger(argumentDateTime.getYear());
UNARY_DATE_TIME_FUNCTION_END

static GenericBuiltinFunctionDescriptor<YearEvaluator, 1, 1> s_registration_YearEvaluatorXPathNS("http://www.w3.org/2005/xpath-functions#year-from-dateTime", 1000);

// MonthEvaluator

UNARY_DATE_TIME_FUNCTION_START(MonthEvaluator, "MONTH")
    result.setInteger(argumentDateTime.getMonth());
UNARY_DATE_TIME_FUNCTION_END

static GenericBuiltinFunctionDescriptor<MonthEvaluator, 1, 1> s_registration_MonthEvaluatorXPathNS("http://www.w3.org/2005/xpath-functions#month-from-dateTime", 1000);

// DayEvaluator

UNARY_DATE_TIME_FUNCTION_START(DayEvaluator, "DAY")
    result.setInteger(argumentDateTime.getDay());
UNARY_DATE_TIME_FUNCTION_END

static GenericBuiltinFunctionDescriptor<DayEvaluator, 1, 1> s_registration_DayEvaluatorXPathNS("http://www.w3.org/2005/xpath-functions#day-from-dateTime", 1000);

// HoursEvaluator

UNARY_DATE_TIME_FUNCTION_START(HoursEvaluator, "HOURS")
    result.setInteger(argumentDateTime.getHour());
UNARY_DATE_TIME_FUNCTION_END

static GenericBuiltinFunctionDescriptor<HoursEvaluator, 1, 1> s_registration_HoursEvaluatorXPathNS("http://www.w3.org/2005/xpath-functions#hours-from-dateTime", 1000);

// MinutesEvaluator

UNARY_DATE_TIME_FUNCTION_START(MinutesEvaluator, "MINUTES")
    result.setInteger(argumentDateTime.getMinute());
UNARY_DATE_TIME_FUNCTION_END

static GenericBuiltinFunctionDescriptor<MinutesEvaluator, 1, 1> s_registration_MinutesEvaluatorXPathNS("http://www.w3.org/2005/xpath-functions#minutes-from-dateTime", 1000);

// SecondsEvaluator

UNARY_DATE_TIME_FUNCTION_START(SecondsEvaluator, "SECONDS")
    result.setInteger(argumentDateTime.getSecond());
UNARY_DATE_TIME_FUNCTION_END

static GenericBuiltinFunctionDescriptor<SecondsEvaluator, 1, 1> s_registration_SecondsEvaluatorXPathNS("http://www.w3.org/2005/xpath-functions#seconds-from-dateTime", 1000);

// TimezoneEvaluator

UNARY_DATE_TIME_FUNCTION_START(TimezoneEvaluator, "TIMEZONE")
    if (argumentDateTime.getTimeZoneOffset() == TIME_ZONE_OFFSET_ABSENT)
        return true;
    else
        result.setData(D_XSD_DURATION, XSDDuration(0, argumentDateTime.getTimeZoneOffset() * 60, 0));
UNARY_DATE_TIME_FUNCTION_END

static GenericBuiltinFunctionDescriptor<TimezoneEvaluator, 1, 1> s_registration_TimezoneEvaluatorXPathNS("http://www.w3.org/2005/xpath-functions#timezone-from-dateTime", 1000);

// TzEvaluator

UNARY_DATE_TIME_FUNCTION_START(TzEvaluator, "TZ")
    if (argumentDateTime.getTimeZoneOffset() == TIME_ZONE_OFFSET_ABSENT)
        result.setString(D_XSD_STRING, "");
    else if (argumentDateTime.getTimeZoneOffset() == 0)
        result.setString(D_XSD_STRING, "Z");
    else {
        std::ostringstream buffer;
        int16_t timeZoneOffset = argumentDateTime.getTimeZoneOffset();
        if (timeZoneOffset < 0) {
            buffer << "-";
            timeZoneOffset = -timeZoneOffset;
        }
        else
            buffer << "+";
        buffer.width(2);
        buffer.fill('0');
        buffer << (timeZoneOffset / 60);
        buffer << ":";
        buffer << (timeZoneOffset % 60);
        result.setString(D_XSD_STRING, buffer.str());
    }
UNARY_DATE_TIME_FUNCTION_END
