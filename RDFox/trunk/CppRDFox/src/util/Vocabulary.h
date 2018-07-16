// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef VOCABULARY_H_
#define VOCABULARY_H_

#include "../all.h"

// Namespaces

extern const char* const RDF_NS;
extern const char* const RDFS_NS;
extern const char* const OWL_NS;
extern const char* const XSD_NS;
extern const char* const SWRL_NS;
extern const char* const SWRLB_NS;
extern const char* const SWRLX_NS;
extern const char* const RULEML_NS;
extern const char* const RDFOX_NS;

// RDF vocabulary

extern const char* const RDF_TYPE;
extern const char* const RDF_FIRST;
extern const char* const RDF_REST;
extern const char* const RDF_NIL;

// RDFS vocabulary

extern const char* const RDFS_SUBCLASSOF;
extern const char* const RDFS_SUBPROPERTYOF;
extern const char* const RDFS_DOMAIN;
extern const char* const RDFS_RANGE;

// OWL vocabulary

extern const char* const OWL_SAME_AS;
extern const char* const OWL_DIFFERENT_FROM;
extern const char* const OWL_THING;
extern const char* const OWL_NOTHING;

// Common resources

extern const char* const RDF_PLAIN_LITERAL;
extern const char* const XSD_STRING;
extern const char* const XSD_BOOLEAN;
extern const char* const XSD_FLOAT;
extern const char* const XSD_DOUBLE;
extern const char* const XSD_INTEGER;
extern const char* const XSD_DATE_TIME;
extern const char* const XSD_DATE_TIME_STAMP;
extern const char* const XSD_TIME;
extern const char* const XSD_DATE;
extern const char* const XSD_G_YEAR_MONTH;
extern const char* const XSD_G_YEAR;
extern const char* const XSD_G_MONTH_DAY;
extern const char* const XSD_G_DAY;
extern const char* const XSD_G_MONTH;
extern const char* const XSD_DURATION;
extern const char* const XSD_DAY_TIME_DURATION;
extern const char* const XSD_YEAR_MONTH_DURATION;

#endif // VOCABULARY_H_
