// JRDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

package uk.ac.ox.cs.JRDFox.model;

import java.util.HashMap;

public enum Datatype {
	INVALID_DATATYPE("INVALID_DATATYPE", 0),
	IRI_REFERENCE("internal:iri-reference", 1),
	BLANK_NODE("internal:blank-node", 2),
	XSD_STRING("http://www.w3.org/2001/XMLSchema#string", 3),
	RDF_PLAIN_LITERAL("http://www.w3.org/1999/02/22-rdf-syntax-ns#PlainLiteral", 4),
	XSD_INTEGER("http://www.w3.org/2001/XMLSchema#integer", 5),
	XSD_FLOAT("http://www.w3.org/2001/XMLSchema#float", 6),
	XSD_DOUBLE("http://www.w3.org/2001/XMLSchema#double", 7),
	XSD_BOOLEAN("http://www.w3.org/2001/XMLSchema#boolean", 8),
	XSD_DATE_TIME("http://www.w3.org/2001/XMLSchema#dateTime", 9),
	XSD_DATE_TIME_STAMP("http://www.w3.org/2001/XMLSchema#dateTimeStamp", XSD_DATE_TIME.getDatatypeID()),
	XSD_TIME("http://www.w3.org/2001/XMLSchema#time", 10),
	XSD_DATE("http://www.w3.org/2001/XMLSchema#date", 11),
	XSD_G_YEAR_MONTH("http://www.w3.org/2001/XMLSchema#gYearMonth", 12),
	XSD_G_YEAR("http://www.w3.org/2001/XMLSchema#gYear", 13),
	XSD_G_MONTH_DAY("http://www.w3.org/2001/XMLSchema#gMonthDay", 14),
	XSD_G_DAY("http://www.w3.org/2001/XMLSchema#gDay", 15),
	XSD_G_MONTH("http://www.w3.org/2001/XMLSchema#gMonth", 16),
	XSD_DURATION("http://www.w3.org/2001/XMLSchema#duration", 17),
	XSD_YEAR_MONTH_DURATION("http://www.w3.org/2001/XMLSchema#yearMonthDuration", XSD_DURATION.getDatatypeID()),
	XSD_DAY_TIME_DURATION("http://www.w3.org/2001/XMLSchema#dayTimeDuration", XSD_DURATION.getDatatypeID()),
	RDF_XML_LITERAL("http://www.w3.org/1999/02/22-rdf-syntax-ns#XMLLiteral", XSD_STRING.getDatatypeID()),
	RDFS_LITERAL("http://www.w3.org/2000/01/rdf-schema#Literal", XSD_STRING.getDatatypeID()),
	OWL_REAL("http://www.w3.org/2002/07/owl#real", XSD_DOUBLE.getDatatypeID()),
	OWL_RATIONAL("http://www.w3.org/2002/07/owl#rational", XSD_DOUBLE.getDatatypeID()),
	XSD_DECIMAL("http://www.w3.org/2001/XMLSchema#decimal", XSD_DOUBLE.getDatatypeID()),
	XSD_NON_NEGATIVE_INTEGER("http://www.w3.org/2001/XMLSchema#nonNegativeInteger", XSD_INTEGER.getDatatypeID()),
	XSD_NON_POSITIVE_INTEGER("http://www.w3.org/2001/XMLSchema#nonPositiveInteger", XSD_INTEGER.getDatatypeID()),
	XSD_POSITIVE_INTEGER("http://www.w3.org/2001/XMLSchema#positiveInteger", XSD_INTEGER.getDatatypeID()),
	XSD_NEGATIVE_INTEGER("http://www.w3.org/2001/XMLSchema#negativeInteger", XSD_INTEGER.getDatatypeID()),
	XSD_LONG("http://www.w3.org/2001/XMLSchema#long", XSD_INTEGER.getDatatypeID()),
	XSD_INT("http://www.w3.org/2001/XMLSchema#int", XSD_INTEGER.getDatatypeID()),
	XSD_SHORT("http://www.w3.org/2001/XMLSchema#short", XSD_INTEGER.getDatatypeID()),
	XSD_BYTE("http://www.w3.org/2001/XMLSchema#byte", XSD_INTEGER.getDatatypeID()),
	XSD_UNSIGNED_LONG("http://www.w3.org/2001/XMLSchema#unsignedLong", XSD_INTEGER.getDatatypeID()),
	XSD_UNSIGNED_INT("http://www.w3.org/2001/XMLSchema#unsignedInt", XSD_INTEGER.getDatatypeID()),
	XSD_UNSIGNED_SHORT("http://www.w3.org/2001/XMLSchema#unsignedShort", XSD_INTEGER.getDatatypeID()),
	XSD_UNSIGNED_BYTE("http://www.w3.org/2001/XMLSchema#unsignedByte", XSD_INTEGER.getDatatypeID()),
	XSD_NORMALIZED_STRING("http://www.w3.org/2001/XMLSchema#normalizedString", XSD_STRING.getDatatypeID()),
	XSD_TOKEN("http://www.w3.org/2001/XMLSchema#token", XSD_STRING.getDatatypeID()),
	XSD_LANGUAGE("http://www.w3.org/2001/XMLSchema#language", XSD_STRING.getDatatypeID()),
	XSD_NAME("http://www.w3.org/2001/XMLSchema#Name", XSD_STRING.getDatatypeID()),
	XSD_NCNAME("http://www.w3.org/2001/XMLSchema#NCName", XSD_STRING.getDatatypeID()),
	XSD_NMTOKEN("http://www.w3.org/2001/XMLSchema#NMTOKEN", XSD_STRING.getDatatypeID()),
	XSD_HEX_BINARY("http://www.w3.org/2001/XMLSchema#hexBinary", XSD_STRING.getDatatypeID()),
	XSD_BASE_64_BINARY("http://www.w3.org/2001/XMLSchema#base64Binary", XSD_STRING.getDatatypeID()),
	XSD_ANY_URI("http://www.w3.org/2001/XMLSchema#anyURI", XSD_STRING.getDatatypeID());
	
	private static Datatype[] s_datatypeIDs = values();	
	
	private static HashMap<String, Datatype> m_iri2Datatype = new HashMap<String, Datatype>();
	
	static {
		for(Datatype datatype : values())
			m_iri2Datatype.put(datatype.getIRI(), datatype);
    }
	
	public static Datatype value(int datatypeID) {
    	return s_datatypeIDs[datatypeID]; 
    }
	
	public static Datatype value(String datatypeIRI) {
		return m_iri2Datatype.get(datatypeIRI);
	}
    
    protected final int m_datatypeID;
    protected final String m_iri;
    
    private Datatype(String iri, int datatypeID) {
    	m_datatypeID = datatypeID;
    	m_iri = iri;
    }
	
    public int getDatatypeID() {
    	return m_datatypeID;
    }
    
    public String getIRI() {
    	return m_iri;
    }
    
};