// JRDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

package uk.ac.ox.cs.JRDFox.store;

import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.LongBuffer;
import java.nio.ShortBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;

import uk.ac.ox.cs.JRDFox.JRDFoxException;
import uk.ac.ox.cs.JRDFox.model.BlankNode;
import uk.ac.ox.cs.JRDFox.model.Datatype;
import uk.ac.ox.cs.JRDFox.model.GroundTerm;
import uk.ac.ox.cs.JRDFox.model.Individual;
import uk.ac.ox.cs.JRDFox.model.Literal;
import uk.ac.ox.cs.JRDFox.store.ExtendedBuffer;

public class Dictionary {
	
	protected native static long nGetMaxResourceID(long dictionaryPtr);
	protected native static int nGetResources(long dictionaryPtr, long resourceIDsPtr, int firstIndex, int limitIndex, long stringByteBufferPtr, int stringByteBufferLength, long stringLengthsBufferPtr, long datatypeIDsBufferPtr) throws JRDFoxException;
	protected native static void nResolveResources(long dictionaryPtr, long[] resourceIDs, String[] lexicalForms, int[] datatypes) throws JRDFoxException;

	enum ExportTypeName { Resource, GroundTerm }
	
	public static interface ResourceProvider {
		public int getResources(long resourceIDsPtr, int firstIndex, int limitIndex, long stringByteBufferPtr, int stringByteBufferLength, long stringLengthsBufferPtr, long datatypeIDsBufferPtr)  throws JRDFoxException; 
	}
	
 	protected static class JNIToken { 
		protected final Charset m_charset;
		protected final CharsetDecoder m_charsetDecoder;
		protected final int m_initialCapacity = 1;
		protected int m_capacity;
		protected ExtendedBuffer<ByteBuffer> m_stringByteBuffer;
		protected ExtendedBuffer<IntBuffer> m_stringLengthsBuffer;
		protected ExtendedBuffer<ShortBuffer> m_datatypeIDsBuffer;
		protected ExtendedBuffer<LongBuffer> m_resourceIDsBuffer;
		protected byte[] m_stringByteArray;
		protected long[] m_resourceIDArray;
		protected Resource[] m_resourceArray;
		
		protected JNIToken() {
			m_charset = Charset.forName("UTF-8");
			m_charsetDecoder = m_charset.newDecoder();
			m_capacity = 0;
			ensureStringCapacity(1024 * 1024);
			ensureCapacity(m_initialCapacity);
			m_resourceIDArray = new long[1];
			m_resourceArray = new Resource[1];
		}

		protected void ensureCapacity(int capacity) {
			if (m_capacity < capacity) {
				m_stringLengthsBuffer = ExtendedBuffer.getDirectIntBuffer(capacity);
				m_datatypeIDsBuffer = ExtendedBuffer.getDirectShortBuffer(capacity);
				m_resourceIDsBuffer = ExtendedBuffer.getDirectLongBuffer(capacity);
				m_capacity = capacity;
			}
		}

		protected void ensureStringCapacity(int capacity) {
			if (m_stringByteBuffer == null || m_stringByteBuffer.m_buffer.capacity() < capacity) {
				m_stringByteBuffer = ExtendedBuffer.getDirectByteBuffer(capacity);
				m_stringByteArray = new byte[capacity];
			}
		}

	}

	protected final static ThreadLocal<JNIToken> s_jniToken = new ThreadLocal<JNIToken>() {
		protected JNIToken initialValue() {
			return new JNIToken();
		}
	};

	protected volatile long m_dictionaryPtr;
	final protected DataStore m_dataStore;
	final protected Cache<Resource> m_resourceCache;
	final protected Cache<GroundTerm> m_groundTermCache;
	final protected ResourceProvider m_resourceProvider;

	protected void ensureValidState() throws JRDFoxException {
		if (m_dictionaryPtr == 0)
			throw new JRDFoxException("The store of this dictionary has been disposed of.");
	}
	
	public synchronized void initialize() {
		long maxResourceID = nGetMaxResourceID(m_dictionaryPtr);
		m_resourceCache.initialize(maxResourceID);
		m_groundTermCache.initialize(maxResourceID);
	}

	public Dictionary(DataStore dataStore, long dictionaryPtr, boolean cacheResources, boolean cacheGroundTerms) throws JRDFoxException {
		m_dictionaryPtr = dictionaryPtr;
		m_dataStore = dataStore;
		m_resourceCache = new Cache<Resource>(cacheResources);
		m_groundTermCache = new Cache<GroundTerm>(cacheGroundTerms);
		m_resourceProvider = new ResourceProvider() {
			@Override
			public int getResources(long resourceIDsPtr, int firstIndex, int limitIndex, long stringByteBufferPtr, int stringByteBufferLength, long stringLengthsBufferPtr, long datatypeIDsBufferPtr)  throws JRDFoxException {
				return nGetResources(m_dictionaryPtr, resourceIDsPtr, firstIndex, limitIndex, stringByteBufferPtr, stringByteBufferLength, stringLengthsBufferPtr, datatypeIDsBufferPtr);
			}
		};
	}

	protected static Object getValue(ExportTypeName exportTypeName, String lexicalForm, Datatype datatype) {
		if (exportTypeName == ExportTypeName.GroundTerm) {
			switch (datatype) {
			case IRI_REFERENCE:
				return Individual.create(lexicalForm);
			case BLANK_NODE:
				return BlankNode.create(lexicalForm);
			default:
				return Literal.create(lexicalForm, datatype);
			}
		}
		else
			return new Resource(lexicalForm, datatype);
	}
	
	protected static String getLexicalFormUsingByteArray(JNIToken jniToken) {
		int length = jniToken.m_stringByteBuffer.m_buffer.remaining() - 1;
		jniToken.m_stringByteBuffer.m_buffer.get(jniToken.m_stringByteArray, 0, length);
		return new String(jniToken.m_stringByteArray, 0, length, jniToken.m_charset);
	}
	
	@SuppressWarnings("unchecked")
	static protected <ExportType> void getResources(ExtendedBuffer<LongBuffer> resourceIDsBuffer, ExportTypeName exportTypeName, ExportType[] values, Dictionary dictionary, ResourceProvider resourceProvider) throws JRDFoxException {		
		JNIToken jniToken = Dictionary.s_jniToken.get();
		jniToken.ensureCapacity(resourceIDsBuffer.m_buffer.limit());
		Cache<ExportType> cache = (Cache<ExportType>)dictionary.getCache(exportTypeName);
		LongBuffer missingResourcesBuffer = jniToken.m_resourceIDsBuffer.m_buffer;
		missingResourcesBuffer.limit(missingResourcesBuffer.capacity()).position(0);
		int[] missingResourcesArrayIndexes = new int[missingResourcesBuffer.capacity()];
		for (int index = 0; index < resourceIDsBuffer.m_buffer.limit(); index++)        	
			if ((values[index] = cache.get(resourceIDsBuffer.m_buffer.get(index))) == null) {
				missingResourcesArrayIndexes[missingResourcesBuffer.position()] = index;
				missingResourcesBuffer.put(resourceIDsBuffer.m_buffer.get(index));
			}
		missingResourcesBuffer.limit(missingResourcesBuffer.position()).position(0);
		while (missingResourcesBuffer.hasRemaining()) {
			jniToken.m_stringByteBuffer.m_buffer.limit(0).position(0);
			int firstUnprocessed = resourceProvider.getResources(jniToken.m_resourceIDsBuffer.m_bufferPtr, missingResourcesBuffer.position(), missingResourcesBuffer.limit(), jniToken.m_stringByteBuffer.m_bufferPtr, jniToken.m_stringByteBuffer.m_buffer.capacity(), jniToken.m_stringLengthsBuffer.m_bufferPtr, jniToken.m_datatypeIDsBuffer.m_bufferPtr);
			for (int index = missingResourcesBuffer.position(); index < firstUnprocessed; index++) {
				int valueIndex = missingResourcesArrayIndexes[index];
				int beginning = jniToken.m_stringByteBuffer.m_buffer.limit();
				jniToken.m_stringByteBuffer.m_buffer.limit(beginning + jniToken.m_stringLengthsBuffer.m_buffer.get(index)).position(beginning);
				if ((values[valueIndex] = cache.get(missingResourcesBuffer.get(index))) == null) {
					Datatype datatype = Datatype.value(jniToken.m_datatypeIDsBuffer.m_buffer.get(index));
					String lexicalForm = getLexicalFormUsingByteArray(jniToken);
					values[valueIndex] = (ExportType)getValue(exportTypeName, lexicalForm, datatype);
					if (!cache.cache(missingResourcesBuffer.get(index), values[valueIndex]))
						if (cache.ensureAvailability(nGetMaxResourceID(dictionary.m_dictionaryPtr)))
							cache.cache(missingResourcesBuffer.get(index), values[valueIndex]);
				}
			}
			missingResourcesBuffer.position(firstUnprocessed);
			if (missingResourcesBuffer.hasRemaining())
				jniToken.ensureStringCapacity(jniToken.m_stringLengthsBuffer.m_buffer.get(firstUnprocessed));
		}
	}
	
	protected Object getCache(ExportTypeName exportType) {
		if (exportType == ExportTypeName.Resource)
			return m_resourceCache;
		else
			return m_groundTermCache;
	}

	public Resource getResource(long resourceID) throws JRDFoxException {
		JNIToken jniToken = s_jniToken.get();
		jniToken.m_resourceIDArray[0] = resourceID;
		getResources(jniToken.m_resourceIDArray, jniToken.m_resourceArray);
		return jniToken.m_resourceArray[0];
	}	
	
	public void getResources(long[] resourceIDs, Resource[] resources) throws JRDFoxException {
		JNIToken jniToken = s_jniToken.get();
		jniToken.ensureCapacity(resourceIDs.length);
		for (int index = 0; index < resourceIDs.length; index++)
			jniToken.m_resourceIDsBuffer.m_buffer.put(index, resourceIDs[index]);		
		getResources(jniToken.m_resourceIDsBuffer, resources, m_resourceProvider);
	}

	public void getResources(ExtendedBuffer<LongBuffer> resourceIDsBuffer, Resource[] resources, ResourceProvider resourceProvider) throws JRDFoxException {
		getResources(resourceIDsBuffer, ExportTypeName.Resource, resources, this, resourceProvider);
	}

	public void getGroundTerms(long[] resourceIDs, GroundTerm[] groundTerms) throws JRDFoxException {
		JNIToken jniToken = s_jniToken.get();
		jniToken.ensureCapacity(resourceIDs.length);
		for (int index = 0; index < resourceIDs.length; index++)
			jniToken.m_resourceIDsBuffer.m_buffer.put(index, resourceIDs[index]);
		getGroundTerms(jniToken.m_resourceIDsBuffer, groundTerms, m_resourceProvider);
	}

	public void getGroundTerms(ExtendedBuffer<LongBuffer> resourceIDsBuffer, GroundTerm[] groundTerms, ResourceProvider resourceProvider) throws JRDFoxException {
		getResources(resourceIDsBuffer, ExportTypeName.GroundTerm, groundTerms, this, resourceProvider);
	}

	/**
	 * Resolves the specified ground terms.
	 * 
	 * @param lexicalForms          the lexical forms of the resources
	 * @param datatypes             the data types of the resources
	 * @return                      the resource IDs corresponding to the given ground terms
	 * @throws JRDFoxException   thrown if there is an error
	 */
	public long[] resolveResources(String[] lexicalForms, Datatype[] datatypes) throws JRDFoxException {
		int[] datatypeIDs = new int[datatypes.length];
		for (String lexicalForm : lexicalForms)
			if (lexicalForm == null)
				throw new NullPointerException("Lexical forms should not have non-null values.");
		for (int index = 0; index < datatypes.length; index++)
			datatypeIDs[index] = datatypes[index].getDatatypeID();
		long resourceIDs[] = new long[lexicalForms.length];
		nResolveResources(m_dictionaryPtr, resourceIDs, lexicalForms, datatypeIDs);        	
		return resourceIDs;
	}
}
