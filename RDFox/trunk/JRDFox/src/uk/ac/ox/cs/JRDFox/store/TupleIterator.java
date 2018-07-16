// JRDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

package uk.ac.ox.cs.JRDFox.store;

import java.nio.LongBuffer;
import java.util.Map;

import uk.ac.ox.cs.JRDFox.JRDFoxException;
import uk.ac.ox.cs.JRDFox.Prefixes;
import uk.ac.ox.cs.JRDFox.model.GroundTerm;
import uk.ac.ox.cs.JRDFox.store.Dictionary.ResourceProvider;

public class TupleIterator {
	
    protected volatile long m_tupleIteratorPtr;
    protected final DataStore m_dataStore;
    protected long m_dataStoreGenerationAtOpen;
    protected final Dictionary m_dictionary;
    protected final int m_windowSize;
    protected final int m_arity;
    protected final ExtendedBuffer<LongBuffer> m_resourceIDsBuffer;
    protected final ExtendedBuffer<LongBuffer> m_multiplicitiesBuffer;
    protected final Resource[] m_resources;
    protected final GroundTerm[] m_groundTerms;
    protected boolean m_groundTermsRetrieved;
    protected boolean m_resourcesRetrieved;
    protected final Dictionary.ResourceProvider m_resourceProvider;

    protected native static long nCompileQuery(long dataStorePtr, String queryText, String[] prefixes, String[] parameters, int[] arity) throws JRDFoxException;
    protected native static int nOpen(long tupleIteratorPtr, int windowSize, long  resourceIDsPtr, long  multiplicitiesPtr) throws JRDFoxException;
    protected native static int nAdvance(long tupleIteratorPtr, int windowSize, long  resourceIDsPtr, long  multiplicitiesPtr) throws JRDFoxException;
    protected native static int nGetResources(long tupleIteratorPtr, long resourceIDsPtr, int firstIndex, int lastIndex, long stringByteBufferPtr, int stringByteBufferLength, long stringLengthsBufferPtr, long datatypeIDsBufferPtr) throws JRDFoxException;
    protected native static void nDispose(long tupleIteartorPtr);
    
    protected void ensureValidState() throws JRDFoxException {
        if (m_tupleIteratorPtr == 0)
            throw new JRDFoxException("Access to a disposed iterator.");
        try {
        	m_dataStore.ensureValidState();
        }
        catch (JRDFoxException e) {
            dispose();
        	throw e;
        }
        if (m_dataStoreGenerationAtOpen != 0 && m_dataStoreGenerationAtOpen != m_dataStore.m_generation) {
        	dispose();
        	throw new JRDFoxException("The store has been reinitialised since the last open of the iterator.");
        }
    }
        
    protected TupleIterator(DataStore dataStore, String queryText, Prefixes prefixes, Map<String, String> parameters, int windowSize) throws JRDFoxException {
    	if (windowSize <= 0)
    		throw new JRDFoxException("Window size must be positive.");
    	if (queryText == null)
    		throw new NullPointerException("queryText should not be null.");
        int[] arity = new int[1];
        m_windowSize = windowSize;
        m_tupleIteratorPtr = nCompileQuery(dataStore.m_storePtr, queryText, DataStore.mapToStringArray(prefixes.getPrefixIRIsByPrefixName()), DataStore.mapToStringArray(parameters), arity);        
        m_dataStore = dataStore;
        m_dataStoreGenerationAtOpen = 0;
        m_dictionary = dataStore.getDictionary();
        m_arity = arity[0];
        m_resourceIDsBuffer = ExtendedBuffer.getDirectLongBuffer(m_windowSize * m_arity);
        m_multiplicitiesBuffer = ExtendedBuffer.getDirectLongBuffer(m_windowSize);
        m_multiplicitiesBuffer.m_buffer.limit(0).position(0);
        m_resources = new Resource[m_resourceIDsBuffer.m_buffer.capacity()];
        m_groundTerms = new GroundTerm[m_resourceIDsBuffer.m_buffer.capacity()];
        m_groundTermsRetrieved = false;
        m_resourcesRetrieved = false;
        m_resourceProvider = new ResourceProvider() {
			public int getResources(long resourceIDsPtr, int firstIndex, int lastIndex, long stringByteBufferPtr, int stringByteBufferLength, long stringLengthsBufferPtr, long datatypeIDsBufferPtr)  throws JRDFoxException {
				return nGetResources(m_tupleIteratorPtr, resourceIDsPtr, firstIndex, lastIndex, stringByteBufferPtr, stringByteBufferLength, stringLengthsBufferPtr, datatypeIDsBufferPtr);
			}
		};
    }

    /**
     * Gets the arity of the tuple iterator.
     * 
     * @return                      the arity of the tuple iterator
     * @throws JRDFoxException   thrown if the iterator is in invalid state
     */
    public synchronized int getArity() throws JRDFoxException {        
        ensureValidState();
        return m_arity;
    }

    /**
     * Gets the multiplicity of the current tuple.
     * 
     * @return						the multiplicity of the current tuple
     * @throws JRDFoxException	thrown if the iterator is in invalid state
     */
    public synchronized long getMultiplicity() throws JRDFoxException {
        ensureValidState();        
        return m_multiplicitiesBuffer.m_buffer.hasRemaining() ? m_multiplicitiesBuffer.m_buffer.get(m_multiplicitiesBuffer.m_buffer.position()) : 0;
    }
    
    /**
     * Determines whether the iterator is in a valid state: i.e. it has been opened and not yet exhausted. 
     * 
     * @return						Returns true if the iterator is in a valid state and false otherwise. 
     * @throws JRDFoxException	thrown if the iterator is in invalid state
     */
    public synchronized boolean isValid() throws JRDFoxException {
        ensureValidState();
        return m_multiplicitiesBuffer.m_buffer.hasRemaining();
    }
    
    /**
     * Opens the iterator, i.e. positions itself to the first tuple.
     * 
     * @return                      the multiplicity of the first tuple (0 if there are no tuples)
     * @throws JRDFoxException   thrown if the iterator is in invalid state
     */
    public synchronized long open() throws JRDFoxException {
    	m_dataStoreGenerationAtOpen = m_dataStore.m_generation;
        ensureValidState();
        m_multiplicitiesBuffer.m_buffer.limit(nOpen(m_tupleIteratorPtr, m_windowSize, m_resourceIDsBuffer.m_bufferPtr, m_multiplicitiesBuffer.m_bufferPtr)).position(0);
        m_resourceIDsBuffer.m_buffer.position(0).limit(m_multiplicitiesBuffer.m_buffer.limit() * m_arity);
        m_groundTermsRetrieved = false;
        m_resourcesRetrieved = false;
        return getMultiplicity();
    }

    /**
     * Advances the iterator (i.e. positions the iterator to the next tuple).
     * 
     * @return                      the multiplicity of the next tuple (0 if there are no more tuples)
     * @throws JRDFoxException   thrown if the iterator is in invalid state
     */
    public synchronized long advance() throws JRDFoxException {
        ensureValidState();
        if (!isValid())
        	throw new JRDFoxException("Access to an invalid iterator.");
        m_multiplicitiesBuffer.m_buffer.get();
        if (m_multiplicitiesBuffer.m_buffer.hasRemaining()) {
        	m_resourceIDsBuffer.m_buffer.position(m_resourceIDsBuffer.m_buffer.position() + m_arity);
        	return m_multiplicitiesBuffer.m_buffer.get(m_multiplicitiesBuffer.m_buffer.position());
        }
        else {
        	if (m_multiplicitiesBuffer.m_buffer.limit() < m_windowSize)
        		return 0;
        	else {
        		m_multiplicitiesBuffer.m_buffer.limit(nAdvance(m_tupleIteratorPtr, m_windowSize, m_resourceIDsBuffer.m_bufferPtr, m_multiplicitiesBuffer.m_bufferPtr)).position(0);
        		m_resourceIDsBuffer.m_buffer.position(0);                
        		m_groundTermsRetrieved = false;
        		m_resourcesRetrieved = false;
        		return getMultiplicity();
        	} 
        }
    }

    /**
     * Returns the ground term by for the given termIndex.
     * 
     * @param termIndex             the index of the term
     * @return                      the ground term corresponding to the given index
     * @throws JRDFoxException   thrown if the iterator is in invalid state
     */
    public synchronized GroundTerm getGroundTerm(int termIndex) throws JRDFoxException {
    	ensureValidState();
    	if (!isValid())
        	throw new JRDFoxException("Access to an invalid iterator.");
    	if (!m_groundTermsRetrieved) {
    		m_dictionary.getGroundTerms(m_resourceIDsBuffer, m_groundTerms, m_resourceProvider);
    		m_groundTermsRetrieved = true;
    	}
    	return m_groundTerms[m_resourceIDsBuffer.m_buffer.position() + termIndex];
    }

    /**
     * Returns the raw ground term for the given termIndex.
     * 
     * @param termIndex             the index of the term
     * @return                      the resource that corresponds to the given term index
     * @throws JRDFoxException   thrown if the iterator is in invalid state
     */
    public synchronized Resource getResource(int termIndex) throws JRDFoxException {
    	ensureValidState();
    	if (!isValid())
    		throw new JRDFoxException("Access to an invalid iterator.");
    	if (!m_resourcesRetrieved) {
    		m_dictionary.getResources(m_resourceIDsBuffer, m_resources, m_resourceProvider);
    		m_resourcesRetrieved = true;
    	}
    	return m_resources[m_resourceIDsBuffer.m_buffer.position() + termIndex];
    }

    /**
     * Returns the resource id for the given termIndex.
     * 
     * @param termIndex             the index of the term
     * @return                      the resource ID that corresponds to the given term index
     * @throws JRDFoxException   thrown if the iterator is in invalid state
     */
    public synchronized long getResourceID(int termIndex) throws JRDFoxException {
        ensureValidState();
        if (!isValid())
        	throw new JRDFoxException("Access to an invalid iterator.");
        return m_resourceIDsBuffer.m_buffer.get(m_resourceIDsBuffer.m_buffer.position() + termIndex);
    }

    /**
     * Disposes of the iterator. This method *must* be called to iterators that are no longer in use!
     */
    public synchronized void dispose() {
        if (m_tupleIteratorPtr != 0) {        	
        	nDispose(m_tupleIteratorPtr);
            m_tupleIteratorPtr = 0;
        }
    }

    protected void finalize() {
        dispose();
    }
    
}
