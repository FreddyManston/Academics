// JRDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

package uk.ac.ox.cs.JRDFox.store;

import java.lang.ref.WeakReference;

import uk.ac.ox.cs.JRDFox.JRDFoxException;

public class Cache<E> {
	
	protected volatile WeakReference<E>[] m_data;
	protected boolean m_cache;
	
	public Cache(boolean cache) throws JRDFoxException {
		m_cache = cache;
		if (m_cache && !initialize(1024))
			throw new JRDFoxException("Error initializing cache!");
	}
	
	@SuppressWarnings("unchecked")
	public boolean initialize(long length) {
		if (m_cache) {
			if (length < 0 || length >= Integer.MAX_VALUE)
				return false;
			synchronized (this) {
				m_data = new WeakReference[(int)length];
			}
		}
		return true;
	}
	
	public E get(long index) {
		if (m_cache && index >= 0 && index < Integer.MAX_VALUE) {
			WeakReference<E>[] data = m_data;
			if (index < data.length) {
				WeakReference<E> termReference = data[(int)index];
				if (termReference != null)
					return termReference.get();
			}
		}
		return null;
	}
	
	public boolean cache(long index, E term) {
		if (!m_cache || index < 0 || index > Integer.MAX_VALUE)
			return true;
		if (m_data.length <= index) 
			return false;
		m_data[(int)index] = new WeakReference<E>(term);
		return true;
	}
	
	@SuppressWarnings("unchecked")
	public boolean ensureAvailability(long length) {
		if (!m_cache)
			return true;
	    if (length < 0 || length >= Integer.MAX_VALUE)
	        return false;
		synchronized (this) {
		    try {
		        if (m_data.length <= length) {
		            WeakReference<E>[] newData = new WeakReference[(int)length];
		            System.arraycopy(m_data, 0, newData, 0, m_data.length);
		            m_data = newData;
		        }
		        return true;
		    }
		    catch(Exception ex) { return false; }
		}
	}
	
}
