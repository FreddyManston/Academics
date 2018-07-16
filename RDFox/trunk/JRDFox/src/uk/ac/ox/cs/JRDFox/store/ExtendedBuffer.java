package uk.ac.ox.cs.JRDFox.store;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.nio.LongBuffer;
import java.nio.ShortBuffer;

class ExtendedBuffer<T extends Buffer> {

	protected static native long nGetBufferPtr(ByteBuffer buffer);
	
	public static ExtendedBuffer<ByteBuffer> getDirectByteBuffer(int capacity) {
		ByteBuffer byteBuffer = ByteBuffer.allocateDirect(capacity);
		byteBuffer.order(ByteOrder.nativeOrder());
		return new ExtendedBuffer<ByteBuffer>(byteBuffer, nGetBufferPtr(byteBuffer));
	}

	public static ExtendedBuffer<ShortBuffer> getDirectShortBuffer(int capacity) {
		ByteBuffer tmpBuffer = ByteBuffer.allocateDirect(capacity * Short.SIZE / Byte.SIZE);
		tmpBuffer.order(ByteOrder.nativeOrder());			
		return new ExtendedBuffer<ShortBuffer>(tmpBuffer.asShortBuffer(), nGetBufferPtr(tmpBuffer));
	}

	public static ExtendedBuffer<IntBuffer> getDirectIntBuffer(int capacity) {
		ByteBuffer tmpBuffer = ByteBuffer.allocateDirect(capacity * Integer.SIZE / Byte.SIZE);
		tmpBuffer.order(ByteOrder.nativeOrder());
		return new ExtendedBuffer<IntBuffer>(tmpBuffer.asIntBuffer(), nGetBufferPtr(tmpBuffer));
	}

	public static ExtendedBuffer<LongBuffer> getDirectLongBuffer(int capacity) {
		ByteBuffer tmpBuffer = ByteBuffer.allocateDirect(capacity * Long.SIZE / Byte.SIZE);
		tmpBuffer.order(ByteOrder.nativeOrder());
		return new ExtendedBuffer<LongBuffer>(tmpBuffer.asLongBuffer(), nGetBufferPtr(tmpBuffer));
	}
	
	public T m_buffer;
	public long m_bufferPtr;

	public ExtendedBuffer(T buffer, long bufferPtr) {
		m_buffer = buffer;
		m_bufferPtr = bufferPtr;
	}
	
}