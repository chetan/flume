package com.cloudera.flume.handlers.thrift;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.net.Socket;

import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;

/**
 * Simple wrapper around {@link TSocket} which adds buffering to the input and output streams.
 * 
 * @author chetan
 *
 */
public class TBufferedSocket extends TSocket {

	public TBufferedSocket(Socket socket) throws TTransportException {
		super(socket);
		wrapStreams();
	}

	public TBufferedSocket(String host, int port, int timeout) {
		super(host, port, timeout);
		wrapStreams();
	}

	public TBufferedSocket(String host, int port) {
		super(host, port);
		wrapStreams();
	}
	
	@Override
	public void open() throws TTransportException {
		super.open();
		wrapStreams();
	}
	
	private void wrapStreams() {
		if (!isOpen() || inputStream_ == null) {
			return;
		}
		inputStream_ = new BufferedInputStream(inputStream_);
		outputStream_ = new BufferedOutputStream(outputStream_);
	}

}
