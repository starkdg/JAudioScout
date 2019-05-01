package org.phash.audioscout.net;

import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;
import org.zeromq.ZFrame;
import org.zeromq.ZMQ.Socket;

/** 
 *	aux. class for sending commands to auscoutd server app
 *
 *  Not thread-safe
 *
 *  @author dgs
 **/
public class QuerySender {

    private byte[] command = { 0 };
	
    private byte[] parray = { 0 };

    private ZContext zmqctx;

    private Socket skt;

    
    /** Method to send empty message through zeromq socket
     * @param skt  zeromq socket
     * @param flags zeromq flag
     **/
    public static void sendEmpty(Socket skt, int flags){
		byte [] empty = {};
		ZFrame emptyMsg = new ZFrame(empty);
		emptyMsg.sendAndDestroy(skt, flags);
    }

	/** Flush the socket 
	 * @param skt
	 **/
    public static void flushSocket(Socket skt){
		while (skt.hasReceiveMore()){
			skt.recv();
		}
    }

    /** Method to send string through a zeromq socket
     * @param skt 
     * @param msg a string message to be sent
     * @param flags zeromq flags
     **/
    public static void sendString(Socket skt, String msg, int flags){
		ZFrame strMsg = new ZFrame(msg);
		strMsg.sendAndDestroy(skt, flags);
    }

    /** Method to convert byte[] to Float in little endian order
     * @param bytes 
     * @return Float 
     **/
    public static Float convertByteArrayToFloat(byte[] bytes){
		ByteBuffer buff = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
		buff.rewind();
		float val = buff.getFloat();
		return Float.valueOf(val);
    }


    /** Method to convert byte[] to Integer in little endian order
     * @param bytes
     * @return Integer
     **/
    public static Integer convertByteArrayToInteger(byte[] bytes){
		ByteBuffer buff = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
		buff.rewind();
		int val = buff.getInt();
		return Integer.valueOf(val);
    }

    /** Method to send int value through zeromq socket
     * @param skt
     * @param value 
     * @param flags zeromq flags
     **/
    public static void sendInt(Socket skt, int value, int flags){
		byte[] bytearray = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(value).array();
		ZFrame intMsg = new ZFrame(bytearray);
		intMsg.sendAndDestroy(skt, flags);
    }

    /** Method to send int[] through zmq socket
     * @param skt zeromq socket
     * @param intarray 
     * @param flags zeromq send flags
     **/
    public static void sendIntArray(Socket skt, int[] intarray, int flags){
		ByteBuffer tmpbuffer = ByteBuffer.allocate(intarray.length*4).order(ByteOrder.LITTLE_ENDIAN);
		for (int i=0;i<intarray.length;i++){
			tmpbuffer.putInt(intarray[i]);
		}
		ZFrame intarraymsg = new ZFrame(tmpbuffer.array());
		intarraymsg.sendAndDestroy(skt, flags);
    }

    /** Method to send float value through zmq socket 
     * @param skt zmq socket
     * @param value 
     * @param flags zmq send flags
     **/
    public static void sendFloat(Socket skt, float value, int flags){
		byte[] bytearray = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putFloat(value).array();
		ZFrame floatMsg = new ZFrame(bytearray);
		floatMsg.sendAndDestroy(skt, flags);
    }

    /** Method to send byte[][] through zeromq socket
     * @param skt zmq socket
     * @param toggles 
     * @param flags zmq send flags
     **/
    public static  void sendToggles(Socket skt, byte[][] toggles, int flags){
		for (int i=0;i<toggles.length-1;i++){
			ZFrame toggleRowMsg = new ZFrame(toggles[i]);
			toggleRowMsg.sendAndDestroy(skt,ZMQ.SNDMORE);
		}
		ZFrame lastrowmsg = new ZFrame(toggles[toggles.length-1]);
		lastrowmsg.sendAndDestroy(skt, flags);
    }

	/** Method to send int[] through zeromq socket
	 * @param skt zmq socket
	 * @param toggles
	 * @param flags zmq send flags
	 **/
    public static void sendToggles(Socket skt, int[] toggles, int flags){
		sendIntArray(skt, toggles, flags);
    }

    /** Constructor
     * @param address address of auscoutd, e.g. http://localhost:4005
	 * @param nbthreads number I/O threads to use
	 **/
    public QuerySender(String address, int nbthreads){
		this.zmqctx = new ZContext();
		zmqctx.setIoThreads(nbthreads);
		this.skt = zmqctx.createSocket(ZMQ.REQ);
		skt.connect(address);
    }

    /** Send sync command to server
     * @param timeout (in milliseconds)
     * @return String ack message from server
     **/
    public String sendSync(int timeout){
		command[0] = 4;
		ZFrame frameMsg = new ZFrame(command);
		frameMsg.sendAndDestroy(skt, ZMQ.SNDMORE);
		sendEmpty(skt, 0);

		skt.setReceiveTimeOut(timeout);
		ZMsg msg = ZMsg.recvMsg(skt);
		ZFrame respframe = msg.getFirst();
		String result = new String(respframe.getData());
		respframe.destroy();
		msg.destroy();
		
		return result;
    }

    /** Send sync command with no timeout
	 * @return String ack message from server
	 **/
    public String sendSync(){
		return sendSync(-1);
    }

    /** Send threshold value to server. Change threshold for later responses.
     * @param threshold 
     * @param timeout (in millisecs)
     * @return Float returned value from server
     **/
    public Float sendThreshold(float threshold, int timeout){
		command[0] = 5;
		ZFrame cmdMsg = new ZFrame(command);
		cmdMsg.sendAndDestroy(skt, ZMQ.SNDMORE);
		sendFloat(skt, threshold, 0);

		skt.setReceiveTimeOut(timeout);
		ZMsg msg = ZMsg.recvMsg(skt);
		ZFrame ackframe = msg.getFirst();
		Float result = 0.0f;
		if (ackframe.size() == 4) 
			result = convertByteArrayToFloat(ackframe.getData());
		ackframe.destroy();
		msg.destroy();
		
		return result;
    }

    /** Send threshold value to server with no timeout.
	 * @param threshold
	 * @return Float returned value from server 
	 **/
    public Float sendThreshold(float threshold){
		return sendThreshold(threshold, -1);
    }

    /** Send ID value to server for scheduled deletion
     * @param idValue 
     * @param timeout (in millisecs)
     * @return String ack from server
     **/
    public String sendDeletes(Integer idValue, int timeout){
		command[0] = 6;
		ZFrame cmdMsg = new ZFrame(command);
		cmdMsg.sendAndDestroy(skt, ZMQ.SNDMORE);
		sendInt(skt, idValue.intValue(), ZMQ.SNDMORE);
		sendEmpty(skt, 0);
		
		skt.setReceiveTimeOut(timeout);
		ZMsg msg = ZMsg.recvMsg(skt);
		ZFrame ackMsg = msg.getFirst();
		String result = new String(ackMsg.getData());
		msg.destroy();

		return result;
    }

    /** Send ID value to server for scheduled deletion without timeout
	 * @param idValue
	 * @return String ack
	 **/
    public String sendDeletes(Integer idValue){
		return sendDeletes(idValue, -1);
    }
	
    /** Send query command to server 
     * @param hasharray 
     * @param toggles bit positions of each hash frame to toggle to get more lookup candidates.
     * @param threshold
	 * @param blockSize 
     * @param timeout (in millisecs)
     * @return List<MatchResult> List of Matches
     **/;
    public List<MatchResult> sendQuery(int[] hasharray, int[] toggles, float threshold, int blockSize, int timeout){
		command[0] = 7;
		int p = (toggles != null) ? Integer.bitCount(toggles[0]) : 0;

		ZFrame cmdMsg = new ZFrame(command);
		cmdMsg.sendAndDestroy(skt, ZMQ.SNDMORE);
		sendFloat(skt, threshold, ZMQ.SNDMORE);
		sendInt(skt, blockSize, ZMQ.SNDMORE);
		sendIntArray(skt, hasharray, ZMQ.SNDMORE);
		if (toggles != null) sendToggles(skt, toggles, ZMQ.SNDMORE);
		sendEmpty(skt, 0);
    
		skt.setReceiveTimeOut(timeout);
		ArrayList<MatchResult> retList = new ArrayList<MatchResult>();
		ZMsg responseMsg = ZMsg.recvMsg(skt);
		Iterator<ZFrame> iter = responseMsg.iterator();
		while (iter.hasNext()) {
			ZFrame nameFrame = iter.next();
			if (nameFrame == null || !iter.hasNext()) break;
			String name = new String(nameFrame.getData());
			nameFrame.destroy();

			ZFrame csFrame = iter.next();
			if (csFrame == null || csFrame.size() != 4 || !iter.hasNext()) break;
			Float cs = convertByteArrayToFloat(csFrame.getData());
			csFrame.destroy();
			
			ZFrame posFrame  = iter.next();
			if (posFrame == null || posFrame.size() != 4 || !iter.hasNext()) break;
			Integer pos = convertByteArrayToInteger(posFrame.getData());
			posFrame.destroy();

			ZFrame idFrame = iter.next();
			if (idFrame == null || idFrame.size() != 4) break;
			Integer id = convertByteArrayToInteger(idFrame.getData());
			idFrame.destroy();

			retList.add(new MatchResult(name, cs, pos, id));
			
		};
	
		responseMsg.destroy();
		return retList;
    }

    /** 
	 * Send query command to server
	 * @param hasharray
	 * @param toggles
	 * @param threshold
	 * @param blockSize
	 * @return List<MatchResult>
	 **/
    public List<MatchResult> sendQuery(int[] hasharray, int[] toggles, float threshold, int blockSize){
		return sendQuery(hasharray, toggles, threshold, blockSize, -1);
    }

    /** send new submission to server
     * @param hasharray array of hash frames to submit
     * @param metadataStr identifying string to store with submission
     *                    (can be up to 256 characters)
     * @param timeout milliseconds
     * @return Integer new assigned id
     **/
    public Integer sendSubmission(int[] hasharray, String metadataStr, int timeout){
		command[0] = 2;
		int nbframes = hasharray.length;
		ZFrame frame = new ZFrame(command);
		frame.send(skt, ZMQ.SNDMORE);
		sendIntArray(skt, hasharray, ZMQ.SNDMORE);
		sendString(skt, metadataStr, 0);
		
		skt.setReceiveTimeOut(timeout);
		Integer id = 0;
		ZMsg msg = ZMsg.recvMsg(skt);
		if (msg != null && msg.size() > 0 && msg.getFirst().size() == 4){
			id = convertByteArrayToInteger(msg.getFirst().getData());
		}
		return id;
    }
 
    /** Send new submission to server with no timeout
	 *  @param hasharray
	 *  @param metadataStr
	 *  @return Integer new assigned id
	 **/
    public Integer sendSubmission(int[] hasharray, String metadataStr){
		return sendSubmission(hasharray, metadataStr, -1);
    }

    /** Send new submission to server
     * @param hasharray 
     * @param metadataBytes 
     * @param timeout (in millisecs)
     * @return Integer new assigned id
     **/
    public Integer sendSubmission(int[] hasharray, byte[] metadataBytes, int timeout){
		command[0] = 2;
		int nbframes = hasharray.length;

		ZFrame cmdframe = new ZFrame(command);
		cmdframe.sendAndDestroy(skt, ZMQ.SNDMORE);
		sendInt(skt, nbframes, ZMQ.SNDMORE);
		sendIntArray(skt, hasharray, ZMQ.SNDMORE);

		skt.setReceiveTimeOut(timeout);
		Integer id = 0;

		ZMsg msg = ZMsg.recvMsg(skt);
		if (msg != null && msg.size() > 0){
			ZFrame respframe = msg.getFirst();
			if (respframe != null && respframe.size() == 4){
				id = convertByteArrayToInteger(respframe.getData());
				respframe.destroy();
			}
		}
		msg.destroy();
		
		return id;
    }

    /** Send new submission to server with no timeout.
	 * @param hasharray
	 * @param metadataBytes
	 * @return Integer new assigned id
	 **/
    public Integer sendSubmission(int[] hasharray, byte[] metadataBytes){
		return sendSubmission(hasharray, metadataBytes, -1);
    }

    /** 
     * Close connection
     **/
    public void close(){
		zmqctx.close();
    }
}
