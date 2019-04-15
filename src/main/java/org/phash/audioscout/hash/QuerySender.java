package org.phash.audioscout.hash;

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

/** auxiliary class to send commands to audio scout server
 *  Not thread-safe
 *  @author dgs
 *  @version 1.0
 **/
public class QuerySender {

    private byte[] command = { 0 };
	
    private byte[] parray = { 0 };

    private ZContext zmqctx;

    private Socket skt;

    
    /** static method to send empty message through zeromq socket
     * @param skt zmq socket
     * @param flags zmq flag
     **/
    public static void sendEmpty(Socket skt, int flags){
		byte [] empty = {};
		ZFrame emptyMsg = new ZFrame(empty);
		emptyMsg.sendAndDestroy(skt, flags);
    }

    public static void flushSocket(Socket skt){
		while (skt.hasReceiveMore()){
			skt.recv();
		}
    }

    /** static method to send string through zeromq socket
     * @param skt zmq socket
     * @param msg string value
     * @param flags zmq flags
     **/
    public static void sendString(Socket skt, String msg, int flags){
		ZFrame strMsg = new ZFrame(msg);
		strMsg.sendAndDestroy(skt, flags);
    }

    /** static method to convert byte[] to Float type in little endian order
     * @param bytes array of bytes, byte[]
     * @return Float value
     **/
    public static Float convertByteArrayToFloat(byte[] bytes){
		ByteBuffer buff = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
		buff.rewind();
		float val = buff.getFloat();
		return Float.valueOf(val);
    }


    /** static method to convert byte[] to Integer in little endian order
     * @param bytes
     * @return Integer value
     **/
    public static Integer convertByteArrayToInteger(byte[] bytes){
		ByteBuffer buff = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
		buff.rewind();
		int val = buff.getInt();
		return Integer.valueOf(val);
    }

    /** static method to send int through zeromq socket
     * @param skt
     * @param value int to send
     * @param flags zeromq send flag
     **/
    public static void sendInt(Socket skt, int value, int flags){
		byte[] bytearray = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(value).array();
		ZFrame intMsg = new ZFrame(bytearray);
		intMsg.sendAndDestroy(skt, flags);
    }

    /** static method to send int[] through zmq socket
     * @param skt zeromq socket
     * @param intarray array of int's
     * @param flags zmq send flags
     **/
    public static void sendIntArray(Socket skt, int[] intarray, int flags){
		ByteBuffer tmpbuffer = ByteBuffer.allocate(intarray.length*4).order(ByteOrder.LITTLE_ENDIAN);
		for (int i=0;i<intarray.length;i++){
			tmpbuffer.putInt(intarray[i]);
		}
		ZFrame intarraymsg = new ZFrame(tmpbuffer.array());
		intarraymsg.sendAndDestroy(skt, flags);
    }

    /** static method to send float value through zmq socket 
     * @param skt zmq socket
     * @param value float value to send
     * @param flags zmq send flags
     **/
    public static void sendFloat(Socket skt, float value, int flags){
		byte[] bytearray = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putFloat(value).array();
		ZFrame floatMsg = new ZFrame(bytearray);
		floatMsg.sendAndDestroy(skt, flags);
    }

    /** aux method to send toggle 2D arrays, byte[][]
     * @param skt zmq socket
     * @param toggles toggle array to send
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

    public static void sendToggles(Socket skt, int[] toggles, int flags){
		sendIntArray(skt, toggles, flags);
    }

    /** constructor
     * @param address audioscout server is running on, e.g. tcp://localhost:4005
     **/
    public QuerySender(String address, int nbthreads){
		this.zmqctx = new ZContext();
		zmqctx.setIoThreads(nbthreads);
		this.skt = zmqctx.createSocket(ZMQ.REQ);
		skt.connect(address);
    }

    /** send synchronization command to server
     * @param timeout milliseconds
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

    /** see above, default timeout, -1 **/
    public String sendSync(){
		return sendSync(-1);
    }

    /** send change threshold message to server
     * @param threshold float value for new threshold
     * @param timeout milliseconds
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

    /** see above with default timeout, -1 **/
    public Float sendThreshold(float threshold){
		return sendThreshold(threshold, -1);
    }

    /** send list of id int's to server to delete
     * @param deleteIds list of int ids represented as strings
     * @param timeout milliseconds
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

    /** see above with default timeout, -1 **/
    public String sendDeletes(Integer idValue){
		return sendDeletes(idValue, -1);
    }
	
    /** send query command to server 
     * @param hasharray array of hash values to look up
     * @param toggles 2d byte array, nbframesxp designating which bit positions of
     *        each hash value most likely to toggle to find more candidate lookups.
     * @param threshold float value for lookup threshold
     * @param timeout milliseconds
     * @return List<MatchResult> list of found items
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

    /** see above with default timeout, -1 **/
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
 
    /** see above, with default timeout, -1 **/
    public Integer sendSubmission(int[] hasharray, String metadataStr){
		return sendSubmission(hasharray, metadataStr, -1);
    }

    /** send new submission to server
     * @param hasharray int[] hash array
     * @param metadataBytes array of bytes, byte[] of metadata
     * @param timeout milliseconds
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

    /** See above, with default timeout, -1 **/
    public Integer sendSubmission(int[] hasharray, byte[] metadataBytes){
		return sendSubmission(hasharray, metadataBytes, -1);
    }

    /** close connection 
     *
     **/
    public void close(){
		zmqctx.close();
    }
}
