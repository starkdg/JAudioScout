package org.phash.audioscout.server;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Iterator;
import org.zeromq.ZContext;
import org.zeromq.ZFrame;
import org.zeromq.ZMsg;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMQ.Poller;
import org.zeromq.ZMQ.PollItem;
import org.zeromq.ZMQException;
import org.apache.log4j.Logger;
import krati.io.SerializationException;

public class WorkerThread implements Runnable {

    static protected final long Timeout = 50;
    static protected Float threshold = 0.075f;
    static protected int blockSize = 128;
    static protected final String defaultReplyMsg = "NOT FOUND";
    static protected final String emptyMsg = "";
    static protected final String successMsg = "success";
    static protected final String failMsg = "fail";
    static protected final String DeletesFile = "audeletes.txt";
    protected ZContext ctx;
    protected String address;
    protected int threadnumber;
    protected KratiAudioHashStore pStore = null;
    protected KratiMetaDataStore mStore = null;
    static protected List<Integer> deleteIdsList;
    static private Logger logger = Logger.getLogger(WorkerThread.class);
    boolean stop = false;

    public WorkerThread(String address, KratiMetaDataStore mStore, ZContext ctx, KratiAudioHashStore pStore, 
                        Float threshold, int i, int blockSize, List<Integer> deletes) throws Exception, IOException{
		this.ctx = ctx;
		this.address = address;
		this.threshold = threshold;
		this.threadnumber = i;
		this.blockSize = blockSize;
		this.deleteIdsList = deletes;
		this.pStore = pStore;
		this.mStore = mStore;
    }
    
    public void close(){
		stop = true;
    }
	
    protected void sendEmpty(Socket skt, int flags){
		byte [] empty = {};
		ZFrame emptyMsg = new ZFrame(empty);
		emptyMsg.sendAndDestroy(skt, flags);
    }

    protected void sendString(Socket skt, String msg, int flags){
		ZFrame strMsg = new ZFrame(msg);
		strMsg.sendAndDestroy(skt, flags);
    }

    protected void sendInt(Socket skt, int value, int flags){
		ByteBuffer buff = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(value);
		buff.rewind();
		ZFrame intMsg = new ZFrame(buff.array());
		intMsg.sendAndDestroy(skt, flags);
    }

    protected void sendFloat(Socket skt, float value, int flags){
		ByteBuffer buff = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putFloat(value);
		buff.rewind();
		ZFrame floatMsg = new ZFrame(buff.array());
		floatMsg.sendAndDestroy(skt, flags);
    }

    protected void pause(long millis) throws InterruptedException {
		Thread.sleep(millis);
    }
    
    protected Integer convertByteArrayToInteger(byte[] bytes){
		ByteBuffer buf = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
		buf.rewind();
		int val = buf.getInt();
		return Integer.valueOf(val);
    }

    protected Float convertByteArrayToFloat(byte[] bytes){
		ByteBuffer buf = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
		buf.rewind();
		float val = buf.getFloat();
		return Float.valueOf(val);
    }
	
    protected int[] convertByteArrayToIntArray(byte[] bytes){
		IntBuffer buf = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).asIntBuffer();
		buf.rewind();
		int[] intarray = new int[buf.limit()];
		for (int i=0;i < intarray.length;i++){
			intarray[i] = buf.get(i);
		}
		return intarray;
    }
	
    protected int[] retrieveToggles(Iterator<ZFrame> iter){
		int[] toggles = null;
		ZFrame frame = iter.next();
		if (frame != null){
			byte[] togglebytes = frame.getData();
			toggles = convertByteArrayToIntArray(togglebytes);
		}
		return toggles;
    }
	
    protected ArrayList<String>  getMetaDataFromIds(ArrayList<KratiAudioHashStore.FoundId> resultIds){
		ArrayList<String> results = new ArrayList<String>();
		for (KratiAudioHashStore.FoundId currentId: resultIds){
			logger.debug("metadata lookup for id " + currentId.id);
			String mdataStr = mStore.getMetaData(currentId.id);
			if (mdataStr != null && !mdataStr.isEmpty())
				results.add(mdataStr);
		}
		return results;
    }

    protected void storeDeleteId(int deleteValue){
		try {
			StringBuilder ss = new StringBuilder();
			Calendar now = Calendar.getInstance();
			int day = now.get(Calendar.DAY_OF_YEAR);
			int hour = now.get(Calendar.HOUR_OF_DAY);
			int min =  now.get(Calendar.MINUTE);
			ss.append(day);
			ss.append("d-");
			ss.append(hour);
			ss.append("h-");
			ss.append(min);
			ss.append("m");
			
			File delsFile = new File(DeletesFile);
			if (!delsFile.exists()) delsFile.createNewFile();

			FileWriter writer = new FileWriter(delsFile, true);
			writer.write(ss.toString());
			writer.write(Integer.toString(deleteValue));
			writer.write("\n");
			writer.flush();
			writer.close();
		} catch (Exception ex){
			logger.error("unable to write delete id to backup" + ex.getMessage(), ex);
		}
    }
	
    protected void handleDeletes(Iterator<ZFrame> iter, Socket qskt){
		try {
			while (iter.hasNext()){
				ZFrame frame = iter.next();
				if (frame != null && frame.size() == 4){
					Integer delvalue = convertByteArrayToInteger(frame.getData());
					logger.debug("delete id " + delvalue);
					deleteIdsList.add(delvalue);
					mStore.deleteId(delvalue);
					storeDeleteId(delvalue);
				}
			}
		} catch (Exception ex){
			logger.error("unable to complete id op" + ex.getMessage(), ex);
			sendString(qskt, failMsg, 0);
			return;
		}
		sendString(qskt, successMsg, 0);
    }

    protected void handleSubmission(Iterator<ZFrame> iter, Socket qskt, int[] hasharray){
		int idvalue;
		if (iter.hasNext()){
			ZFrame mdataFrame = iter.next();
			logger.debug("received mdata: " + mdataFrame.toString());
			
			// send reply 
			try {
				idvalue = mStore.storeMetaData(mdataFrame.getData());
				if (idvalue != 0 && pStore.storeAudioHash(idvalue, hasharray)){
					logger.debug("hash successfully stored" + idvalue);
					sendInt(qskt, idvalue, 0);
				} else {
					logger.error("unable to store audio hash");
					sendInt(qskt, 0, 0);
				}
			} catch (Throwable ex){
				logger.error("unable to store audio hash", ex);
				sendInt(qskt, 0, 0);
			}
		} else {
			// reply to query socket with 0 id
			logger.debug("no metadata sent");
			sendInt(qskt, 0, 0);
		}
    }
    protected void handleQuery(Iterator<ZFrame> iter, Socket qskt, int[] hasharray){
		ArrayList<KratiAudioHashStore.FoundId> resultIds = null;
		ArrayList<String> results = null;
		int[] toggles = null;
		if (iter.hasNext()){
			toggles = retrieveToggles(iter);
		}
		try {
			int nbtoggles = (toggles != null) ? toggles.length : 0;
			logger.debug("lookup frames " + hasharray.length + "toggles " + nbtoggles + "; threshold " + threshold);
			resultIds = pStore.lookupAudioHash(hasharray, toggles, threshold, blockSize);
			results = getMetaDataFromIds(resultIds);
			for (String res : results){
				sendString(qskt, res, ZMQ.SNDMORE);
				logger.debug("found: " + res);
			} 
		} catch (Throwable ex){
			logger.error("Unable to perform lookup - ", ex);
		} 
		sendEmpty(qskt, 0);
    }
	
    protected void handleQuery2(Iterator<ZFrame> iter, Socket qskt, int[] hasharray, 
								float lookupThreshold, int localBlockSize){
		ArrayList<KratiAudioHashStore.FoundId> resultIds = null;
		ArrayList<String> results = null;
		int[] toggles = null;
		if (iter.hasNext()){
			toggles = retrieveToggles(iter);
		}
		try {
			logger.debug("lookup frames " + hasharray.length + "toggles " + toggles.length + 
						 "; threshold " + lookupThreshold);
			resultIds = pStore.lookupAudioHash(hasharray, toggles, lookupThreshold, localBlockSize);
			results = getMetaDataFromIds(resultIds);
			for (int i=0;i < results.size();i++){
				sendString(qskt, results.get(i), ZMQ.SNDMORE);
				float cs = resultIds.get(i).cs;
				sendFloat(qskt, cs, ZMQ.SNDMORE);
				sendInt(qskt, resultIds.get(i).pos, ZMQ.SNDMORE);
				sendInt(qskt, resultIds.get(i).id, ZMQ.SNDMORE);
				logger.debug("found: " + results.get(i) + " " + resultIds.get(i).cs + " " 
							 + resultIds.get(i).pos + " " + resultIds.get(i).id);
			}
		} catch (Throwable ex){
			logger.error("Unable to perform lookup - ", ex);
		} 
		sendEmpty(qskt, 0);
    }

    protected void handleRequest(ZMsg msg, Socket qskt) throws Exception {

		Iterator<ZFrame> iter = msg.iterator();
		ZFrame cmdframe = iter.next();
		if (cmdframe == null || cmdframe.size() != 1 || !iter.hasNext()){
			sendInt(qskt, 0, 0);
			return;
		}
		byte[] cmd = cmdframe.getData();
		int command = (int)cmd[0];
		
		logger.debug("command byte: " + command);
		Float threshold_local = threshold;
		Integer blocksize_local = blockSize;
		
		//non-hash admin commmands
		if (command == 4){ // sync command
			logger.info("recieved sync command");
			pStore.sync();
			mStore.sync();
			sendString(qskt,successMsg,0);
			return;
		} else if (command == 5){ // change threshold command
			ZFrame thresholdFrame = iter.next();
			if (thresholdFrame != null && thresholdFrame.size() == 4){
				WorkerThread.threshold = convertByteArrayToFloat(thresholdFrame.getData());
			}
			sendFloat(qskt, WorkerThread.threshold, 0);
			logger.debug("received change threshold cmd - " + WorkerThread.threshold);
			return;
		} else if (command == 6){ // delete entry by id command
			logger.debug("recieved delete command");
			handleDeletes(iter, qskt);
			return;
		} 
		
		if (command == 7){
			ZFrame threshFrame = iter.next();
			if (threshFrame != null && threshFrame.size() == 4)
				threshold_local = convertByteArrayToFloat(threshFrame.getData());
			
			ZFrame bsFrame = iter.next();
			if (bsFrame != null && bsFrame.size() == 4)
				blocksize_local = convertByteArrayToInteger(bsFrame.getData());
		}

		ZFrame hashFrame = iter.next();
		if (hashFrame == null || hashFrame.size()%4!=0){
			logger.debug("hash data msg part bad - continuing");
			sendInt(qskt, 0, 0);
			return;
		}
		
		// handle hash commands
		int[] hasharray = convertByteArrayToIntArray(hashFrame.getData());
		logger.debug("hash array recieved: " + hasharray.length);
		
		switch (command) { 
		case 1: //query
			handleQuery(iter, qskt, hasharray);
			break;
		case 2: //submit
			handleSubmission(iter, qskt, hasharray);
			break;
		case 3:
			handleQuery2(iter, qskt, hasharray, threshold, blockSize);
			break;
		case 7:
			handleQuery2(iter, qskt, hasharray, threshold_local, blocksize_local);
			break;
		default:
			logger.debug("no such cmd - " + command);
		}
		
		return;
    }
	
    public void run(){
		Socket qskt = null;
		try {
			logger.info("start worker thread " + threadnumber);
			qskt = ctx.createSocket(ZMQ.REP);
			qskt.connect(address);
			
			PollItem[] items = new PollItem[] { new PollItem(qskt, Poller.POLLIN)};
			
			while (!Thread.currentThread().isInterrupted() && !stop){
				int nbevents = ZMQ.poll(items, 10);
				while (nbevents > 0 && items[0].isReadable()){
					ZMsg msg = ZMsg.recvMsg(qskt, ZMQ.DONTWAIT); 
					if (msg != null && msg.size() > 0) {
						handleRequest(msg, qskt);
						msg.destroy();
					}
					nbevents--;
				}
			}
			qskt.disconnect(address);
			qskt.close();
		} catch (Throwable ex){
			logger.fatal("unable to run worker thread", ex);
		}
		logger.info("Done.");
    }
}
