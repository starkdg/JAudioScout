package org.phash.audioscout.server;

import java.io.File;
import java.io.IOException;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Map;
import krati.store.ObjectStore;
import krati.util.IndexedIterator;
import krati.store.factory.DataStoreFactory;
import krati.store.factory.DynamicDataStoreFactory;
import krati.store.factory.StaticDataStoreFactory;
import krati.store.DynamicDataStore;
import krati.store.StaticDataStore;
import krati.core.segment.SegmentFactory;
import krati.core.segment.MappedSegmentFactory;
import krati.core.segment.MemorySegmentFactory;
import krati.core.StoreConfig;
import krati.io.Closeable;
import krati.io.SerializationException;
import org.apache.log4j.Logger;

public class KratiMetaDataStore implements Closeable {

    static private StaticDataStore mstore = null;
	
    static private Logger logger = Logger.getLogger(KratiMetaDataStore.class);

    public KratiMetaDataStore(File homeDir, int batchSize,  int numSyncBatches, int segmentSize, boolean mmapSegments) 
                                                                       throws Exception, IOException {
		if (mstore == null){
			mstore = createDataStore(homeDir, batchSize, numSyncBatches, segmentSize, mmapSegments);
		}
    }

    public final Object getDataStore(){
		return mstore;
    }

    protected StaticDataStore createDataStore(File storeDir, 
											  int batchSize, 
											  int numSyncBatches, 
											  int segmentSize, boolean mmapSegments) throws IOException {
		StoreConfig config;
		int initcap = 1 << 20;
		double loadfactor = 0.50;
		double compactfactor = 0.50;
		StaticDataStoreFactory storeFactory = new StaticDataStoreFactory();

		config = new StoreConfig(storeDir, initcap);
		config.setBatchSize(batchSize);
		config.setHashLoadFactor(loadfactor);
		config.setIndexesCached(true);
		config.setNumSyncBatches(numSyncBatches);
		config.setSegmentCompactFactor(compactfactor);
		config.setSegmentFactory(createSegmentFactory(mmapSegments));
		config.setSegmentFileSizeMB(segmentSize);
		return storeFactory.create(config);
    }

    protected SegmentFactory createSegmentFactory(boolean mmapSegments){
		if (mmapSegments){
			return new MappedSegmentFactory();
		} else {
			return new MemorySegmentFactory();
		}
    }

    protected byte[] getBytesForInt(int value){
		byte[] bytes = new byte[4];
		bytes[3] = (byte)(value & 0xff);
		bytes[2] = (byte)((value >>> 8) & 0xff);
		bytes[1] = (byte)((value >>> 12) & 0xff);
		bytes[0] = (byte)((value >>> 16) & 0xff);
		return bytes;
    }

    public IndexedIterator<Map.Entry<byte[],byte[]>> getIndexedIterator(){
		return mstore.iterator();
    }

    public int storeMetaData(byte[] mdataBytes) throws Exception, IOException {
		int idValue = 0;
		byte[] byteValues = null;
		byte[] entryBytes = null;
		String mdataStr = new String(mdataBytes);
		StringBuilder keyString = new StringBuilder(mdataStr);
		do {
			keyString.append(System.nanoTime());
			idValue = keyString.toString().hashCode();
			byteValues = getBytesForInt(idValue);
			entryBytes = mstore.get(byteValues);
		} while (entryBytes != null);
	
		if (!mstore.put(byteValues, mdataBytes))
			throw new IOException("unable to store metadata");

		return idValue;
    }
	
    public int storeMetaData(String mdataString) throws Exception, IOException{
		int idValue = 0;
		byte[] byteValues = null;
		byte[] entryBytes = null;
		StringBuilder keyString = new StringBuilder(mdataString);
		do {
    	    keyString.append(System.nanoTime());
			idValue = keyString.toString().hashCode();
			byteValues = getBytesForInt(idValue);
			entryBytes = mstore.get(byteValues);
		} while(entryBytes != null);
		
		if (!mstore.put(byteValues, mdataString.getBytes()))
			throw new IOException("unable to store metadata");
		
		return idValue;
    }


    public String getMetaData(int id){
        byte[] results = mstore.get(getBytesForInt(id));
		String retString = null;
		if (results != null) 
			retString = new String(results);
		return retString;
    }

    public boolean deleteId(int id)throws Exception{
		byte[] byteValues = getBytesForInt(id);
		return mstore.delete(byteValues);
    }

    public void sync() throws IOException {
		mstore.sync();
    }

    @Override
    public boolean isOpen() {
		return mstore.isOpen();
    }

    @Override
    public void open() throws IOException {
		mstore.open();
    }

    @Override
    public void close() throws IOException {
		mstore.close();
    }
}
