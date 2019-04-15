package org.phash.audioscout.server;

import java.io.File;
import java.io.IOException;
import java.nio.ByteOrder;
import java.util.List;
import java.util.ArrayList;
import java.util.ListIterator;
import java.util.TreeSet;
import krati.store.ObjectStore;
import krati.util.IndexedIterator;
import krati.store.factory.DynamicObjectStoreFactory;
import krati.store.factory.StaticObjectStoreFactory;
import krati.core.segment.SegmentFactory;
import krati.core.segment.MemorySegmentFactory;
import krati.core.segment.MappedSegmentFactory;
import krati.core.StoreConfig;
import krati.io.Closeable;
import krati.io.serializer.IntSerializer;
import krati.io.SerializationException;
import org.apache.log4j.Logger;

public class KratiAudioHashStore implements Closeable {

    static private int MaxLookups = 10; // maximum number of found Ids that can be returned from lookup function

    static private ObjectStore<Integer,ArrayList<TableValue>> store = null;

    static private Logger logger = Logger.getLogger(KratiAudioHashStore.class);

    public KratiAudioHashStore(File homeDir, int batchSize, int numSyncBatches, int segmentSize, boolean mmapSegments) 
                                                                       throws IOException {
		if (store == null){
			this.store = createObjectStore(homeDir, batchSize, numSyncBatches, segmentSize, mmapSegments);
		}
    }

    public final Object getDataStore(){

		return store;
    }

    protected ObjectStore<Integer, ArrayList<TableValue>> createObjectStore(File storeDir, 
                                                                            int batchSize, 
                                                                            int numSyncBatches, 
                                                                            int segmentSize,
																			boolean mmapSegments) 
		throws IOException {
		StoreConfig config;
		ByteOrder bo = ByteOrder.BIG_ENDIAN;
		int initcap = 1 << 25;
		double lf = 0.50;
		double cf = 0.50;
		//DynamicObjectStoreFactory storeFactory = new DynamicObjectStoreFactory();
		StaticObjectStoreFactory storeFactory = new StaticObjectStoreFactory();

		config = new StoreConfig(storeDir, initcap);
		config.setBatchSize(batchSize);
		config.setHashLoadFactor(lf);
		config.setNumSyncBatches(numSyncBatches);
		config.setSegmentCompactFactor(cf);
		config.setSegmentFactory(createSegmentFactory(mmapSegments));
		config.setSegmentFileSizeMB(segmentSize);
		return storeFactory.create(config, new IntSerializer(bo), new TableValueSerializer());
    }

    protected SegmentFactory createSegmentFactory(boolean mmapSegments){
		if (mmapSegments){
			return new MappedSegmentFactory();
		} else {
			return new MemorySegmentFactory();
		}
    }

    protected int toggleBit(int value, int bit){
		return ((0x80000000 >>> bit)^value);
    }
	
    protected void getCandidates(int hashvalue, byte[] toggles, int[] candidates){
		if (candidates.length > 0){
			candidates[0] = hashvalue;
			for (int i=1; i < candidates.length;i++){
				int currentvalue = hashvalue, perms = i, bitnum = 0;
				while (perms != 0){
					if ((perms & 0x00000001) != 0){
						currentvalue = toggleBit(currentvalue, (int)toggles[bitnum]);
					}
					bitnum++;
					perms >>>= 1;
				}
				candidates[i] = currentvalue;
			}
		}
    }

    protected void getCandidates(int hashvalue, int toggle, int[] candidates){
		if (candidates.length > 0){
			candidates[0] = hashvalue;
			for (int i=1;i < candidates.length;i++){
				int currentvalue = (toggle != 0) ? hashvalue : 0;
				int perms = i;
				int bitnum = 0x80000000;
				while (perms != 0 && bitnum != 0 && toggle != 0){
					while ((bitnum & toggle) == 0 && bitnum != 0) bitnum >>>= 1;
					if ((perms & 0x00000001) != 0){
						currentvalue ^= bitnum;
					}
					perms >>>= 1;
					bitnum >>>= 1;
				}
				candidates[i] = currentvalue;
			}
		}
    }

    public boolean storeAudioHash(Integer id, int[] hashframes) throws Exception {
		boolean result = true;
		int prevhash = 0;
		TableValue tblValue = null;
		for (int i = 0;i < hashframes.length;i++){
			if (hashframes[i] == 0 || hashframes[i] == prevhash){
				continue;
			} 
			tblValue = new TableValue();
			tblValue.id = id.intValue();
			tblValue.pos = i;
			ArrayList<TableValue> entrylist = store.get(hashframes[i]);
			if (entrylist == null) entrylist = new ArrayList<TableValue>();
			if (entrylist.size() < 4) entrylist.add(tblValue);
			result = store.put(hashframes[i], entrylist);
			prevhash = hashframes[i];
		}
		store.persist();
		return result;
    }

    private class TrackerId {
		int id;
		int startpos;
		int lastpos;
		int count;
    }

    public class FoundId {
		public int id;
		public int pos;
		public float cs;
		public int count;
    }

    public ArrayList<FoundId> lookupAudioHash(int[] hashframes, int[] toggles, 
                                              float threshold, int blockSize){
		int p = (toggles != null && toggles.length > 0) ? Integer.bitCount(toggles[0]) : 0;
		int nbcandidates = 1 << p;
		ArrayList<FoundId> results = new ArrayList<FoundId>();
		ArrayList<TrackerId> tracker = new ArrayList<TrackerId>();
		int[] candidates = new int[nbcandidates];
		int advance = blockSize;
		int windowWidth = 200;
		for (int i=0;i<hashframes.length-blockSize;i+=advance){
			for (int j=i;j<i+blockSize;j++){
				// get candidate matches for this frame
				if (toggles == null) {
					getCandidates(hashframes[j], 0, candidates);
				} else {
					int toggle = (j < toggles.length) ? toggles[j] : 0;
					getCandidates(hashframes[j], toggle, candidates);
				}
				// look up all candidates found for this frame
				for (int k=0;k<nbcandidates;k++){
					Integer key = candidates[k];
					ArrayList<TableValue> entryList = store.get(key);
					if (entryList != null){
						for (TableValue entry : entryList){
							boolean needToAdd = true;
							for (TrackerId track : tracker){
								if (entry.id == track.id) {
									needToAdd = false;
									if (entry.pos > track.lastpos &&  entry.pos <= track.lastpos + windowWidth){
										track.count++;
										track.lastpos = entry.pos;
									}
									break;
								} 
							}
							if (needToAdd){
								TrackerId newTrack = new TrackerId();
								newTrack.id = entry.id;
								newTrack.startpos = entry.pos;
								newTrack.lastpos = entry.pos;
								newTrack.count = 1;
								tracker.add(newTrack);
							}
						}
					}
				}
			}
			for (TrackerId track : tracker){
				float score = (float)track.count/(float)blockSize;
				if (score >= threshold){
					boolean addToFound = true;
					for (FoundId fnd : results){
						if (track.id == fnd.id){
							addToFound = false;
							fnd.cs += score;
							fnd.count += 1;
							break;
						}
					}
					if (addToFound){
						FoundId fnd = new FoundId();
						fnd.id = track.id;
						fnd.pos = track.startpos;
						fnd.cs = score;
						fnd.count = 1;
						results.add(fnd);
					}
				} 
			}
			tracker.clear();
			if (results.size() >= MaxLookups) {
				break;
			}
		}
		
		// filter results according to accumulated confidence score
		ListIterator<FoundId> iter = results.listIterator();
		while (iter.hasNext()){
			FoundId elem = iter.next();
			if (elem.cs < 2*threshold){
				iter.remove();
			}
		}
		
		return results;
    }

    public void deleteIds(List<Integer> idsList) throws IOException, Exception{

		IndexedIterator<Integer> iter = store.keyIterator();
		while (iter.hasNext()){
			Integer key = iter.next();
			ArrayList<TableValue> entryList = store.get(key);
			if (entryList.size() > 0){
				for (TableValue tblval : entryList){
					boolean found = false;
					for (Integer id:idsList){
						if (id == tblval.id){
							found = true;
							entryList.remove(tblval);
							break;
						}
					}
					if (found) break;
				}
				store.put(key, entryList);
			}
		}
		store.persist();
    }

    public void sync() throws IOException {
		store.sync();
    }

    @Override
    public boolean isOpen() {
		return store.isOpen();
    }

    @Override
    public void open() throws IOException {
		store.open();
    }

    @Override
    public void close() throws IOException {
		store.close();
    }
}
