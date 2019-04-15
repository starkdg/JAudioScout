package org.phash.audioscout.util;

import org.phash.audioscout.server.KratiMetaDataStore;
import krati.util.IndexedIterator;
import java.util.Map;
import java.util.Map.Entry;
import java.io.File;
import java.io.IOException;

/** util class to iterate through metadata index
 *  util class to iterate through entries in metadata index
 *  not thread safe
 *  @version 1.0
 *  @author dgs
 **/
public class MetaDataIter {

    protected KratiMetaDataStore mStore = null;


    /**
     * ctor to create class
     * @param homeDir File indicating directory in which index is stored
     * @throws Exception, IOException
     **/
	
    public MetaDataIter(File homeDir) throws Exception, IOException{

		this.mStore = new KratiMetaDataStore(homeDir, 10000, 10, 64, false);

    }

    /** obtain iterator of type krati.util.IndexedIterator<Map.Entry<byte[],byte[]>
     *
     *  @return the iterator
     **/
    public IndexedIterator<Map.Entry<byte[],byte[]>> getIndexedIterator(){

		return mStore.getIndexedIterator();

    }

    /**
     *  close the index
     *  @throws IOException
     **/
    public void close() throws IOException{

		mStore.close();

    }
}
