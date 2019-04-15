package org.phash.audioscout.server;

import java.io.File;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.util.List;
import org.apache.log4j.Logger;

public class Sweeper implements Runnable {

    static private Logger logger = Logger.getLogger(Sweeper.class);

    protected KratiAudioHashStore pStore;

    protected List<Integer> deleteIds;

    public Sweeper(KratiAudioHashStore pStore, List<Integer> deleteList){
		this.pStore = pStore;
		this.deleteIds = deleteList;
    }
    
    public void run(){

		logger.info("sweep ids");
		try {
			if (deleteIds != null && deleteIds.size() > 0){
				logger.info("delete " + deleteIds.size() + " ids");
				pStore.deleteIds(deleteIds);
				pStore.sync();
			}
		} catch (FileNotFoundException ex){
			logger.error("file not found", ex);
		} catch (IOException ex){
			logger.error("unable to read", ex);
		} catch (Exception ex){
			logger.error("unable to finish operation" + ex.getMessage(), ex);
		}
		logger.info("done.");
    }
}
