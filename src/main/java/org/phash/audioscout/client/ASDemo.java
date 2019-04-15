package org.phash.audioscout.client;

import java.util.List;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.ByteOrder;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMQQueue;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import org.phash.AudioData;
import org.phash.AudioMetaData;
import org.phash.phashaudio.AudioHasher;
import org.phash.phashaudio.AudioHashInfo;
import org.phash.audioscout.hash.MatchResult;
import org.phash.audioscout.server.KratiMetaDataStore;
import org.phash.audioscout.server.KratiAudioHashStore;
import org.phash.audioscout.server.KratiAudioHashStore.FoundId;
import org.apache.log4j.Logger;
import org.apache.log4j.varia.NullAppender;

public class ASDemo {

    final private static char[] recordSeparator = { 32, 30, 32 };

    final private static int SR = 6000;

    final private static int BatchSize = 1000;
	
    final private static int NumSyncBatches = 10;

    final private static int SegmentSize = 64;

    final private static int BlockSize = 128;

    static private KratiMetaDataStore mStore = null;
	
    static private KratiAudioHashStore hStore = null;
	
    public static String metadataToInlineString(AudioMetaData mdata){
		StringBuilder bldr = new StringBuilder();
		if (mdata.composer != null) bldr.append(mdata.composer);
		bldr.append(recordSeparator);
		if (mdata.title1 != null) bldr.append(mdata.title1);
		if (mdata.title2 != null) bldr.append(mdata.title2);
		if (mdata.title3 != null) bldr.append(mdata.title3);
		bldr.append(recordSeparator);
		if (mdata.tpe1 != null) bldr.append(mdata.tpe1);
		if (mdata.tpe2 != null) bldr.append(mdata.tpe2);
		if (mdata.tpe3 != null) bldr.append(mdata.tpe3);
		if (mdata.tpe4 != null) bldr.append(mdata.tpe4);
		bldr.append(recordSeparator);
		if (mdata.date != null) bldr.append(mdata.date);
		if (mdata.genre != null) bldr.append(mdata.genre);
		if (mdata.album != null) bldr.append(mdata.album);
		bldr.append(recordSeparator);
		bldr.append(mdata.year);
		bldr.append(recordSeparator);
		bldr.append(mdata.duration);
		bldr.append(recordSeparator);
		bldr.append(mdata.partofset);
		
		return bldr.toString();
    }

    public static void main(String[] args){
		ASDemoOptions options = new ASDemoOptions();
		JCommander commander = null;

		System.out.printf("\n\n   AudioScout Demo Program 2014   \n\n");
		try { 
			commander = new JCommander(options, args);
			commander.setProgramName("AudioScoutDemo");
		} catch (Exception ex){
			System.out.println("Unable to parse options.  Check parameter usage.");
			commander.usage();
			return;
		}

		if (options.help == true || options.command == null || options.gdir == null || options.mdir == null){
			commander.usage();
			return;
		}
	

		System.out.printf("index dir in %s\n", options.gdir);
		System.out.printf("metadata dir in %s\n", options.mdir);

		try {
			File mdir = new File(options.mdir);
			File gdir = new File(options.gdir);

			if (mdir.isDirectory()){
				mStore = new KratiMetaDataStore(mdir, BatchSize, NumSyncBatches, SegmentSize, true);
			}
			
			if (gdir.isDirectory()){
				hStore = new KratiAudioHashStore(gdir, BatchSize, NumSyncBatches, SegmentSize, true);
			}
		} catch (IOException ex){
			System.out.println("unable to open directory");
			ex.printStackTrace();
			System.exit(0);
		} catch (Exception ex){
			System.out.println("Error occured opening directory.");
			ex.printStackTrace();
			System.exit(0);
		}

		if (mStore == null || hStore == null){
			System.out.printf("unable to open data store\n");
			System.exit(0);
	}
		
	
		System.out.printf("src dir in %s\n", options.srcdir);
		File srcdir = new File(options.srcdir);
		AudioHasher hasher = new AudioHasher(SR);
		AudioMetaData mdata = new AudioMetaData();

		if (srcdir.isDirectory()){
			File[] files = srcdir.listFiles(new AudioFilenameFilter());
			System.out.printf("processing %d files in %s\n\n", files.length, srcdir.getName());
			for (int i=0;i<files.length;i++){
		    
				String filename = files[i].getAbsolutePath();
				System.out.printf("file[%d] %s\n", i, files[i].getName());
				
				float[] signal = AudioData.readAudio(filename, SR, options.nsecs, mdata);
				if (signal == null){
					System.out.printf("no signal ... continue\n\n");
					continue;
				}
				AudioHashInfo hashinfo = hasher.calc(signal, options.nbtoggles);
				if (hashinfo == null){
					System.out.printf("no hash ... continue\n\n");
					continue;
				}
				System.out.printf("no. samples %d, no. frames %d\n", signal.length, hashinfo.hasharray.length);

				if (options.command.compareToIgnoreCase("query") == 0){
					System.out.printf("query with p=%d, threshold=%f\n", 
									  options.nbtoggles, options.threshold);


					List<FoundId> results = hStore.lookupAudioHash(hashinfo.hasharray, 
																   hashinfo.toggles, options.threshold, BlockSize);

					int count = 0;
					System.out.println("FOUND " + results.size() + " results.");
					for (FoundId res : results){
						String mdataStr = mStore.getMetaData(res.id);
						System.out.printf("FOUND: %s cs=%f pos=%d\n", mdataStr, res.cs, res.pos);
					}
		    
				} else if (options.command.compareToIgnoreCase("submit") == 0){
					String metadataInline = metadataToInlineString(mdata);
					
					int id = 0;
					try {
						id = mStore.storeMetaData(metadataInline);
						System.out.println("Storing id = " + id + " (" + metadataInline + ")");
						if (!hStore.storeAudioHash(id, hashinfo.hasharray)){
							System.out.println("(error: failed to store hash.)");
						}
					} catch (Exception ex){
						System.out.printf("error: failed to get id for submission.\n", id);
						ex.printStackTrace();
						break;
					}
					
				} else {
					System.out.println("Unrecognized command: " + options.command);
					break;
				}
				System.out.println("----------------------------------------");
			}
		}		
		
		System.out.println("--------------------Shutdown-Datastore------------");
		try {
			mStore.sync();
			mStore.close();
			hStore.sync();
			hStore.close();
		} catch (IOException ex){
			System.out.printf("Data store not closed properly\n");
			ex.printStackTrace();
			System.exit(1);
		}
		System.out.println("----------------------Done---------------------------");
    }
}
