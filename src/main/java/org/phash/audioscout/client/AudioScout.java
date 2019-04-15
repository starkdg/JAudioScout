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
import org.phash.AudioMetaData;
import org.phash.AudioData;
import org.phash.phashaudio.AudioHasher;
import org.phash.phashaudio.AudioHashInfo;
import org.phash.audioscout.hash.QuerySender;
import org.phash.audioscout.hash.MatchResult;

public class AudioScout {

    final private static char[] recordSeparator = { 32, 30, 32 };
	
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
		AuscoutClientOptions options = new AuscoutClientOptions();
		JCommander commander = null;
		try { 
			commander = new JCommander(options, args);
			commander.setProgramName("AudioScout");
		} catch (Exception ex){
			System.out.println("Unable to parse options.  Check parameter usage.");
			commander.usage();
			return;
		}
		
		if (options.help == true || options.command == null){
			commander.usage();
			return;
		}
		
		if (options.command.compareToIgnoreCase("help")==0 || options.serverAddr == null){
			commander.usage();
			return;
		}

		QuerySender sender = new QuerySender(options.serverAddr, 1);

		if (options.command.compareToIgnoreCase("sync") == 0){
			System.out.println("sync ... ");
			String msgStr = sender.sendSync();
			System.out.println("received: " + msgStr);
		} else if (options.command.compareToIgnoreCase("thresh") == 0){
			System.out.println("change threshold to " + options.threshold);
			Float retThreshold = sender.sendThreshold(options.threshold);
			System.out.println("ret: " + retThreshold);
		} else if (options.command.compareToIgnoreCase("delete") == 0){
			System.out.println("send deletes ...");
			String msgStr = sender.sendDeletes(options.idvalue);
			System.out.println("returned: " + msgStr);
		} else {
			File srcdir = new File(options.srcdir);
			AudioHasher hasher = new AudioHasher(options.sr);
			AudioMetaData mdata = new AudioMetaData();
			if (srcdir.isDirectory()){
				File[] files = srcdir.listFiles(new AudioFilenameFilter());
				for (int i=0;i<files.length;i++){
					
					String filename = files[i].getAbsolutePath();
					System.out.printf("file[%d] %s\n", i, filename);
					
					float[] signal = AudioData.readAudio(filename, options.sr, options.nsecs, mdata);
					if (signal == null){
						System.out.printf("skipping:unable to read signal from file\n");
						continue;
					}

					AudioHashInfo hashinfo = hasher.calc(signal, options.nbtoggles);
					
					System.out.printf("no. samples %d, no. frames %d\n", signal.length, hashinfo.hasharray.length);
					
					if (options.command.compareToIgnoreCase("query") == 0){
						System.out.printf("query with p=%d, threshold=%f, nbframes=%d\n", 
										  options.nbtoggles, options.threshold, hashinfo.hasharray.length);
						List<MatchResult> results = sender.sendQuery(hashinfo.hasharray, hashinfo.toggles, 
																	 options.threshold, options.blockSize);
						int count = 0;
						System.out.println("FOUND " + results.size() + " results.");
						for (MatchResult res : results){
							System.out.printf("Found(%d) %s\n", count, res.name);
							System.out.printf("          cs = %f, pos = %d, id = %d\n", res.cs, res.position, res.id);
						}
						
					} else if (options.command.compareToIgnoreCase("submit") == 0){
						String metadataInline = metadataToInlineString(mdata);
						
						System.out.printf("Sending %s\n", metadataInline);
						Integer id = sender.sendSubmission(hashinfo.hasharray, metadataInline);
						System.out.println("assigned id: " + id);
					} else if (options.command.compareToIgnoreCase("print") == 0){
						for (int j=0;j<hashinfo.hasharray.length ;j++){
							System.out.printf(" %10x ", hashinfo.hasharray[j]);
						}
						System.out.println("");
					} else {
						System.out.println("Unrecognized command: " + options.command);
						break;
					}
					System.out.println("----------------------------------------");
				}
			}		
		}
		System.out.println("------------------------Done-----------------------");
		sender.close();
    }
}
