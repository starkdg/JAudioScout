package org.phash.audioscout.util;

import org.phash.audioscout.server.KratiMetaDataStore;
import krati.util.IndexedIterator;
import java.util.Map;
import java.util.Map.Entry;
import java.io.IOException;
import java.io.File;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/** @brief util class to iterate through metadata index
 *  @summary util class to iterate through entries in metadata index
 *           NOT thread safe
 *  @version 1.0
 *  @author dgs
 **/
public class MDataScroll {

    public static Integer convertByteArrayToInteger(byte[] bytes){
		ByteBuffer buff = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
		buff.rewind();
		int val = buff.getInt();
		return Integer.valueOf(val);
    }

    public static void main(String[] args){
		if (args.length < 1){
			System.out.println("not enough args");
			System.out.println("prog dir");
			System.exit(0);
		}
		String dirname = args[0];
		
		System.out.println("Opening index at " + dirname);

		try {
			File homedir = new File(dirname);
			
			if (homedir != null && homedir.exists() && homedir.isDirectory()){
				
				MetaDataIter index = new MetaDataIter(homedir);
		
				if (index != null){
					IndexedIterator<Map.Entry<byte[],byte[]>> iter = index.getIndexedIterator();
					
					int n = 0;
					Integer keyInt = 0;
					String valueStr;
					while (iter.hasNext()){
						Map.Entry<byte[], byte[]>  entry = iter.next();
						
						
						byte[] key = entry.getKey();
						byte[] value = entry.getValue();
						
						if (key != null && key.length == 4)
							keyInt = convertByteArrayToInteger(key);
						valueStr = new String(value);
						
						System.out.println(n + " key: " + keyInt + " value: " + valueStr);
						
						n++;
					}
					System.out.println("Total entries: " + n);
					index.close();
				} else {
					System.out.println("Unable to open index.");
				}
			} else {
				System.out.println("No such directory available: " + homedir);
			}
		} catch (Exception ex){
			System.out.println("unable to read index: " + ex.getMessage());
			ex.printStackTrace();
		}
		System.out.println("Done.");
    }
}
