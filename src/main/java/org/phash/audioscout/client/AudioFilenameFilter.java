package org.phash.audioscout.client;

import java.io.File;
import java.io.FilenameFilter;

public class AudioFilenameFilter implements FilenameFilter {

    public boolean accept(File dir, String name){
	if (name.endsWith(".wav") || name.endsWith(".mp3") || name.endsWith(".amr") || name.endsWith(".aiff") || name.endsWith(".flac")) 
	    return true;
	return false;
    }

}