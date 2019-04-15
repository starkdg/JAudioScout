package org.phash.audioscout.client;

import java.util.List;
import java.util.ArrayList;
import com.beust.jcommander.Parameter;

public class ASDemoOptions {

    @Parameter(names = "-cmd", description = "query, submit, help") 
    public String command = null;

    @Parameter(names = "-dir", description = "source files")
    public String srcdir = null;

    @Parameter(names = "-gdir", description = "hash fingerprint directory")
    public String gdir = null;

    @Parameter(names = "-mdir", description = "metadata index directory")
    public String mdir = null;

    @Parameter(names = "-nsecs", description = "first no. seconds from files to sample - default 0 for whole file")
    public Integer nsecs = 0;

    @Parameter(names = "-p", description = "no. bit toggles - defaults to 0")
    public Integer nbtoggles = 0;
    
    @Parameter(names = "-t", description = "threshold for query or reset threshold on server - default 0.075")
    public Float threshold = 0.075f;

    @Parameter(names = {"-help", "?"}, description = "print usage screen")
    public Boolean help = false;

}