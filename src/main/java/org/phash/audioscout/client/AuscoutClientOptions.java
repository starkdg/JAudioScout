package org.phash.audioscout.client;

import java.util.List;
import java.util.ArrayList;
import com.beust.jcommander.Parameter;

public class AuscoutClientOptions {

    @Parameter(names = "-cmd", description = "query, submit, sync, thresh, delete, print, help") 
    public String command = null;

    @Parameter(names = "-dir", description = "source files")
    public String srcdir = null;

    @Parameter(names = "-addr", description = "address for auscoutd server - e.g. tcp://localhost:4005 or ipc://auscoutd")
    public String serverAddr = null;

    @Parameter(names = "-nsecs", description = "first no. seconds from files to sample - default 0 for whole file")
    public Integer nsecs = 0;

    @Parameter(names = "-p", description = "no. bit toggles - defaults to 0")
    public Integer nbtoggles = 0;
    
    @Parameter(names = "-t", description = "threshold for query or reset threshold on server - default 0.075")
    public Float threshold = 0.075f;

    @Parameter(names = "-b", description = "block size for query")
    public Integer blockSize = 128;

    @Parameter(names = {"-id"}, description = "id value to delete")
    public Integer idvalue = 0;

    @Parameter(names = "-sr", description = "sample rate, default 6000 samples/sec")
    public Integer sr = 6000;

    @Parameter(names = {"-help", "?"}, description = "print usage screen")
    public Boolean help = false;

}