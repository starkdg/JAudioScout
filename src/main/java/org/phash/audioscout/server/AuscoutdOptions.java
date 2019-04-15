package org.phash.audioscout.server;

import java.util.List;
import java.util.ArrayList;
import com.beust.jcommander.Parameter;

public class AuscoutdOptions {

    @Parameter(names = "-port", description = "server port")
    public Integer port = 4005;

    @Parameter(names =  "-ipc", description = "use ipc address, ipc://auscoutd")
    public Boolean useIPC = false;

    @Parameter(names = "-mdata", description = "directory location for metadata store")
    public String mdataLocAddress = null;

    @Parameter(names = "-threshold", description = "lookup threshold")
    public Float threshold = 0.070f;

    @Parameter(names = "-blocksize", description = "normalization parameter for queries")
    public int blockSize = 128;

    @Parameter(names = "-wdir", description = "working directory")
    public String workingDir = "";

    @Parameter(names = "-log", description = "log4j log properties file")
    public String logPropertiesFile = "../etc/auscoutd.properties";

    @Parameter(names = "-nbthreads", description = "Number of active server threads")
    public Integer numberThreads = 4;

    @Parameter(names = "-homedir", description = "directory location of kata store")
    public String homeDir = null;

    @Parameter(names = "-sw", description = "hour of day to purge deletes from index (0-23)")
    public int sweeptime = 0;

    @Parameter(names = "-batchsize", description = "number of updates for per update batch")
    public int batchSize = 10000;

    @Parameter(names = "-numsyncbatches", description = "number of update batches for updating datastore")
    public int numSyncBatches = 10;

    @Parameter(names = "-segmentsize", description = "size of segment file")
    public int segmentSize = 64;

    @Parameter(names = "-mmapSegments", description = "memory map segment files")
    public Boolean mmapSegments = false;

    @Parameter(names = "-help", description = "usage information") 
    public boolean help = false;

    @Parameter(names = "-io", description = "no. io threads")
    public Integer iothreads = 1;

}