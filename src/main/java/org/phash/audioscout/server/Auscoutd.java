package org.phash.audioscout.server;

import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.Calendar;
import java.io.File;
import java.io.IOException;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMQQueue;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import org.apache.log4j.Logger;
import org.apache.log4j.Appender;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.SimpleLayout;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.RollingFileAppender;
import org.apache.log4j.PropertyConfigurator;

public class Auscoutd {
    public static final String tcpAddress = "tcp://*:";
    public static final String ipcAddress = "ipc://auscoutd";
    public static final String InprocAddress = "inproc://queries";
    public static final String QueueThreadName = "QueueThread";
    public static final int SweepPeriod = 1000*60*60*24; // no. milliseconds in 24 hours
    public static final int DeleteListSize = 128;
    static private Logger logger = Logger.getLogger(Auscoutd.class);
    static private ZContext zmqctx;
    static private Socket frontSocket = null;
    static private Socket backSocket = null;
    static private Thread queueThread = null;
    static private ZMQQueue queue = null;
    static private Thread workers[] = null;
    static private WorkerThread workerThrs[] = null;
    static private ScheduledFuture<?> sweepHandler = null;
    static private KratiMetaDataStore mStore = null;
    static private KratiAudioHashStore pStore = null;

    static protected void addDaemonShutdownHook(){
		Runtime.getRuntime().addShutdownHook(new Thread(){ 
				public void run(){
					try {
						logger.debug("shutdown");
						sweepHandler.cancel(false);
						for (int i=0;i<workerThrs.length;i++){
							workerThrs[i].close();
							workers[i].interrupt();
							workers[i].join();
						}
						logger.info("Close metadata datastore.");
						mStore.close();
						
						logger.info("Close hash datastore.");
						pStore.close();
						
						logger.info("Close sockets.");
						frontSocket.close();
						backSocket.close();
						queue.close();
						queueThread.join();
						zmqctx.close();
					} catch (Exception ex){
						logger.error("Unable to complete shutdown process: " + ex.getMessage());
					}
					logger.info("GoodByte.");
				}
			});
    }

    public static void daemonize() throws IOException {
		String propKey = System.getProperty("auscoutd.pidfile");
		if (propKey != null && !propKey.isEmpty()){
			File pidFile = new File(propKey);
			pidFile.deleteOnExit();
		}
		System.out.close();
		System.in.close();
    }
	
    public static void main(String[] args){
      
		List<Integer> deleteIdList = Collections.synchronizedList(new ArrayList<Integer>(DeleteListSize));

		Appender startupAppender = new ConsoleAppender(new SimpleLayout(), "System.err");

		try {
			AuscoutdOptions options = new AuscoutdOptions();

			// parse parameter options
			JCommander commander = new JCommander(options, args);
			commander.setProgramName("Auscoutd");
			PropertyConfigurator.configure(options.logPropertiesFile);
			logger.addAppender(startupAppender);
			
			if (options.help == true){
				logger.info("print usage info");
				commander.usage();
				return;
			}

			StringBuilder mainAddress = new StringBuilder();
			if (options.useIPC){
				mainAddress.append(ipcAddress);
			} else {
				mainAddress.append(tcpAddress);
				mainAddress.append(options.port);
			} 
			
			File storeLocation = null;
			if (options.homeDir != null && !options.homeDir.isEmpty()){
				storeLocation = new File(options.homeDir);
				if (!storeLocation.isDirectory()){
					logger.fatal("store location is not a directory");
					return;
				}
			} else {
				logger.info("no home dir given on command line");
				commander.usage();
				return;
			}

			
			pStore = new KratiAudioHashStore(storeLocation, options.batchSize, options.numSyncBatches, options.segmentSize, false);
			if (pStore == null){
				logger.error("unable to open audio hash store");
				return;
			}

			File mdataLocation = null;
			if (options.mdataLocAddress != null && !options.mdataLocAddress.isEmpty()){
				mdataLocation = new File(options.mdataLocAddress);
				if (!mdataLocation.isDirectory()){
					logger.fatal("mdata location is not a directory");
					return;
				}
			} else {
				logger.info("no metadata dir given on command line");
				commander.usage();
				return;
			}
			
			
			mStore = new KratiMetaDataStore(mdataLocation, options.batchSize, options.numSyncBatches, options.segmentSize, false);
			if (mStore == null){
				logger.error("unable to open metadata store");
				return;
			}
			
			if (options.sweeptime < 0 || options.sweeptime > 23){
				logger.info("invalid value for hour sweep time given - [0-23]");
				commander.usage();
				return;
			}


			logger.info("home dir " + options.homeDir);
			logger.info("mdata dir " + options.mdataLocAddress);
			logger.info("Auscoutd " + mainAddress.toString());
			
			// setup messaging
			zmqctx = new ZContext();
			zmqctx.setIoThreads(options.iothreads);
			
			logger.debug("create front socket");
			frontSocket = zmqctx.createSocket(ZMQ.ROUTER);
			
			logger.debug("bind to " + mainAddress.toString());
			frontSocket.bind(mainAddress.toString());
			
			logger.debug("create back socket");
			backSocket = zmqctx.createSocket(ZMQ.DEALER);
			
			logger.debug("bind to " + InprocAddress);
			backSocket.bind(InprocAddress);
			
			logger.debug("create worker threads");
			workers = new Thread[options.numberThreads];
			workerThrs = new WorkerThread[options.numberThreads];
			for (int i=0;i < options.numberThreads;i++){
				String threadName = "auworker" + i;
				logger.info("worker thread " + i);
				
				workerThrs[i] = new WorkerThread(InprocAddress, mStore, zmqctx, pStore,  
												 options.threshold, i, options.blockSize, deleteIdList);
		
				workers[i] = new Thread(workerThrs[i]);
	    
				workers[i].setName(threadName);
				workers[i].setDaemon(true);
				workers[i].start();

				Thread.sleep(100);
			}

			Calendar cal = Calendar.getInstance();
			int nowhour = cal.get(Calendar.HOUR_OF_DAY);
			int delta = (options.sweeptime > nowhour) ? options.sweeptime - nowhour : 24 - nowhour + options.sweeptime;
			cal.add(Calendar.HOUR_OF_DAY , delta );
			
			logger.info("schedule sweeper for " + cal.toString());
			
			//sweeper thread
			Sweeper sweeper = new Sweeper(pStore, deleteIdList);
			
			ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
			
			long delay = cal.getTimeInMillis() - System.currentTimeMillis();
			
			sweepHandler = scheduler.scheduleAtFixedRate(sweeper,delay,SweepPeriod,TimeUnit.MILLISECONDS);
			
			//start daemon
			logger.info("daemonize");
			daemonize();
			addDaemonShutdownHook();
		} catch (IOException ex){
			logger.fatal("unable to start worker threads", ex);
			return;
		} catch (Throwable ex){
			logger.fatal("unable to start daemon" , ex);
			return;
		} 
		logger.removeAppender(startupAppender);
		
		try {
			ZMQ.proxy(frontSocket, backSocket, null);
		} catch (Throwable ex){
			logger.error("unable to start proxy: " + ex.getMessage());
			logger.fatal("unable to start proxy msg'ing", ex);
			System.exit(0);
		}
    }
}
