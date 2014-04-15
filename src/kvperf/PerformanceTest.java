package kvperf;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import krati.core.StoreConfig;
import krati.core.StoreFactory;
import krati.core.StoreParams;
import krati.core.segment.WriteBufferSegmentFactory;
import krati.store.index.HashIndexDataHandler;

import org.apache.log4j.Logger;

public class PerformanceTest {

	private static final Logger logger = Logger.getLogger(PerformanceTest.class);

	private static final int NUM_SAMPLES = 100000;
	private static final int DEFAULT_DONT_MEASURE_SECS = 120;

	private KVStore store;
	private final int threads;
	private final int scanThreads;
	private final int scanThreadSleepMs;
	private final long operations;
	private final long runTimeSecs;
	private final int throughput;
	private final int numKeys;
	private final int minValueSize;
	private final int maxValueSize;
	private final int keySize;
	private final long reportingInterval;
	private final double gets;
	private final double puts;
	private final double deletes;
	private final boolean warmUp;
	private final boolean prepopulate;
	private final File resultsDirectory;
	private PrintStream latencyOutFile;

	public PerformanceTest(KVStore store,
			int threads,
			int scanThreads,
			int scanThreadSleepMs,
			int throughput,
			long operations,
			long runTimeSecs,
			int numKeys,
			int minValueSize,
			int maxValueSize,
			int keySize,
			long reportingInterval,
			double gets,
			double puts,
			double deletes,
			boolean warmUp,
			boolean prepopulate,
			File resultsDirectory) {
		this.store = store;
		this.threads = threads;
		this.scanThreads = scanThreads;
		this.scanThreadSleepMs = scanThreadSleepMs;
		this.operations = operations;
		this.runTimeSecs = runTimeSecs;
		this.throughput = throughput;
		this.numKeys = numKeys;
		this.minValueSize = minValueSize;
		this.maxValueSize = maxValueSize;
		this.keySize = keySize;
		this.reportingInterval = reportingInterval;
		this.gets = gets;
		this.puts = puts;
		this.deletes = deletes;
		this.warmUp = warmUp;
		this.prepopulate = prepopulate;
		this.resultsDirectory = resultsDirectory;
		try {
			this.latencyOutFile = new PrintStream(new File(this.resultsDirectory, "latency.out"));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		
		double total = this.gets + this.puts + this.deletes;
		if(Math.abs(total - 1.0) > 0.01)
			throw new IllegalArgumentException("Reads, writes and deletes should add up to 1.0, but got " + total + ".");
	}
	
	public PrintStream getLatencyOutFile() {
		return latencyOutFile;
	}

	public static void main(String[] args) throws Exception {
		OptionParser parser = new OptionParser();
		OptionSpec<Integer> threadsOpt = 
				parser.accepts("threads", "The number of concurrent threads making requests.")
				.withRequiredArg()
				.describedAs("num_threads")
				.ofType(Integer.class)
				.defaultsTo(1);
		OptionSpec<Integer> scanThreadsOpt = 
				parser.accepts("scan-threads", "The number threads doing background scans on the store.")
				.withRequiredArg()
				.describedAs("scan_threads")
				.ofType(Integer.class)
				.defaultsTo(0);
		OptionSpec<Integer> scanThreadSleepOpt = 
				parser.accepts("scan-thread-sleep", "The time to sleep (in ms) between scans.")
				.withRequiredArg()
				.describedAs("scan_thread_sleep_ms")
				.ofType(Integer.class)
				.defaultsTo(0);
		OptionSpec<Integer> throughputOpt = 
				parser.accepts("throughput", "The target throughput (req/sec) to maintain.")
				.withRequiredArg()
				.describedAs("req_per_sec")
				.ofType(Integer.class)
				.defaultsTo(-1);
		OptionSpec<Long> opsOpt = 
				parser.accepts("ops", "The total number of operations to do.")
				.withRequiredArg()
				.describedAs("ops")
				.ofType(Long.class)
				.defaultsTo(0L);
		OptionSpec<Long> runTimeOpt =
				parser.accepts("runtime", "Total number of seconds to run workload.")
				.withRequiredArg()
				.describedAs("runtime_secs")
				.ofType(Long.class)
				.defaultsTo(600L);
		OptionSpec<?> warmupOpsOpt = 
				parser.accepts("warm-up", "warmp up the store before experiments begin");
		OptionSpec<Integer> numKeysOpt = 
				parser.accepts("num-keys", "The number of unique keys to use if not reading keys from a file.")
				.withRequiredArg()
				.describedAs("num_keys")
				.ofType(Integer.class)
				.defaultsTo(10000000);
		OptionSpec<Integer> keySizeOpt = 
				parser.accepts("key-size", "The number of bytes in the key.")
				.withRequiredArg()
				.describedAs("key_size_bytes")
				.ofType(Integer.class)
				.defaultsTo(8);
		OptionSpec<Integer> maxValueSizeOpt = 
				parser.accepts("max-value-size", "The max number of bytes in the values.")
				.withRequiredArg()
				.describedAs("max_value_size_bytes")
				.ofType(Integer.class)
				.defaultsTo(100);
		OptionSpec<Integer> minValueSizeOpt = 
				parser.accepts("min-value-size", "The min number of bytes in the values.")
				.withRequiredArg()
				.describedAs("min_value_size_bytes")
				.ofType(Integer.class)
				.defaultsTo(1);
		OptionSpec<Integer> reportingIntervalOpt = 
				parser.accepts("reporting-interval", "The number of seconds in between perf stat reports.")
				.withRequiredArg()
				.describedAs("reporting_interval_secs")
				.ofType(Integer.class)
				.defaultsTo(Integer.MAX_VALUE);
		OptionSpec<Double> percentGetsOpt = 
				parser.accepts("percent-gets", "The percentage of requests that are gets [0-100].")
				.withRequiredArg()
				.describedAs("percent_gets")
				.ofType(Double.class)
				.defaultsTo(60.0);
		OptionSpec<Double> percentPutsOpt = 
				parser.accepts("percent-puts", "The percentage of requests that are puts [0-100].")
				.withRequiredArg()
				.describedAs("percent_puts")
				.ofType(Double.class)
				.defaultsTo(35.0);
		OptionSpec<Double> percentDeletesOpt = 
				parser.accepts("percent-deletes", "The percentage of requests that are deletes [0-100].")
				.withRequiredArg()
				.describedAs("percent_deletes")
				.ofType(Double.class)
				.defaultsTo(5.0);
		OptionSpec<String> directoryOpt = 
				parser.accepts("directory", "The directory in which to write data.")
				.withRequiredArg()
				.describedAs("data_directory")
				.ofType(String.class);
		OptionSpec<String> resultsDirectoryOpt = 
				parser.accepts("results-directory", "The directory in which to write results to.")
				.withRequiredArg()
				.describedAs("results_directory")
				.ofType(String.class)
				.defaultsTo("result");
		OptionSpec<String> storeTypeOpt = 
				parser.accepts("store-type", "KVStore type {bdb, mysql, krati, leveldb, rocksdb}")
				.withRequiredArg()
				.describedAs("type")
				.ofType(String.class);
		OptionSpec<String> storePropsOpt =
				parser.accepts("store-properties", "Pick up the config properties for the store from this file")
				.withRequiredArg()
				.describedAs("store-properties")
				.ofType(String.class);
		
		OptionSpec<?> cleanupOpt = parser.accepts("cleanup", "Delete the contents of the data directory when completed.");
		OptionSpec<?> helpOpt = parser.accepts("help", "Print this help message.");
		OptionSpec<?> prepopulateOpt = 
				parser.accepts("prepopulate", "Prepopulate with sequential keys. Requires the num-keys option to be present. " + 
						"Unlike the warm-up option this will not generate any garbage, but is very fast.");

		OptionSet options = null;
		try {
			options = parser.parse(args);
		} catch(OptionException e) {
			System.err.println("ERROR: " + e.getMessage());
			parser.printHelpOn(System.err);
			System.exit(1);
		}

		if(options.has(helpOpt)) {
			parser.printHelpOn(System.out);
			System.exit(1);
		}

		if(!options.has(directoryOpt)) {
			System.err.println("Missing required arg --directory");
			System.exit(1);
		}
		
		if (!options.has(storePropsOpt)){
			System.err.println("Missing required arg --store-properties");
			System.exit(1);
		}

		int threads = options.valueOf(threadsOpt);
		int scanThreads = options.valueOf(scanThreadsOpt);
		int scanThreadSleepMs = options.valueOf(scanThreadSleepOpt);
		int throughput = options.valueOf(throughputOpt);
		long operations = options.valueOf(opsOpt);
		long runTime = options.valueOf(runTimeOpt);
		int numKeys = options.valueOf(numKeysOpt);
		int minValueSize = options.valueOf(minValueSizeOpt);
		int maxValueSize = options.valueOf(maxValueSizeOpt);
		int keySize = options.valueOf(keySizeOpt);
		long reportingInterval = options.valueOf(reportingIntervalOpt);
		double gets = options.valueOf(percentGetsOpt) / 100.0;
		double puts = options.valueOf(percentPutsOpt) / 100.0;
		double deletes = options.valueOf(percentDeletesOpt) / 100.0;
		boolean warmUp = options.has(warmupOpsOpt);
		final boolean cleanup = options.has(cleanupOpt);
		boolean prepopulate = options.has(prepopulateOpt);
		Properties props = new Properties();
		props.load(new FileInputStream(options.valueOf(storePropsOpt)));
		
		final File directory = new File(options.valueOf(directoryOpt));
		if(!directory.exists())
			directory.mkdirs();
		
		final File resultsDirectory = new File(options.valueOf(resultsDirectoryOpt));
		if (!resultsDirectory.exists()){
			System.err.println("Results directory "+ resultsDirectory.getAbsolutePath()+" does not exist");
			System.exit(0);
		}
		
		if (!new File(resultsDirectory, "gc.log").exists()){
			System.err.println("Please specify -Xloggc:"+resultsDirectory.getPath()+"/gc.log when running the tool");
			System.exit(0);
		}
			

		if ((!options.has(runTimeOpt) && !options.has(opsOpt)) || (options.has(runTimeOpt) && options.has(opsOpt))){
			System.err.println("Specify exactly one of these options.. --runtime or --ops");
			System.exit(0);
		}

		String storeType = options.valueOf(storeTypeOpt);
		final KVStore store;
		if("krati".equals(storeType)) {
			StoreConfig config = new StoreConfig(directory, 50000000);
			config.setBatchSize(10000);
			config.setNumSyncBatches(100);

			// Configure store segments
			config.setSegmentFactory(new WriteBufferSegmentFactory());
			config.setSegmentFileSizeMB(128);
			config.setSegmentCompactFactor(0.67);

			// Configure index segments
			config.setInt(StoreParams.PARAM_INDEX_SEGMENT_FILE_SIZE_MB, 32);
			config.setDouble(StoreParams.PARAM_INDEX_SEGMENT_COMPACT_FACTOR, 0.5);

			// Configure to reduce memory footprint
			config.setDataHandler(new HashIndexDataHandler());

			// Disable linear hashing
			config.setHashLoadFactor(1.0);

			store = new KratiStore(StoreFactory.createIndexedDataStore(config));
		} else if("bdbje".equals(storeType)) {
			store = new BdbJeStore(directory,resultsDirectory);
		} else if("mysql".equals(storeType)) {
			store = new MysqlStore(directory.getName(), resultsDirectory);
		} else if("leveldb".equals(storeType)) {
			store = new LevelDBStore(directory, resultsDirectory);
		} else if("rocksdb".equals(storeType)) {
			store = new RocksDBStore(directory, resultsDirectory);
		} else if("bdbc".equals(storeType)) {
			store = new BdbStore(directory, resultsDirectory);
		}else {
			throw new Exception("Unknown store type");
		}
		store.init(props);

		// log the options you got
		String opts = "";
		for(OptionSpec<?> spec: options.specs())
			opts += spec.toString() + "=" + options.valueOf(spec) + ", ";
		logger.info("Running performance test with the following parameters: " + opts);
		PerformanceTest test = new PerformanceTest(store,
				threads, 
				scanThreads, 
				scanThreadSleepMs,
				throughput,
				operations,
				runTime,
				numKeys,
				minValueSize,
				maxValueSize,
				keySize,
				1000 * reportingInterval, 
				gets, 
				puts, 
				deletes, 
				warmUp,
				prepopulate,
				resultsDirectory);
		Metrics metrics = test.run();
		metrics.print(test.getLatencyOutFile());

		try {
			store.close();
		} catch(Exception e) {
			e.printStackTrace();
		}
		if(cleanup) {
			File[] files = directory.listFiles();
			if(files != null) {
				for(File f: files)
					delete(f);
			}
		}
	}



	public Metrics run() {
		try {
			Metrics global = new Metrics();
			Metrics snapshot = new Metrics(global);

			final Random rand = new Random();
			if (this.prepopulate) {
				logger.info("Starting prepopulate phase");
				store.beginPrepopulate();
				final CountDownLatch latch = new CountDownLatch(threads);
				ExecutorService executors = Executors.newFixedThreadPool(threads);
				for (int i = 0; i < threads; i++) {
					final int threadid = i;
					executors.submit(new Runnable() {
						public void run() {
							int id = threadid;
							try {
								long startMs = System.currentTimeMillis();
								for (int i = id; i < numKeys; i += threads) {
									if (i % 10000000 == 0) {
										long now = System.currentTimeMillis();
										logger.debug("Loaded " + i
												+ " entries in "
												+ (now - startMs) / 1000 + " s");
										startMs = now;
									}
									ByteBuffer keyBuffer = ByteBuffer.allocate(4 + (keySize-4));
									keyBuffer.putInt(i);
									byte[] key = keyBuffer.array();
									byte[] value = new byte[minValueSize + 
									                        rand.nextInt(maxValueSize-minValueSize)];
									//System.out.println("Prepop:"+ value.length);
									rand.nextBytes(value);
									store.put(key, value);
								}
							} catch (Exception e) {
								logger.error("Error prepopulating in thread "
										+ id, e);
							} finally {
								latch.countDown();
							}
						}
					});
				}
				latch.await();
				executors.shutdown();
				store.endPrepopulate();
			}


			if(warmUp) {
				logger.info("Doing warm up writes.");
				store.beginWarmup();
				int totalWrites = 0;
				for (int factor=2; factor <= numKeys/2; factor=factor*2){
					boolean warmupCompleted = false;
					for (int key=0; key<numKeys;key+=factor){
						ByteBuffer keyBuffer = ByteBuffer.allocate(4 + (keySize-4));
						keyBuffer.putInt(key);
						byte[] keyBytes = keyBuffer.array();
						byte[] value = new byte[minValueSize + 
						                        rand.nextInt(maxValueSize-minValueSize)];
					        //System.out.println("Warmup:"+ value.length);
						rand.nextBytes(value);
						store.put(keyBytes, value);
						totalWrites++;

						if (totalWrites % 1000000 == 0){
							if (store.isWarmedup()){
								warmupCompleted = true;
								break;
							}
						}

					}
					if (warmupCompleted)
						break;
				}
				store.endWarmup();
			}
			
			// Schedule GC log separation thread
			long dontMeasureIntevalSecs = Math.min((long) (0.10 * runTimeSecs), DEFAULT_DONT_MEASURE_SECS);
			GCLogRampupSeparator logSeparatorThread = new GCLogRampupSeparator(new File(resultsDirectory,"gc.log"), dontMeasureIntevalSecs);
			logSeparatorThread.start();

			logger.debug("Starting " + threads + " load generation threads.");
			Metrics.printColumnNames(latencyOutFile);
			long endTimeMs = System.currentTimeMillis() + this.runTimeSecs * 1000;
			List<PerfThread> perfThreads = new ArrayList<PerfThread>();
			for(int i = 0; i < threads; i++) {
				boolean throttled = throughput > 0;
				double perThreadThroughput = throughput / (double) threads;
				PerfThread thread = new PerfThread(store, 
												   snapshot, 
												   operations / threads, 
												   endTimeMs, 
												   ops(gets, puts, deletes), 
												   throttled, 
												   perThreadThroughput, 
												   minValueSize, 
												   maxValueSize, 
												   keySize, 
												   numKeys);
				thread.setName("perf-client-thread-"+i);
				thread.start();
				perfThreads.add(thread);
			}

			logger.debug("Starting " + scanThreads + " scan threads.");
			List<ScanThread> scanThreads = new ArrayList<ScanThread>();
			for(int i = 0; i < this.scanThreads; i++) {
				ScanThread thread = new ScanThread(this.store, snapshot, scanThreadSleepMs);
				thread.setName("scan-thread-"+i);
				thread.start();
				scanThreads.add(thread);
			}

			ReporterThread reporter = new ReporterThread(snapshot, reportingInterval, store, latencyOutFile, dontMeasureIntevalSecs);
			reporter.start();

			// wait for perf threads to complete
			for(PerfThread thread: perfThreads)
				thread.join();
			logger.debug("Perf threads completed");

			// now interrupt the other threads and wait for them to stop
			reporter.interrupt();
			for(ScanThread thread: scanThreads)
				thread.interrupt();
			for(ScanThread thread: scanThreads)
				thread.join();
			logger.debug("Scan threads completed");
			reporter.join();
			logger.debug("Reporter thread completed");
			
			latencyOutFile.flush();
			latencyOutFile.close();
			
			return global;
		} catch(Exception e) {
			throw new RuntimeException(e);
		}
	}

	private Op[] ops(double gets, double puts, double deletes) {
		ArrayList<Op> ops = new ArrayList<Op>();
		int numGets = (int) (100 * gets);
		int numPuts = (int) (100 * puts);
		int numDeletes = (int) (100 * deletes);
		for(int i = 0; i < numGets; i++)
			ops.add(Op.GET);
		for(int i = 0; i < numPuts; i++)
			ops.add(Op.PUT);
		for (int i=0; i < numDeletes; i++)
			ops.add(Op.DELETE);
		Collections.shuffle(ops);
		return ops.toArray(new Op[ops.size()]);
	}
	
	public static void copyFile(File sourceFile, File destFile) throws IOException {
	    if(!destFile.exists()) {
	        destFile.createNewFile();
	    }

	    FileChannel source = null;
	    FileChannel destination = null;

	    try {
	        source = new FileInputStream(sourceFile).getChannel();
	        destination = new FileOutputStream(destFile).getChannel();
	        destination.transferFrom(source, 0, source.size());
	    }
	    finally {
	        if(source != null) {
	            source.close();
	        }
	        if(destination != null) {
	            destination.close();
	        }
	    }
	}
	
	private static class GCLogRampupSeparator extends Thread {

		private File gcLog;
		private long dontMeasureIntervalSecs;

		public GCLogRampupSeparator(File gclog, long dontMeasureIntervalSecs){
			this.gcLog = gclog;
			this.dontMeasureIntervalSecs = dontMeasureIntervalSecs;
		}

		public void run() {
			try {
				Thread.sleep(dontMeasureIntervalSecs* 1000L);
				File gcLogDM = new File(gcLog.getParentFile(),gcLog.getName()+".rampup");
				copyFile(gcLog, gcLogDM);
				logger.info("Separated GC log");
			}	
			catch(Exception e) {
				logger.error("Error in GC log separation", e);
			}
		}
	}

	private static class PerfThread extends Thread {

		private static AtomicInteger counter = new AtomicInteger(0);

		private final KVStore store;
		private final Metrics metrics;
		private final boolean throttled;
		private final double throughput;
		private final Op[] opTypes;
		private final int minValueSize;
		private final int maxValueSize;
		private final int keySize;
		private long ops;
		private long endTimeMs;
		private int numKeys;
		private final Random random;

		public PerfThread(KVStore store, 
				Metrics metrics, 
				long ops,
				long endTimeMs,
				Op[] opTypes, 
				boolean throttled,
				double throughput, 
				int minValueSize,
				int maxValueSize,
				int keySize,
				int numKeys) {
			super("perf-thread-" + counter.getAndIncrement());
			this.metrics = metrics;
			this.store = store;
			this.ops = ops;
			this.opTypes = opTypes;
			this.throttled = throttled;
			this.throughput = throughput;
			this.minValueSize = minValueSize;
			this.maxValueSize = maxValueSize;
			this.keySize = keySize;
			this.random = new Random();
			this.numKeys = numKeys;
			this.endTimeMs = endTimeMs;
		}

		public void run() {
			try {
				RateThrottler throttler = null;
				if (throttled)
					throttler = new RateThrottler(throughput);

				long elapsed = 0;
				byte[] value = new byte[minValueSize+ random.nextInt(maxValueSize-minValueSize)];
				random.nextBytes(value);
				long opsDone = 0;
				while(true) {
					ByteBuffer keyBuffer = ByteBuffer.allocate(4 + (this.keySize-4));
					keyBuffer.putInt(random.nextInt(numKeys));
					byte[] key = keyBuffer.array();
					long start = System.nanoTime();
					switch(opTypes[(int)(opsDone % opTypes.length)]) {                            
					case GET:
						byte[] found = this.store.get(key);
						elapsed = System.nanoTime() - start;
						this.metrics.getLatency.record(elapsed);
						if(found != null)
							this.metrics.getBytes.record(found.length);
						break;
					case PUT:
						this.store.put(key, value);
						elapsed = System.nanoTime() - start;
						this.metrics.putLatency.record(elapsed);
						this.metrics.putBytes.record(value.length);
						break;
					case DELETE:
						this.store.delete(key);
						elapsed = System.nanoTime() - start;
						this.metrics.deleteLatency.record(System.nanoTime() - start);
						break;      
					}
					if(throttler != null)
						throttler.throttle();
					opsDone++;
					if (ops > 0 && opsDone >= ops){
						return;
					}
					if (ops <= 0 && System.currentTimeMillis() >= endTimeMs){
						return;
					}
				}
			} catch(InterruptedException e) {
				e.printStackTrace();
				return;
			} catch(Exception e) {
				e.printStackTrace();
				return;
			}
		}
	}

	/**
	 * 
	 * Throttles events at the specified rate. 
	 * Notes about usage: 
	 * 1) Be sure to call throttle() in the event loop i.e once every event. 
	 * 2) Clock starts ticking once you construct this object.
	 * 3) This class is not thread safe
	 *
	 */
	private static class RateThrottler {

		double targetRatePerMs;
		long startTimeMs;
		long numOps;

		long totalSleepTime;

		RateThrottler(double targetRatePerSec) {
			this.targetRatePerMs = targetRatePerSec/ 1000.0;
			this.startTimeMs = System.currentTimeMillis();
			this.numOps = 0;
			this.totalSleepTime=0;
		}

		public void throttle() {
			this.numOps++;
			double timePerOp = ((double) this.numOps) / (this.targetRatePerMs);
			long timeElapsed = System.currentTimeMillis() - this.startTimeMs;
			if (timeElapsed < timePerOp) {
				try {
					long sleepTime = (long)(timePerOp - timeElapsed); 
					Thread.sleep(sleepTime);
					this.totalSleepTime += sleepTime;
				} catch(InterruptedException e) {}
			}
		}

		@Override
		public String toString() {
			return this.getClass() + 
					" rate/sec: " + (targetRatePerMs * 1000.0) + 
					", totalSleepTimeMs:" + this.totalSleepTime + 
					" totalElapsedTimeMs:" + (System.currentTimeMillis()- startTimeMs);
		}
	}

	private static class ScanThread extends Thread {
		private static AtomicInteger counter = new AtomicInteger(0);

		private final KVStore store;
		private final Metrics metrics;
		private final int sleep;

		public ScanThread(KVStore store, Metrics metrics, int sleep) {
			super("scan-thread-" + counter.getAndIncrement());
			this.store = store;
			this.metrics = metrics;
			this.sleep = sleep;
		}

		public void run() {
			Iterator<byte[]> iter = null;
			while(!isInterrupted()) {
				try {
					iter = this.store.values();
					while(iter.hasNext() && !isInterrupted()) {
						byte[] value = iter.next();
						this.metrics.scanThroughput.record(value.length);
					}
					sleep(sleep);
				} catch(InterruptedException e) {
					break;
				} catch(Exception e) {
					logger.error("Error in scan thread:", e);
				}
			}
			logger.debug("Scan thread exiting.");
		}
	}

	private class ReporterThread extends Thread {

		private final Metrics metrics;
		private final long sleepMs;
		private KVStore store;
		private PrintStream out;
		private long dontMeasureSecs;

		public ReporterThread(Metrics metrics, long sleepMs, KVStore store, PrintStream out, long dontMeasureSecs) {
			super("reporter-thread");
			this.metrics = metrics;
			this.sleepMs = sleepMs;
			this.store = store;
			this.setDaemon(true);
			this.out = out;
			this.dontMeasureSecs = dontMeasureSecs;
		}

		public void run() {
			try {
				Thread.sleep(dontMeasureSecs*1000);
				while(!isInterrupted()) {
					sleep(sleepMs);
					metrics.print(out);
					store.reportDetailedStats();
					metrics.reset();
				}
			} catch(InterruptedException ie){
				return;
			} catch(Exception e) {
				logger.error("Error in reporter thread", e);
				e.printStackTrace();
			}
		}
	}

	private static enum Op {GET, PUT, DELETE};

	private static class Metrics {
		private static int WIDTH = 10;
		public Metric getLatency;
		public Metric getBytes;
		public Metric putLatency;
		public Metric putBytes;
		public Metric deleteLatency;
		public Metric scanThroughput;

		public Metrics(Metrics parent) {
			this.getLatency = new Metric("g-latency", NUM_SAMPLES, parent.getLatency);
			this.getBytes = new Metric("g-bytes", NUM_SAMPLES, parent.getBytes);
			this.putLatency = new Metric("p-latency", NUM_SAMPLES, parent.putLatency);
			this.putBytes = new Metric("p-bytes", NUM_SAMPLES, parent.putBytes);
			this.deleteLatency = new Metric("d-latency", NUM_SAMPLES, parent.deleteLatency);
			this.scanThroughput = new Metric("scan-rate", NUM_SAMPLES, parent.scanThroughput);
		}

		public Metrics() {
			this.getLatency = new Metric("g-latency", NUM_SAMPLES);
			this.getBytes = new Metric("g-bytes", NUM_SAMPLES);
			this.putLatency = new Metric("p-latency", NUM_SAMPLES);
			this.putBytes = new Metric("p-bytes", NUM_SAMPLES);
			this.deleteLatency = new Metric("d-latency", NUM_SAMPLES);
			this.scanThroughput = new Metric("scan-rate", NUM_SAMPLES);
		}

		public synchronized void reset() {
			this.getLatency.reset();
			this.getBytes.reset();
			this.putLatency.reset();
			this.putBytes.reset();
			this.deleteLatency.reset();
			this.scanThroughput.reset();
		}

		public static void printColumnNames(PrintStream out) {
			String format = "%" + WIDTH + "s";
			printRequestCols("g_", out);
			//out.printf(format, "g_rate");
			printRequestCols("p_", out);
			//out.printf(format, "p_rate");
			printRequestCols("d_", out);
			//out.printf(format, "scan_rate");
			out.println();
		}

		private static void printRequestCols(String prefix, PrintStream out) {
			String[] cols = {"reqs", "qps", "avg", "50", "95", "99", "99_9"};
			for(String col: cols)
				out.print(String.format("%" + WIDTH +"s", prefix + col));
		}

		public synchronized void print(PrintStream out) {
			printRequest(this.getLatency, out);
			//out.printf(floatFmt(0), this.getBytes.rate());
			printRequest(this.putLatency, out);
			//out.printf(floatFmt(0), this.putBytes.rate());
			printRequest(this.deleteLatency, out);
			//out.printf(floatFmt(0), this.scanThroughput.rate());
			out.println();
		}

		private void printRequest(Metric m, PrintStream out) {
			out.printf("%" + WIDTH + "d", m.count());
			out.printf(floatFmt(1), m.occuranceRate());
			out.printf(floatFmt(1), m.avg() / (1000.0*1000.0));
			double[] quantiles = m.quantiles(0.5, 0.95, 0.99, 0.999);
			for(double q: quantiles)
				out.printf(floatFmt(1), q / (1000.0*1000.0));
		}

		private String floatFmt(int decimals) {
			return "%" + WIDTH + "." + decimals + "f";
		}
	}

	private static class Metric {
		private final String name;
		private final int numSamples;
		private long start;
		private long count;
		private double total;
		private double max;
		private Sampler sampler;
		private Metric[] parents;

		public Metric(String name, int samples) {
			this(name, samples, new Metric[0]);
		}

		public Metric(String name, int samples, Metric... parents) {
			this.name = name;
			this.numSamples = samples;
			this.count = 0L;
			this.total = 0.0d;
			this.max = Double.MIN_VALUE;
			this.sampler = new Sampler(samples, System.nanoTime());
			this.start = System.nanoTime();
			this.parents = parents;
		}

		public synchronized void record(double v) {
			this.max = Math.max(v, this.max);
			this.total += v;
			this.count++;
			this.sampler.sample(v);
			for(Metric parent: parents)
				parent.record(v);
		}

		private double ellapsedSecs() {
			long now = System.nanoTime();
			return (now - start) / (1000.0 * 1000.0 * 1000.0);
		}

		public String name() {
			return this.name;
		}

		public synchronized double rate() {
			return this.total / ellapsedSecs();
		}

		public synchronized double max() {
			return this.max;
		}

		public synchronized double occuranceRate() {
			return this.count / ellapsedSecs();
		}

		public synchronized double avg() {
			return total / count;
		}

		public synchronized long count() {
			return this.count;
		}

		public synchronized double[] quantiles(double...qs) {
			return this.sampler.quantiles(qs);
		}

		public synchronized void reset() {
			this.count = 0L;
			this.total = 0.0d;
			this.max = Double.MIN_VALUE;
			this.start = System.nanoTime();
			this.sampler = new Sampler(this.numSamples, System.nanoTime());
		}
	}

	/**
	 * Resevoir sampler
	 */
	public static class Sampler {
		private final Random rand;
		private final double[] values;
		private long count;

		public Sampler(int size, long seed) {
			this.values = new double[size];
			this.count = 1;
			this.rand = new Random(seed);
		}

		public void sample(double v) {
			if(this.count <= this.values.length) {
				this.values[(int) count - 1] = v;
				this.count++;
			} else {
				this.count++;
				long r = Math.abs((long) rand.nextLong() % this.count);
				if(r < this.values.length)
					this.values[(int) r] = v;
			}          
		}

		public double[] quantiles(double... qs) {
			int size = Math.min(this.values.length, (int) count);
			double[] copy = new double[size];
			System.arraycopy(this.values, 0, copy, 0, size);
			Arrays.sort(this.values);
			double[] results = new double[qs.length];
			for(int i = 0; i < qs.length; i++)
				results[i] = this.values[(int) Math.round(qs[i] * (size-1))];
			return results;
		}

		@Override
		public String toString() {
			StringBuilder b = new StringBuilder();
			b.append('[');
			for(int i = 0; i < this.values.length; i++) {
				b.append(this.values[i]);
				b.append(' ');
			}
			b.append(']');
			return b.toString();
		}

		public static void main(String[] args) {
			System.out.println("Random samples of {0...99}");
			for(int i = 0; i < 10; i++) {
				Sampler s = new Sampler(10, i);
				for(int j = 0; j < 100; j++)
					s.sample(j);
				System.out.println(s);
			}
		}
	}

	public static void delete(File file) {
		if(file.isFile()) {
			file.delete();
		} else if(file.isDirectory()) {
			for(File f: file.listFiles())
				f.delete();
			file.delete();
		}
	}	
}
