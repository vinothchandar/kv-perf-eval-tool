package kvperf;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.text.SimpleDateFormat;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import com.sleepycat.je.CacheMode;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Durability;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentMutableConfig;
import com.sleepycat.je.EnvironmentStats;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.cleaner.FileSummary;
import com.sleepycat.je.cleaner.UtilizationProfile;
import com.sleepycat.je.dbi.EnvironmentImpl;

class SpaceUtilizationStats {

	private final EnvironmentImpl envImpl;

	private SortedMap<Long, FileSummary> summaryMap;
	private long totalSpaceUsed = 0;
	private long totalSpaceUtilized = 0;
	private long totalFiles = 0;

	public SpaceUtilizationStats(Environment env) {
		this(DbInternal.getEnvironmentImpl(env));
	}

	private SpaceUtilizationStats(EnvironmentImpl envImpl) {
		this.envImpl = envImpl;
		UtilizationProfile profile = this.envImpl.getUtilizationProfile();
		summaryMap = profile.getFileSummaryMap(true);

		Iterator<Map.Entry<Long, FileSummary>> fileItr = summaryMap.entrySet().iterator();
		while(fileItr.hasNext()) {
			Map.Entry<Long, FileSummary> entry = fileItr.next();
			FileSummary fs = entry.getValue();
			totalSpaceUsed += fs.totalSize;
			totalSpaceUtilized += fs.totalSize - fs.getObsoleteSize();
			totalFiles++;
		}
	}
	
	public long getTotalFiles() {
		return totalFiles;
	}

	public long getTotalSpaceUsed() {
		return totalSpaceUsed;
	}

	public long getTotalSpaceUtilized() {
		return totalSpaceUtilized;
	}

	public double getUtilization() {
		return ((getTotalSpaceUtilized() * 100.0) / getTotalSpaceUsed());
	}
}


public class BdbStore implements KVStore {

	private static final Logger logger = Logger.getLogger(BdbStore.class);
	private static final double UTIL_PCT = 50.0;

	private Database database;
	private Environment environment;
	private File envDir; 
	private EnvironmentConfig envConfig;
	private DatabaseConfig dbConfig;

	private PrintStream bdbStatsFile = null;
	private Properties  properties;
	private DynamicCacheAdjuster cacheAdjusterThread;
	private File resultsDirectory;
	
	
	AtomicInteger nullCount = new AtomicInteger(); 

	public BdbStore(File envDir, File resultsDirectory) {		
		this.resultsDirectory = resultsDirectory;
		this.envDir = envDir;
	}
	
	private PrintStream getStatsFile() {
		if (bdbStatsFile == null){
			try {
				bdbStatsFile = new PrintStream(new FileOutputStream(new File(this.resultsDirectory, "bdb.stats")));
			} catch (FileNotFoundException e) {
				logger.error("Error creating bdb.stats file", e);
				e.printStackTrace();
			}
		}
		return bdbStatsFile;
	}
	
	
	class DynamicCacheAdjuster extends Thread {
		
		private final static long CACHE_UNIT_BYTES = 100L * 1024L * 1024L;
		private final static long MIN_LOG_FILES = 2;
		private final static int OBSERVATION_INTERVAL_MS=30 * 1000;
		private final static double UTILIZATION_DELTA = 2.0;
		private final static int CLEANER_BACKLOG_THRESHOLD=2;
		private final static long MAX_MEMORY_BYTES = 10L * 1024L * 1024L * 1024L;
		
		private boolean stop = false;
		
		
		public void run() {
			this.setName("CacheAdjuster");
			logger.info("Starting cache adjuster...");
			while(!stop){
				try {
					Thread.sleep(OBSERVATION_INTERVAL_MS);
					
					SpaceUtilizationStats spaceStats = new SpaceUtilizationStats(environment);
					if (spaceStats.getTotalFiles() < MIN_LOG_FILES)
						continue;
					EnvironmentStats stats = getEnvStats(false); 
					logger.info("Cache Adjuster Probe: "+ "Current Util:"+ String.format("%.2f", spaceStats.getUtilization())
							    +", CleanerBacklog:"+stats.getCleanerBacklog());
					if (((UTIL_PCT - spaceStats.getUtilization()) >= UTILIZATION_DELTA) && stats.getCleanerBacklog() > CLEANER_BACKLOG_THRESHOLD){
						EnvironmentMutableConfig mConfig = environment.getMutableConfig();
						long currentCacheInBytes = mConfig.getCacheSize();
						if (currentCacheInBytes >= MAX_MEMORY_BYTES)
							continue;
						
						logger.info("Cache Adjuster Probe: Current Cache Size(MB):"+ (currentCacheInBytes/(1024*1024))
								+ " adding "+ CACHE_UNIT_BYTES);
						
						mConfig.setCacheSize(currentCacheInBytes + CACHE_UNIT_BYTES);
						environment.setMutableConfig(mConfig);
					}
				}catch (InterruptedException ie){
					return;
				}
				catch (Exception e){
					logger.error("Error in adjuster", e);
				}
			}
		}
				
		private void stopAdjuster(){
			stop = true;
			this.interrupt();
		}
	}
	
	
	private void initInternal(boolean batchWriteMode){
		envConfig = new EnvironmentConfig();
		if (batchWriteMode)
			envConfig.setTransactional(false);
		else
			envConfig.setTransactional(true);

		envConfig.setAllowCreate(true);
		envConfig.setReadOnly(false);
		
		if (properties.containsKey("je.cache.size")){
			envConfig.setCacheSize(Long.parseLong(properties.getProperty("je.cache.size")));	
		} else {
			logger.error("Specify a BDB cache size ..");
			System.exit(0);
		}
		
		envConfig.setDurability(Durability.COMMIT_NO_SYNC);

		// Properties picked up from config file..
		envConfig.setConfigParam(EnvironmentConfig.LOG_FILE_MAX,
				properties.getProperty(EnvironmentConfig.LOG_FILE_MAX, Long.toString(60 * 1024L * 1024L)));
		envConfig.setConfigParam(EnvironmentConfig.CHECKPOINTER_BYTES_INTERVAL, properties.getProperty(EnvironmentConfig.CHECKPOINTER_BYTES_INTERVAL,"2147483648"));
		envConfig.setConfigParam(EnvironmentConfig.TREE_BIN_DELTA, properties.getProperty(EnvironmentConfig.TREE_BIN_DELTA,"75"));
		envConfig.setConfigParam(EnvironmentConfig.TREE_MAX_DELTA, properties.getProperty(EnvironmentConfig.TREE_MAX_DELTA,"100"));
		envConfig.setConfigParam(EnvironmentConfig.CLEANER_MIN_UTILIZATION,
				properties.getProperty(EnvironmentConfig.CLEANER_MIN_UTILIZATION, Integer.toString((int) UTIL_PCT)));
		envConfig.setConfigParam(EnvironmentConfig.CLEANER_THREADS,
				properties.getProperty(EnvironmentConfig.CLEANER_THREADS, Integer.toString(1)));
		envConfig.setConfigParam(EnvironmentConfig.CLEANER_BYTES_INTERVAL, 
				properties.getProperty(EnvironmentConfig.CLEANER_BYTES_INTERVAL, Long.toString(30 * 1024L * 1024L)));
		
		// TODO leaving these configs hardcoded for now.. Not sure if we will even change them
		envConfig.setConfigParam(EnvironmentConfig.CLEANER_MIN_FILE_UTILIZATION, Integer.toString(0));
		envConfig.setConfigParam(EnvironmentConfig.CLEANER_LOOK_AHEAD_CACHE_SIZE,
				Integer.toString(8192));
		envConfig.setConfigParam(EnvironmentConfig.LOCK_N_LOCK_TABLES,
				Integer.toString(7));
		envConfig.setConfigParam(EnvironmentConfig.ENV_FAIR_LATCHES,
				Boolean.toString(false));
		envConfig.setConfigParam(EnvironmentConfig.CHECKPOINTER_HIGH_PRIORITY,
				Boolean.toString(false));
		envConfig.setConfigParam(EnvironmentConfig.CLEANER_MAX_BATCH_FILES,
				Integer.toString(0));
		envConfig.setConfigParam(EnvironmentConfig.LOG_FAULT_READ_SIZE,
				Integer.toString(4 * 1024));
		envConfig.setConfigParam(EnvironmentConfig.LOG_ITERATOR_READ_SIZE,
				Integer.toString(8192));
		envConfig.setConfigParam(EnvironmentConfig.CLEANER_LAZY_MIGRATION, Boolean.toString(false));
		envConfig.setConfigParam(EnvironmentConfig.EVICTOR_LRU_ONLY, Boolean.toString(false));
		envConfig.setCacheMode(CacheMode.EVICT_LN);
		envConfig.setLockTimeout(500, TimeUnit.MILLISECONDS);
		logger.info(envConfig);

		// Building the database config
		dbConfig = new DatabaseConfig();
		if (batchWriteMode){
			dbConfig.setTransactional(false);
			dbConfig.setDeferredWrite(true);
		}
		else {
			dbConfig.setTransactional(true);
			dbConfig.setDeferredWrite(false);
		}
		dbConfig.setAllowCreate(true);
		dbConfig.setSortedDuplicates(false);
		dbConfig.setNodeMaxEntries(Integer.parseInt(properties.getProperty("je.tree.fanout", "512")));
		dbConfig.setReadOnly(false);

		logger.info(dbConfig);
		long startTimeMs = System.currentTimeMillis();
		environment = new Environment(envDir, envConfig);
		database = environment.openDatabase(null, "test", dbConfig);
		logger.info("Startup completed in "+ (System.currentTimeMillis() - startTimeMs)/1000 + " secs");
		
		if (dbConfig.getTransactional()){
			// dummy stat fetch to reset counters
			getEnvStats(true);
			if (Boolean.parseBoolean(properties.getProperty("je.adjust.cache", "false"))){
				cacheAdjusterThread = new DynamicCacheAdjuster();
				cacheAdjusterThread.start();
			}
		}
	}
	
	
	
	@Override
	public void init(Properties props) {
		this.properties = props;
		initInternal(false);
	}

	@Override
	public void put(byte[] key, byte[] value) throws Exception {
		Transaction transaction = null;
		boolean success =false;
		try {
			if (dbConfig.getTransactional()) {
				transaction = environment.beginTransaction(null, null);
			}
			DatabaseEntry keyEntry = new DatabaseEntry(key);
			DatabaseEntry valueEntry = new DatabaseEntry(value);
			database.put(transaction, keyEntry, valueEntry);
			success = true;
		} finally {
			if (transaction != null){
				if (success) {
					transaction.commit();
				} else {
					transaction.abort();
				}
			}
		}
	}

	@Override
	public byte[] get(byte[] key) throws Exception {
		DatabaseEntry keyEntry = new DatabaseEntry(key);
		DatabaseEntry valueEntry = new DatabaseEntry();
		OperationStatus status  = database.get(null, keyEntry, valueEntry, LockMode.READ_UNCOMMITTED);
		if (status != OperationStatus.SUCCESS)
			nullCount.incrementAndGet();
		return valueEntry.getData();
	}

	@Override
	public void delete(byte[] key) throws Exception {
		//TODO this needs transactions too
		DatabaseEntry keyEntry = new DatabaseEntry(key);
		database.delete(null, keyEntry);
	}

	@Override
	public Iterator<byte[]> values() throws Exception {
		final Cursor cursor = database.openCursor(null, null);
		cursor.setCacheMode(CacheMode.EVICT_BIN);
		return new Iterator<byte[]>() {

			byte[] current = null;

			@Override
			public boolean hasNext() {
				return current != null || fetchNextKey();
			}

			public byte[] next() {
				byte[] result = null;
				if(current == null) {
					if(!fetchNextKey())
						throw new NoSuchElementException("Iterated to end.");
				}
				result = current;
				current = null;
				return result;
			}

			private boolean fetchNextKey() {
				DatabaseEntry keyEntry = new DatabaseEntry();
				DatabaseEntry valueEntry = new DatabaseEntry();
				valueEntry.setPartial(true);
				try {
					OperationStatus status = cursor.getNext(keyEntry,
							valueEntry,
							LockMode.READ_UNCOMMITTED);
					if(OperationStatus.NOTFOUND == status) {
						// we have reached the end of the cursor
						return false;
					}
					current = valueEntry.getData();
					return true;
				} catch(DatabaseException e) {
					e.printStackTrace();
					return false;
				}
			}

			@Override
			public void remove() {
				OperationStatus status = cursor.delete();
			}
		};
	}

	private EnvironmentStats getEnvStats(boolean clear){
		StatsConfig config = new StatsConfig();
		config.setFast(true);
		config.setClear(clear);
		return environment.getStats(config);
	}
	
	public void reportDetailedStats() throws Exception{
		PrintStream statsFile = getStatsFile();
		SpaceUtilizationStats spaceStats = new SpaceUtilizationStats(environment);
		double utilpct = spaceStats.getUtilization();
		logger.info("Space utilization: Total:"+ spaceStats.getTotalSpaceUsed() + "  util%:" + String.format("%.2f",utilpct));
		String dateString = getCurrentDateString();	
		EnvironmentStats stats = getEnvStats(true);
		statsFile.println(dateString+",AllotedCache,"+ stats.getCacheTotalBytes());
		statsFile.println(dateString+",NumCacheMiss,"+ stats.getNCacheMiss());
		statsFile.println(dateString+",NumNotResident,"+ stats.getNNotResident());
		statsFile.println(dateString+",EvictionPassed,"+ stats.getNEvictPasses());
		statsFile.println(dateString+",BINFetches,"+ stats.getNBINsFetch());
		statsFile.println(dateString+",BINFetchMisses,"+ stats.getNBINsFetchMiss());
		statsFile.println(dateString+",INFetches,"+ stats.getNUpperINsFetch());
		statsFile.println(dateString+",INFetchMisses,"+ stats.getNUpperINsFetchMiss());
		statsFile.println(dateString+",CachedBINs,"+ stats.getNCachedBINs());
		statsFile.println(dateString+",CachedINs,"+ stats.getNCachedUpperINs());
		statsFile.println(dateString+",EvictedBINs,"+ (stats.getNBINsEvictedCacheMode() + stats.getNBINsEvictedCritical()
				+ stats.getNBINsEvictedDaemon() + stats.getNBINsEvictedManual()));
		statsFile.println(dateString+",EvictedINs,"+ (stats.getNUpperINsEvictedCacheMode() + stats.getNUpperINsEvictedCritical()
				+ stats.getNUpperINsEvictedDaemon() + stats.getNUpperINsEvictedManual()));
		statsFile.println(dateString+",NumReadBytes,"+ (stats.getNRandomReadBytes() + stats.getNSequentialReadBytes()));
		statsFile.println(dateString+",NumWriteBytes,"+ (stats.getNRandomWriteBytes() + stats.getNSequentialWriteBytes()));
		statsFile.println(dateString+",NumReads,"+ (stats.getNRandomReads() + stats.getNSequentialReads()));
		statsFile.println(dateString+",NumWrites,"+ (stats.getNRandomWrites() + stats.getNSequentialReads()));
		statsFile.println(dateString+",CleanerBacklog,"+ stats.getCleanerBacklog());
		statsFile.println(dateString+",NumCheckpoints,"+ stats.getNCheckpoints());
		statsFile.println(dateString+",NumCleanerRuns,"+ stats.getNCleanerRuns());
		statsFile.println(dateString+",NumCleanerEntriesRead,"+ stats.getNCleanerEntriesRead());
		statsFile.println(dateString+",Util%,"+ utilpct);
		statsFile.println(dateString+",BtreeLatches,"+ stats.getRelatchesRequired());
		statsFile.flush();	
	}

	@Override
	public void close() throws Exception {
		logger.info("Closing BDB store.. Null Count in experiment :"+ nullCount.intValue());

		if (Boolean.parseBoolean(properties.getProperty("je.adjust.cache", "false"))){
			cacheAdjusterThread.stopAdjuster();
			logger.info("Waiting for cache adjuster to shut down");
			cacheAdjusterThread.join();
		}
		
		long startTimeMs = System.currentTimeMillis();
		database.close();
		environment.close();
		logger.info("Shutdown completed in "+ (System.currentTimeMillis() - startTimeMs)/1000+" secs");
		if (bdbStatsFile != null)
		{
			bdbStatsFile.flush();
			bdbStatsFile.close();
		}
	}

	public void beginWarmup(){
		// turn off cleaner
		EnvironmentMutableConfig mConfig = environment.getMutableConfig();
		mConfig.setConfigParam(EnvironmentConfig.ENV_RUN_CLEANER,
				Boolean.toString(false));
		environment.setMutableConfig(mConfig);
		return;
	}

	public void endWarmup(){
		// turn on cleaner
		EnvironmentMutableConfig mConfig = environment.getMutableConfig();
		mConfig.setConfigParam(EnvironmentConfig.ENV_RUN_CLEANER,
				Boolean.toString(true));
		environment.setMutableConfig(mConfig);
		// dummy stat fetch to reset counters
		getEnvStats(true);
		return;
	}
	
	private String getCurrentDateString() {
		SimpleDateFormat dateformat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		return dateformat.format(System.currentTimeMillis());
	}
	
	public boolean isWarmedup(){
		SpaceUtilizationStats spaceStats = new SpaceUtilizationStats(environment);
		System.out.println(getCurrentDateString() + ": Warmup check "+ String.format("%.2f",spaceStats.getUtilization()));
		return ((int)spaceStats.getUtilization() <= UTIL_PCT);
	}

	public File getEnvDir(){
		return envDir;
	}

	/**
	 * Test main method
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		BdbStore store = new BdbStore(new File(args[1]), new File(args[1]));
		int numRecords = Integer.parseInt(args[0]);
		
		Properties props = new Properties();
		props.put(EnvironmentConfig.MAX_MEMORY, Long.toString(100 * 1024 * 1024));
		store.init(props);

		for (int i=0;i<numRecords;i++){
			store.put(Integer.toString(i).getBytes(), ("valueee"+i).getBytes());
		}
		store.close();
	}

	@Override
	public void beginPrepopulate() throws Exception {
		this.close();
		initInternal(true);
	}

	@Override
	public void endPrepopulate() throws Exception  {
		this.close();
		initInternal(false);
	}
}
