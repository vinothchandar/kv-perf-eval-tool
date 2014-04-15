package kvperf;

import java.io.File;
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import com.sleepycat.db.Database;
import com.sleepycat.db.DatabaseConfig;
import com.sleepycat.db.DatabaseEntry;
import com.sleepycat.db.DatabaseType;
import com.sleepycat.db.Environment;
import com.sleepycat.db.EnvironmentConfig;
import com.sleepycat.db.LockMode;
import com.sleepycat.db.OperationStatus;
import com.sleepycat.db.Transaction;



public class BdbStore implements KVStore {

	private static final Logger logger = Logger.getLogger(BdbStore.class);
	
	private File envDir; 
	private File resultsDirectory;
	private EnvironmentConfig envConfig;
	private DatabaseConfig dbConfig;
	private Properties  properties;
	private Database database;
	private Environment environment;

	AtomicInteger nullCount = new AtomicInteger(); 

	
	public BdbStore(File envDir, File resultsDirectory) {		
		this.resultsDirectory = resultsDirectory;
		this.envDir = envDir;	
	}
	
	private void initInternal() throws Exception {
		envConfig = new EnvironmentConfig();
		envConfig.setTransactional(true);

		envConfig.setAllowCreate(true);
		envConfig.setPrivate(true);

		if (properties.containsKey("bdbc.cache.size")){
			envConfig.setCacheSize(Long.parseLong(properties.getProperty("bdbc.cache.size")));	
		} else {
			logger.error("Specify a BDB cache size ..");
			System.exit(0);
		}

		// Don't store anything as blobs
		envConfig.setBlobThreshold(0);
		envConfig.setTxnNoSync(true);
		envConfig.setInitializeCache(true);
		envConfig.setCachePageSize(Integer.parseInt(properties.getProperty("bdbc.page.size", Integer.toString(4 * 1024))));
		envConfig.setInitializeRegions(true);
		// Properties picked up from config file..
		envConfig.setMaxLogFileSize(Integer.parseInt(properties.getProperty("bdbc.max.log.file.size", Integer.toString(60 * 1024 * 1024))));
		// TODO configure the checkpointer 
		envConfig.setLockTableSize(50);
		envConfig.setInitializeLocking(true);
		envConfig.setInitializeLogging(true);
		envConfig.setLockTimeout(500* 1000);
		logger.info("Environment Config: " + envConfig);

		// Building the database config
		dbConfig = new DatabaseConfig();

		dbConfig.setAllowCreate(true);
		dbConfig.setBlobThreshold(0);
		dbConfig.setBtreeMinKey(2);
		dbConfig.setSortedDuplicates(false);
		dbConfig.setUnsortedDuplicates(false);
		dbConfig.setType(DatabaseType.BTREE);
		dbConfig.setPageSize(Integer.parseInt(properties.getProperty("bdbc.page.size", Integer.toString(4 * 1024))));
		// TODO add support for batch loading, if any
		dbConfig.setTransactional(true);
		//dbConfig.setDeferredWrite(false);


		logger.info("Database config: " + dbConfig);
		long startTimeMs = System.currentTimeMillis();
		environment = new Environment(envDir, envConfig);

		database = environment.openDatabase(null, "test.db", "test", dbConfig);
		logger.info("Startup completed in "+ (System.currentTimeMillis() - startTimeMs)/1000 + " secs");
	}
	

	@Override
	public void init(Properties props) throws Exception {
		this.properties = props;
		initInternal();
	}

	@Override
	public void beginPrepopulate() throws Exception {
		// NOTHING TO DO
	}

	@Override
	public void endPrepopulate() throws Exception {
		// NOTHING TO DO
	}

	@Override
	public void beginWarmup() {
		// NOTHING TO DO
	}

	@Override
	public void endWarmup() {
		// NOTHING TO DO
	}

	@Override
	public boolean isWarmedup() {
		// let the warmup end on it own
		return false;
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
		DatabaseEntry keyEntry = new DatabaseEntry(key);
		database.delete(null, keyEntry);
	}

	@Override
	public Iterator<byte[]> values() throws Exception {
		//TODO implement this.. 
		return null;
	}

	@Override
	public void reportDetailedStats() throws Exception {
		// TODO Dump out some stats later..
	}

	@Override
	public void close() throws Exception {
		logger.info("Closing BDB store.. Null Count in experiment :"+ nullCount.intValue());
		long startTimeMs = System.currentTimeMillis();
		database.close();
		environment.close();
		logger.info("Shutdown completed in "+ (System.currentTimeMillis() - startTimeMs)/1000+" secs");	
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		
	}
}
