package kvperf;

import java.io.File;
import java.util.Iterator;
import java.util.Properties;

import org.apache.log4j.Logger;

import rocksdb.jna.Options;
import rocksdb.jna.ReadOptions;
import rocksdb.jna.RocksDB;
import rocksdb.jna.WriteOptions;

public class RocksDBStore implements KVStore {

	private static final Logger logger = Logger.getLogger(RocksDBStore.class);

	private File dataDir;
	private File resultsDirectory;
	private RocksDB rocksDB;
	
	public RocksDBStore(File envDir, File resultsDirectory){
		this.resultsDirectory = resultsDirectory;
		this.dataDir = envDir;
	}
	
	
	
	@Override
	public void init(Properties props) throws Exception {
		
		if (props.containsKey("rocksdb.jna.libpath")){
			RocksDB.loadLibrary(props.getProperty("rocksdb.jna.libpath"));
		} else {
			logger.error("Must provide path to jna lib using rocksdb.jna.libpath property");
			System.exit(0);
		}
		
		
		Options opts = new Options();
		opts.cacheSizeBytes = 100 * 1024 * 1024;
		rocksDB = new RocksDB(dataDir.getAbsolutePath(), opts);
	}

	@Override
	public void beginPrepopulate() throws Exception {
		// Nothing to do here
	}

	@Override
	public void endPrepopulate() throws Exception {
		// Nothing to do here
	}

	@Override
	public void beginWarmup() {
		// Nothing to do here ..
	}

	@Override
	public void endWarmup() {
		// Nothing to do here ..
	}

	@Override
	public boolean isWarmedup() {
		// let the warmup end on it own
		return false;
	}

	@Override
	public void put(byte[] key, byte[] value) throws Exception {
		rocksDB.put(key, value, new WriteOptions());
	}

	@Override
	public byte[] get(byte[] key) throws Exception {
		return rocksDB.get(key, new ReadOptions());
	}

	@Override
	public void delete(byte[] key) throws Exception {
		rocksDB.delete(key, new WriteOptions());
	}

	@Override
	public Iterator<byte[]> values() throws Exception {
		return null;
	}

	@Override
	public void reportDetailedStats() throws Exception {
		
	}

	@Override
	public void close() throws Exception {
		if (rocksDB != null){
		  rocksDB.close();
		}
	}

	public static void main(String[] args) throws Exception {
		Options options = new Options();
		WriteOptions wOpts = new WriteOptions();		
		ReadOptions rOpts = new ReadOptions();
		
		RocksDB.loadLibrary("path/to/rocksdb-jna-ubuntu.so");
		RocksDB rocksdb = new RocksDB("sample-rocksapp", options);
		try {

			
			rocksdb.put("test".getBytes(), "test-val".getBytes(),wOpts);
			System.out.println(new String(rocksdb.get("test".getBytes(),rOpts)));
		} finally {
		  // Make sure you close the db to shutdown the 
		  // database and avoid resource leaks.
		  rocksdb.close();
		}

	}

	
}
